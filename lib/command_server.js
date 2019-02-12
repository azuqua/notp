const _ = require("lodash"),
      debug = require("debug")("notp:lib:command_server");

const GenServer = require("./gen_server"),
      NetKernel = require("./kernel"),
      Node = require("./node"),
      utils = require("./utils");

const commands = {
  "join": utils.hasID,
  "leave": _.identity,
  "meet": utils.parseNode,
  "insert": utils.parseNode,
  "minsert": utils.parseNodeList,
  "update": utils.parseNode,
  "remove": utils.parseNode,
  "mremove": utils.parseNodeList,
  "inspect": _.identity,
  "nodes": _.identity,
  "has": utils.hasID,
  "get": utils.hasID,
  "weight": utils.hasID,
  "weights": _.identity,
  "ping": _.identity
};

class CommandServer extends GenServer {
  /**
   *
   * @class CommandServer
   * @memberof Clusterluck
   *
   * @param {Clusterluck.GossipRing} gossip
   * @param {Clusterluck.NetKernel} kernel
   *
   */
  constructor(gossip, kernel) {
    super(kernel);
    this._gossip = gossip;
    this._kernel = kernel;
  }

  /**
   *
   * @method start
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {String} [name]
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  start(name) {
    super.start(name);
    Object.keys(commands).forEach((key) => {
      const handle = this[key].bind(this);
      this.on(key, handle);
      this.once("stop", _.partial(this.removeListener, key, handle).bind(this));
    });
    return this;
  }

  /**
   *
   * @method stop
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Boolean} [force]
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  stop(force = false) {
    if (this.idle() || force === true) {
      super.stop();
      return this;
    }
    this.once("idle", _.partial(this.stop, force).bind(this));
    return this;
  }

  /**
   *
   * @method decodeJob
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Buffer} buf
   *
   * @return {Object|Error}
   *
   */
  decodeJob(buf) {
    const out = super.decodeJob(buf);
    if (out instanceof Error) return out;
    let data = out.data;
    if (commands[out.event] === undefined) {
      return new Error("Cannot run against unknown command '" + out.event + "'");
    }
    const parser = commands[out.event];
    data = parser(data);
    if (data instanceof Error) {
      return data;
    }
    out.data = data;
    return out;
  }

  /**
   *
   * @method decodeSingleton
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   *
   * @return {Object|Error}
   *
   */
  decodeSingleton(data) {
    const out = super.decodeSingleton(data);
    if (commands[out.event] === undefined) {
      return new Error("Cannot run against unknown command '" + data.event + "'");
    }
    const parser = commands[out.event];
    data = parser(out.data);
    if (data instanceof Error) return data;
    out.data = data;
    return out;
  }

  /**
   *
   * @method join
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {String} data.id
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  join(data, from) {
    const out = this._gossip.join(data.id);
    if (out instanceof Error) {
      return this._encodedReply(from, {
        ok: false,
        error: utils.errorToObject(out)
      });
    }
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method meet
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Clusterluck.Node} data.node
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  meet(data, from) {
    this._gossip.meet(data.node);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method leave
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Boolean} data.force
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  leave(data, from) {
    data.force = data.force === true ? true : false;
    this._gossip.leave(data.force);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method insert
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Clusterluck.Node} data.node
   * @param {Boolean} data.force
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  insert(data, from) {
    const force = data.force === true ? true : false;
    const weight = typeof data.weight === "number" && data.weight > 0 ? data.weight : this._gossip.ring().rfactor();
    this._gossip.insert(data.node, weight, force);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method minsert
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Array} data.nodes
   * @param {Boolean} data.force
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  minsert(data, from) {
    const force = data.force === true ? true : false;
    const weight = typeof data.weight === "number" && data.weight > 0 ? data.weight : this._gossip.ring().rfactor();
    let nodes = data.nodes;
    nodes = nodes.filter((node) => {return node.id() !== this._kernel.self().id();});
    this._gossip.minsert(nodes, weight, force);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method update
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Clusterluck.Node} data.node
   * @param {Boolean} data.force
   * @param {Number} data.weight
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  update(data, from) {
    const force = data.force === true ? true : false;
    const weight = typeof data.weight === "number" && data.weight > 0 ? data.weight : this._gossip.ring().rfactor();
    this._gossip.update(data.node, weight, force);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method remove
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Clusterluck.Node} data.node
   * @param {Boolean} data.force
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  remove(data, from) {
    const force = data.force === true ? true : false;
    this._gossip.remove(data.node, force);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method mremove
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {Array} data.nodes
   * @param {Boolean} data.force
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  mremove(data, from) {
    const force = data.force === true ? true : false;
    let nodes = data.nodes;
    nodes = nodes.filter((node) => {return node.id() !== this._kernel.self().id();});
    this._gossip.mremove(nodes, force);
    return this._encodedReply(from, {ok: true});
  }

  /**
   *
   * @method inspect
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Any} data
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  inspect(data, from) {
    const ring = this._gossip.ring();
    return this._encodedReply(from, {
      ok: true,
      data: ring.toJSON(true)
    });
  }

  /**
   *
   * @method nodes
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Any} data
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  nodes(data, from) {
    const nodes = this._gossip.ring().nodes();
    return this._encodedReply(from, {
      ok: true,
      data: nodes
    });
  }

  /**
   *
   * @method has
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {String} data.id
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  has(data, from) {
    const ring = this._gossip.ring();
    const node = new Node(data.id);
    return this._encodedReply(from, {
      ok: true,
      data: ring.isDefined(node)
    });
  }

  /**
   *
   * @method get
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {String} data.id
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  get(data, from) {
    const ring = this._gossip.ring();
    const node = ring.get(new Node(data.id));
    if (node === undefined) {
      const msg = "'" + data.id + "' is not defined in this ring.";
      return this._encodedReply(from, {
        ok: false,
        error: utils.errorToObject(new Error(msg))
      });
    }
    return this._encodedReply(from, {
      ok: true,
      data: node.toJSON(true)
    });
  }

  /**
   *
   * @method weight
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Object} data
   * @param {String} data.id
   * @param {Object} from
   *
   * @return {Clsuterluck.CommandServer}
   *
   */
  weight(data, from) {
    const ring = this._gossip.ring();
    const weight = ring.weights().get(data.id);
    if (weight === undefined) {
      const msg = "'" + data.id + "' is not defined in this ring.";
      return this._encodedReply(from, {
        ok: false,
        error: utils.errorToObject(new Error(msg))
      });
    }
    return this._encodedReply(from, {
      ok: true,
      data: weight
    });
  }

  /**
   *
   * @method weights
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Any} data
   * @param {Object} from
   *
   * @return {Clsuterluck.CommandServer}
   *
   */
  weights(data, from) {
    const ring = this._gossip.ring();
    const weights = ring.weights();
    return this._encodedReply(from, {
      ok: true,
      data: utils.mapToObject(weights)
    });
  }

  /**
   *
   * @method ping
   * @memberof Clusterluck.CommandServer
   * @instance
   *
   * @param {Any} data
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  ping(data, from) {
    return this._encodedReply(from, {
      ok: true,
      data: "pong"
    });
  }

  /**
   *
   * @method _encodedReply
   * @memberof Clusterluck.CommandServer
   * @private
   * @instance
   *
   * @param {Object} from
   * @param {Object} msg
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  _encodedReply(from, msg) {
    const sendMsg = _.extend(NetKernel._encodeMsg(this._kernel.cookie(), _.extend(msg, {
      tag: from.tag
    })));
    this._kernel.ipc().server.emit(from.socket, "message", sendMsg);
    return this;
  } 

  /**
   *
   * @method _parse
   * @memberof Clusterluck.CommandServer
   * @private
   * @instance
   *
   * @param {Object} data
   * @param {Object} stream
   * @param {Object} from
   *
   * @return {Clusterluck.CommandServer}
   *
   */
  _parse(data, stream, from) {
    if (this._kernel.sinks().has(from.node.id()) ||
        from.node.id() === this._kernel.self().id()) return this;
    return super._parse(data, stream, from);
  }
}

module.exports = CommandServer;

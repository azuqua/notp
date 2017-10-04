var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    microtime = require("microtime"),
    debug = require("debug")("clusterluck:lib:command_server");

var GenServer = require("./gen_server"),
    NetKernel = require("./kernel"),
    Node = require("./node"),
    utils = require("./utils");

var commands = {
  "join": utils.hasID,
  "leave": _.identity,
  "meet": utils.parseNode,
  "insert": utils.parseNode,
  "minsert": utils.parseNodeList,
  "remove": utils.parseNode,
  "mremove": utils.parseNodeList,
  "inspect": _.identity,
  "nodes": _.identity,
  "has": utils.hasID,
  "get": utils.hasID,
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
      var handle = this[key].bind(this);
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
    var out = super.decodeJob(buf);
    if (out instanceof Error) return out;
    var data = out.data;
    if (commands[out.event] === undefined) {
      return new Error("Cannot run against unknown command '" + out.event + "'");
    }
    var parser = commands[out.event];
    data = parser(data);
    if (data instanceof Error) {
      return data;
    }
    out.data = data;
    return out;
  }

  decodeSingleton(data) {
    var out = super.decodeSingleton(data);
    if (commands[out.event] === undefined) {
      return new Error("Cannot run against unknown command '" + data.event + "'");
    }
    var parser = commands[out.event];
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
    var out = this._gossip.join(data.id);
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
    var force = data.force === true ? true : false;
    this._gossip.insert(data.node, force);
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
    var force = data.force === true ? true : false;
    var nodes = data.nodes;
    nodes = nodes.filter((node) => {return node.id() !== this._kernel.self().id();});
    this._gossip.minsert(nodes, force);
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
    var force = data.force === true ? true : false;
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
    var force = data.force === true ? true : false;
    var nodes = data.nodes;
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
    var ring = this._gossip.ring();
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
    var nodes = this._gossip.ring().nodes();
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
    var ring = this._gossip.ring();
    var node = new Node(data.id);
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
    var ring = this._gossip.ring();
    var node = ring.get(new Node(data.id));
    if (node === undefined) {
      var msg = "'" + data.id + "' is not defined in this ring.";
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
    var sendMsg = _.extend(NetKernel._encodeMsg(this._kernel.cookie(), _.extend(msg, {
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

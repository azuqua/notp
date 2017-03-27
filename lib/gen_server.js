var _ = require("lodash"),
    async = require("async"),
    shortid = require("shortid"),
    EventEmitter = require("events").EventEmitter,
    stream = require("stream"),
    util = require("util"),
    utils = require("./utils"),
    debug = require("debug")("clusterluck:lib:gen_server");

class GenServer extends EventEmitter {
  /**
   *
   * @class GenServer
   * @memberof Clusterluck
   *
   * @param {Clusterluck.NetKernel} kernel
   *
   */
  constructor(kernel, opts) {
    super();
    this._id = shortid.generate();
    this._kernel = kernel;
    this._streams = new Map();
    this._streamTimeout = (opts && opts.streamTimeout) || 30000;
  }

  /**
   *
   * @method start
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   * 
   * @param {String} [name] - Name to register this handler with instead of the unique id attached.
   *
   */
  start(name) {
    if (name) this._id = name;
    if (this._kernel.listeners(this._id).length > 0) {
      throw new Error("Kernel already has listener at id '" + this._id + "'");
    }
    var handler = this._parse.bind(this);
    this._kernel.on(this._id, handler);
    this.once("pause", _.partial(this._kernel.removeListener, this._id, handler).bind(this._kernel));
    return this;
  }

  /**
   *
   * @method stop
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   */
  stop(force = false) {
    this.pause();
    this.emit("stop");
    this._streams.forEach((val) => {
      clearTimeout(val.timeout);
    });
    this._streams.clear();
    this._id = shortid.generate();
    return this;
  }

  /**
   *
   * @method pause
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   */
  pause() {
    this.emit("pause");
    return this;
  }

  /**
   *
   * @method resume
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   */
  resume() {
    var handler = this._parse.bind(this);
    this._kernel.on(this._id, handler);
    this.once("pause", _.partial(this._kernel.removeListener, this._id, handler).bind(this._kernel));
    this.emit("resume");
    return this;
  }

  /**
   *
   * @method decodeJob
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   */
  decodeJob(job) {
    var out = utils.safeParse(job, (k, v) => {
      if (util.isObject(v) &&
          v.type === "Buffer" &&
          Array.isArray(v.data)) {
        return new Buffer(v);
      }
      return v;
    });
    if (out instanceof Error) return out;
    return {
      event: out.event,
      data: out.data
    };
  }

  /**
   *
   * Acts as a getter/setter for the ID of this instance.
   *
   * @method id
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {String} [id] - Handler ID to set on this instance.
   *
   * @return {String} The ID of this instance.
   *
   */
  id(id) {
    if (id !== undefined) {
      this._id = id;
    }
    return this._id;
  }

  /**
   *
   * Acts as a getter/setter for the netkernel of this instance.
   *
   * @method kernel
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Clusterluck.NetKernel} [kernel] - NetKernel to set on this instance.
   *
   * @return {Clusterluck.NetKernel} NetKernel of this instance.
   *
   */
  kernel(kernel) {
    if (kernel) {
      this._kernel = kernel;
    }
    return this._kernel;
  }

  /**
   *
   * Acts as a getter/setter for the internal message stream map of this instance.
   *
   * @method streams
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Map} [streams] - Internal message stream map to set on this instance.
   *
   * @return {Map} Internal message stream map of this instance.
   *
   */
  streams(streams) {
    if (streams) {
      this._streams = streams;
    }
    return this._streams;
  }

  /**
   *
   * Returns whether this handler is in an idle state or not.
   *
   * @method idle
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @return {Boolean} Whether this process is idle or not.
   *
   */
  idle() {
    return this._streams.size === 0;
  }

  /**
   *
   * @method reply
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Object} from
   * @param {Stream|Buffer} data
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  reply(from, data) {
    this._kernel.reply(from, data);
    return this;
  }

  /**
   *
   * @method call
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {String|Object} id
   * @param {String} event
   * @param {Buffer|String|Number|Boolean|Object|Array} data
   * @param {Function} cb
   * @param {Number} [timeout]
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  call(id, event, data, cb, timeout=Infinity) {
    var out = this._parseRecipient(id);
    this._kernel.call(out.node, out.id, JSON.stringify({
      event: event,
      data: data
    }), cb, timeout);
    return this;
  }

  /**
   *
   * @method multicall
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Array} nodes
   * @param {String} id
   * @param {String} event
   * @param {Stream|Buffer} data
   * @param {Function} cb
   * @param {Number} [timeout]
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  multicall(nodes, id, event, data, cb, timeout=Infinity) {
    this._kernel.multicall(nodes, id, JSON.stringify({
      event: event,
      data
    }), cb, timeout);
    return this;
  }

  /**
   *
   * @method cast
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {String|Object} id
   * @param {String} event
   * @param {Stream|Buffer} data
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  cast(id, event, data) {
    var out = this._parseRecipient(id);
    this._kernel.cast(out.node, out.id, JSON.stringify({
      event: event,
      data: data
    }));
    return this;
  }

  /**
   *
   * @method abcast
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Array} nodes
   * @param {String} id
   * @param {String} event
   * @param {Stream|Buffer} data
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  abcast(nodes, id, event, data) {
    this._kernel.abcast(nodes, id, JSON.stringify({
      event: event,
      data: data
    }));
    return this;
  }

  /**
   *
   * @method _parse
   * @memberof Clusterluck.GenServer
   * @private
   * @instance
   *
   */
  _parse(data, stream, from) {
    if (!this._streams.has(stream.stream)) {
      var t = this._registerTimeout(stream, from);
      this._streams.set(stream.stream, {
        data: Buffer.from(""),
        timeout: t
      });
    }
    var inner = this._streams.get(stream.stream);
    if (data) {
      inner.data = Buffer.concat([inner.data, data], inner.data.length + data.length);
      this._streams.set(stream.stream, inner);
      if (!stream.done) return this;
    }
    clearTimeout(inner.timeout);
    if (!stream.error) {
      var job = this.decodeJob(inner.data);
      // maybe reply to msg stream with error, but stream error occurs sender-side,
      // so they probably know if an error occurred (or at least should)
      if (!(job instanceof Error)) this.emit(job.event, job.data, from);
    }

    this._streams.delete(stream.stream);
    if (this._streams.size === 0) {
      this.emit("idle");
    }
    return this;
  }

  /**
   *
   * @method _parseRecipient
   * @memberof Clusterluck.GenServer
   * @private
   * @instance
   *
   */
  _parseRecipient(id) {
    var node;
    if (typeof id === "string") {
      node = this._kernel.self();
    }
    else if (util.isObject(id)) {
      node = id.node;
      id = id.id;
    }
    return {id: id, node: node};
  }

  /**
   *
   * @method _registerTimeout
   * @memberof Clusteluck.GenServer
   * @private
   * @instance
   *
   */
  _registerTimeout(istream, from) {
    // just remove stream for now, it'll invalidate any gathered JSON and fail `decodeJob`
    var t = setTimeout(() => {
      if (!this._streams.has(istream.stream)) return;
      var rstream = new stream.PassThrough();
      var out = this._safeReply(from, rstream);
      if (out) rstream.emit("error", new Error("Timeout."));
      this._streams.delete(istream.stream);
      if (this._streams.size === 0) {
        this.emit("idle");
      }
    }, this._streamTimeout);
    return t;
  }

  /**
   *
   * @method _safeReply
   * @memberof Clusterluck.GenServer
   * @private
   * @instance
   *
   */
  _safeReply(from, data) {
    if (typeof from.tag !== "string") return false;
    this._kernel.reply(from, data);
    return true;
  }
}

module.exports = GenServer;

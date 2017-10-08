const _ = require("lodash"),
      shortid = require("shortid"),
      EventEmitter = require("events").EventEmitter,
      stream = require("stream"),
      util = require("util"),
      utils = require("./utils"),
      debug = require("debug")("clusterluck:lib:gen_server");

class GenServer extends EventEmitter {
  /**
   *
   * Generic server implementation. Basic unit of logic handling in clusterluck. Provides the ability to send/receive messages across a cluster using `call`, `cast`, `multicall`, and `abcast`. Largely derived from Erlang's gen_server model, but incorporated into node.js' EventEmitter model. Internally, message streams are memoized until finished, which then triggers the firing of an event for given event (say we're listening for event "hello", if this server receives a message stream with event "hello", we'll fire it under the hood). In addition to message streams, message singletons are supported, which are message streams with a singleton message and bypass any stream memoization.
   *
   * Think of it like event emitters that are targetable across a cluster, as well as internal to a node.
   *
   * @class GenServer
   * @memberof Clusterluck
   *
   * @param {Clusterluck.NetKernel} kernel - Network kernel this instance will listen for messages on.
   *
   */
  constructor(kernel, opts) {
    super();
    this._id = shortid.generate();
    this._kernel = kernel;
    this._streams = new Map();
    this._streamTimeout = (opts && opts.streamTimeout) || 30000;
    this._paused = false;
  }

  /**
   *
   * Starts this GenServer. Use this to start processing messages. As a result of this function call, any message with an id equal to this server's id (or name, if provided) will be routed by the network kernel to this server. If called multiple times in a row, this function will throw, since name uniqueness is enforced by the network kernel. If no name is provided, the generated short id at construction-time will be used as the routing name.
   *
   * @method start
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @listens Clusterluck.GenServer#GenServer:pause
   * @listens Clusterluck.NetKernel#NetKernel:user_defined
   * 
   * @param {String} [name] - Name to register this handler with instead of the unique id attached. Any message received on the network kernel with id `name` will be routed to this instance for message stream parsing, and possible event firing.
   *
   */
  start(name) {
    if (name) this._id = name;
    if (this._kernel.listeners(this._id).length > 0) {
      throw new Error("Kernel already has listener at id '" + this._id + "'");
    }
    const handler = this._parse.bind(this);
    this._kernel.on(this._id, handler);
    this.once("pause", _.partial(this._kernel.removeListener, this._id, handler).bind(this._kernel));
    return this;
  }

  /**
   *
   * Stops the processing of messages on this GenServer. As a result, any further messages sent to the network kernel will fail to route to this GenServer. Additionally, all current message streams will be discarded, and the current name will be regenerated to another short id.
   *
   * @method stop
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:stop
   *
   */
  stop(force = false) {
    this.pause();

    /**
     *
     * Emitted when the server has completely stopped executing handler logic on any user-defined events it's listening for.
     *
     * @event Clusterluck.GenServer#GenServer:stop
     * @memberof Clusterluck.GenServer
     *
     * @example
     * // in any subclasses og GenServer, we could make stop fire asynchronously
     * server.on("stop", () => {
     *   console.log("We've stopped!");
     * });
     * server.stop();
     *
     */
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
   * Pauses the GenServer's message processing. As a result, any messages routed from the network kernel to this GenServer will be missed until `this.resume()` is called.
   *
   * @method pause
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:pause
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  pause() {
    /**
     *
     * Emitted when the server has paused. On 'pause', if the server is started with name `name`, the network kernel will remove this server's event handler for `name`.
     * 
     * @event Clusterluck.GenServer#GenServer:pause
     * @memberof Clusterluck.GenServer
     *
     * @example
     * let name = server.id();
     * let old = server.kernel().listeners(name).length;
     * server.once("pause", () => {
     *   assert.lengthOf(server.kernel().listeners(name), old-1);
     * });
     * server.pause();
     *
     */
    this._paused = true;
    this.emit("pause");
    return this;
  }

  /**
   *
   * Resumes the processing of message streams on this GenServer. Any messages missed in between pausing and now will result in failed message parsing (since all JSON contents are sent in one message).
   *
   * @method resume
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:resume
   * @listens Clusterluck.GenServer#GenServer:pause
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  resume() {
    const handler = this._parse.bind(this);
    this._kernel.on(this._id, handler);
    this.once("pause", _.partial(this._kernel.removeListener, this._id, handler).bind(this._kernel));

    /**
     *
     * Emitted when the server has been resumed. Before 'resume' is emitted, the network kernel restarts the server's event handler for `name`, where `name` is the name of the server.
     *
     * @event Clusterluck.GenServer#GenServer:resume
     * @memberof Clusterluck.GenServer
     *
     * @example
     * let name = server.id();
     * let old = server.kernel().listeners(name).length;
     * server.once("resume", () => {
     *   assert.lengthOf(server.kernel().listeners(name), old+1);
     * });
     * server.resume();
     *
     */
    this._paused = false;
    this.emit("resume");
    return this;
  }

  /**
   *
   * Parses a fully memoized message stream into an object containing a key/value pair. If we fail to parse the job buffer (invalid JSON, etc), we just return an error and this GenServer will skip emitting an event. Otherwise, triggers user-defined logic for the parsed event.
   *
   * @method decodeJob
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @param {Buffer} job - Memoized buffer that represents complete message stream.
   *
   * @return {Object} Object containing an event and data key/value pair, which are used to emit an event for user-defined logic.
   *
   */
  decodeJob(job) {
    const out = utils.safeParse(job, (k, v) => {
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
   * Parses a message singleton into an object containing a key/value pair. If we fail to parse the job, we just return an error and this GenServer will skip emitting an event. Otherwise, triggers user-defined logic for the parsed event.
   *
   * @method decodeJob
   * @memberof Clusterluck.GenServer
   * @abstract
   * @instance
   *
   * @param {Object} job - Memoized buffer that represents complete message stream.
   *
   * @return {Object} Object containing an event and data key/value pair, which are used to emit an event for user-defined logic.
   *
   */
  decodeSingleton(data) {
    if (util.isObject(data) &&
        util.isObject(data.data) &&
        data.data.type === "Buffer" &&
        Array.isArray(data.data.data)) {
      data.data = Buffer.from(data.data);
    }
    return data;
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
   * Replies to the sending node for an event stream. Used when a triggered event contains synchronous logic, in which we need to respond to the calling node of this event.
   *
   * @method reply
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Object} from - Object received on synchronous message. This contains a tag to uniquely identify the request on the sender's end.
   * @param {String} from.tag - Unique identifer for request.
   * @param {Clusterluck.Node} from.node - Node representing the sender.
   * @param {Buffer|String|Number|Boolean|Object|Array} data - Data to reply to synchronous message with.
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
   * Makes a synchronous request against `id` with event `event` and event args `data`. If `id` is an object, it will be parsed into a GenServer id and a receiving node. If a callback is supplied, the response will be aggregated and returned on success as `cb(null, res)` or on error as `cb(err)`. In either case, the response stream is returned, which can be used to pipe to other streams.
   *
   * @method call
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {String|Object} id - Receiving GenServer. Can be a string, in which case this message will be sent locally to `id`. If an object, will be parsed into a receiving node and GenServer id.
   * @param {String} event - Event this message will be emitted under on the receiving GenServer.
   * @param {Buffer|String|Number|Boolean|Object|Array} data - Data to send with this message.
   * @param {Function} [cb] - Optional callback for response/error handling.
   * @param {Number} [timeout] - Timeout to wait for response before returning an error. If no callback is supplied, the response stream will emit an equivalent error. Defaults to Infinity, meaning no timeout will occur.
   *
   * @return {Stream} Response stream for request.
   *
   */
  call(id, event, data, cb, timeout=Infinity) {
    const out = this._parseRecipient(id);
    return this._kernel.callSingleton(out.node, out.id, {
      event: event,
      data: data
    }, cb, timeout);
  }

  /**
   *
   * Analogous to `call`, but makes the call to the GenServer with name `id` on each node in `nodes`.
   *
   * @method multicall
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Array} nodes - Array of nodes to send this message to.
   * @param {String} id - Receiving GenServer name.
   * @param {String} event - Event this message will be emitted under on the receiving GenServer.
   * @param {Buffer|String|Number|Boolean|Object|Array} data - Data to send with this message.
   * @param {Function} [cb] - Optional callback for response/error handling.
   * @param {Number} [timeout] - Timeout to wait for response before returning an error. If no callback is supplied, the response stream will emit an equivalent error. Defaults to Infinity, meaning no timeout will occur.
   *
   * @return {Array} Array of response streams.
   *
   */
  multicall(nodes, id, event, data, cb, timeout=Infinity) {
    return this._kernel.multicallSingleton(nodes, id, {
      event: event,
      data
    }, cb, timeout);
  }

  /**
   *
   * Makes an asynchronous request against `id` with event `event` and event args `data`. If `id` is an object, it will be parsed into a GenServer id and a receiving node.
   *
   * @method cast
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {String|Object} id - Receiving GenServer. Can be a string, in which case this message will be sent locally to `id`. If an object, will be parsed into a receiving node and GenServer id.
   * @param {String} event - Event this message will be emitted under on the receiving GenServer.
   * @param {Buffer|String|Number|Boolean|Object|Array} data - Data to send with this message.
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  cast(id, event, data) {
    const out = this._parseRecipient(id);
    this._kernel.cast(out.node, out.id, {
      event: event,
      data: data
    });
    return this;
  }

  /**
   *
   * Analogous to `cast`, but makes the cast to the GenServer with name `id` on each node in `nodes`.
   *
   * @method abcast
   * @memberof Clusterluck.GenServer
   * @instance
   *
   * @param {Array} nodes - Array of nodes to send this message to.
   * @param {String} id - Receiving GenServer.
   * @param {String} event - Event this message will be emitted under on the receiving GenServer.
   * @param {Buffer|String|Number|Boolean|Object|Array} data - Data to send with this message.
   *
   * @return {Clusterluck.GenServer} This instance.
   *
   */
  abcast(nodes, id, event, data) {
    this._kernel.abcast(nodes, id, {
      event: event,
      data: data
    });
    return this;
  }

  /**
   *
   * @method _parse
   * @memberof Clusterluck.GenServer
   * @private
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:user_defined
   * @fires Clusterluck.GenServer#GenServer:idle
   *
   */
  _parse(data, stream, from) {
    if (this._paused === true) return this;
    if (stream.done === true && data !== null && !stream.stream) {
      data = this.decodeSingleton(data);
      if (!(data instanceof Error)) this.emit(data.event, data.data, from);
      return this;
    } else if (stream.done === true && stream.error !== undefined && !stream.stream) {
      return this;
    }
    
    if (!this._streams.has(stream.stream)) {
      const t = this._registerTimeout(stream, from);
      this._streams.set(stream.stream, {
        data: Buffer.from(""),
        timeout: t
      });
    }
    const inner = this._streams.get(stream.stream);
    if (data) {
      inner.data = Buffer.concat([inner.data, data], inner.data.length + data.length);
      this._streams.set(stream.stream, inner);
      if (!stream.done) return this;
    }
    clearTimeout(inner.timeout);
    if (!stream.error) {
      const job = this.decodeJob(inner.data);
      // maybe reply to msg stream with error, but stream error occurs sender-side,
      // so they probably know if an error occurred (or at least should)
      if (!(job instanceof Error)) this.emit(job.event, job.data, from);
    }

    this._streams.delete(stream.stream);
    if (this.idle()) {
     /**
      *
      * Emitted when the number of active messages streams on this server is zero (or, if extending the GenServer class, when this.idle() returns true).
      *
      * @event Clusterluck.GenServer#GenServer:idle
      * @memberof Clusterluck.GenServer
      *
      * @example
      * server.once("idle", () => {
      *   // now we could safely stop the server w/o killing any message streams
      * });
      *
      */
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
    let node;
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
   * @fires Clusterluck.GenServer#GenServer:idle
   *
   */
  _registerTimeout(istream, from) {
    // just remove stream for now, it'll invalidate any gathered JSON and fail `decodeJob`
    const t = setTimeout(() => {
      if (!this._streams.has(istream.stream)) return;
      const rstream = new stream.PassThrough();
      const out = this._safeReply(from, rstream);
      if (out) rstream.emit("error", new Error("Timeout."));
      this._streams.delete(istream.stream);
      if (this.idle()) {
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
 
/**
 *
 * Events defined by users for message handling.
 *
 * Suppose we have a GenServer named `server` started with name `name_goes_here`. Any message stream routed to our node, both internal and external, with id set to `name_goes_here` will be gathered by `serve` into a map of streams. Each stream has a stream ID `stream` and a boolean flag `done` which indicates whether the stream has finished sending data. This stream ID is used as an index for stream memoization between concurrent requests.
 * 
 * ```javascript
 * // 'data' is the input data for this part of the stream
 * if (!this._streams.has(stream.stream)) {
 *   this._streams.set(stream.stream, {data: Buffer.from(""), ...});
 * }
 * let inner = this._streams.get(stream.stream);
 * inner.data = Buffer.concat([inner.data, data], ...);
 * ```
 *
 * Once sending has finished, the job is parsed using `decodeJob` into an event and message, under `event` and `data` respectively. From here, the server emits an event just like an EventEmitter with `event` and `data`.
 *
 * ```javascript
 * // similar to this
 * if (stream.done === true) {
 *   let job = this.decodeJob(memo);
 *   serve.emit(job.event, job.data, from);
 * }
 * ```
 *
 * This is where our own event handling logic comes into play. Everything else is handled under the hood by this class.
 *
 * Diagram of data flow for message streams:
 * ```
 * Messages comes into node with id set to `name_goes_here` ---------------> this._parse
 * Message comes into node with `stream.done` set to true -----------------> this.decodeJob ---> this.emit(event, data,from) ---> this.on(event, handlerLogic...)
 * ```
 *
 * As for singleton message streams (a message stream with only one message, and `done` set to true), no memoization of streams into a stream map occurs. The data is parsed by `decodeSingleton` into an event and message, under `event` and `data` respectively. From here, the server emits an event just like an EventEmitter with `event` and `data`.
 *
 * Diagram of data flow for message singletons:
 * ```
 * Messages comes into node with id set to `name_goes_here` ---------------> this._parse ---> this.decodeSingleton ---> this.emit(event, data,from) ---> this.on(event, handlerLogic...)
 * ```
 *
 * @event Clusterluck.GenServer#GenServer:user_defined
 * @memberof Clusterluck.GenServer
 * @property {Any} data - Data emitted with this user-defined event. Analogous to how an EventEmitter emits events.
 * @property {Object} from - Object received on synchronous message. This contains a tag to uniquely identify the request on the sender's end.
 * @property {String} from.tag - Unique identifer for request.
 * @property {Clusterluck.Node} from.node - Node representing the sender.
 *
 * @example
 * server.on("hello", (data, from) => {
 *   server.reply(from, "world");
 * });
 *
 */
module.exports = GenServer;

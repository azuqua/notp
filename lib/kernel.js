var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    EventEmitter = require("events").EventEmitter,
    stream = require("stream"),
    crypto = require("crypto"),
    util = require("util"),
    debug = require("debug")("clusterluck:lib:kernel");

var Node = require("./node"),
    Connection = require("./conn"),
    utils = require("./utils");

class NetKernel extends EventEmitter {

  /**
   *
   * Networking kernel implementation, responsible for routing messages to/from internal/external nodes, which processors can then read from. Messages can be either synchronous or asynchronous, and can be sent to single nodes or to a list of nodes simulataneously. On start, listens on hostname `host` and port `port` for IPC messages. Maintains two maps: the `sink` map contains information about which nodes this kernel has IPC connections to, while the `source` map contains the sockets of nodes this kernel has connections from. To ensure messages are only from members of a cluster, the kernel has a secret cookie responsible for calculating hmac signatures of incoming and outgoing messages. Any messages that don't match the hmac signature calculated with this cookie are ignored.
   *
   * @class NetKernel NetKernel
   * @memberof Clusterluck
   *
   * @param {IPC} ipc - IPC instance to route node communication through.
   * @param {String} id - Unique identifier of this node.
   * @param {String} host - The hostname this node will listen on. Can be either an IP address (longname) or a hostname (shortname).
   * @param {Number} port - The port this node will listen on.
   *
   */
  constructor(ipc, id, host, port) {
    super();
    this._ipc = ipc;
    this._id = id;
    this._host = host;
    this._port = port;
    this._self = new Node(id, host, port);
    this._srcs = new Map();
    this._sinks = new Map();
    this._cookie = null;
  }

  /**
   *
   * Starts the IPC server on the configured host and port, as well as the routing routine. Can be configured with a cluster cookie (for hmac signature cluster member checks), the retry interval for failed message sends, the number of times to retry sending a message, and whether to use TLS on TCP sockets between nodes or not.
   *
   * @method start
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Object} [opts] - Options object for IPC server configuration between nodes.
   * @param {Number} [opts.retry] - Retry internval for failed message sends.
   * @param {Number} [opts.maxRetries] - Maximum number of times to retry sending a message.
   * @param {String} [opts.cookie] - Cluster cookie to use for hmac signature verification. If undefined or null, will default to not checking cluster membership.
   * @param {Object} [opts.tls] - TLS configuration object for IPC socket communication.
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  start(opts) {
    opts = util.isObject(opts) && !Array.isArray(opts) ? opts : {};
    this._ipc.config.id = this._id;
    this._ipc.config.retry = opts.retry || this._ipc.config.retry;
    this._ipc.config.maxRetries = opts.maxRetries || this._ipc.config.maxRetries;
    this._cookie = opts.cookie || null;
    this._ipc.config.tls = opts.tls || this._ipc.config.tls;
    this._ipc.serveNet(this._host, this._port, this._startData.bind(this));
    this._ipc.server.start();
    return this;
  }

  /**
   *
   * Stops the IPC server on this node and empties any sockets receiving data from external nodes. Maintains connections for sending messages to external nodes. All listening processes should be terminated before calling this function, in order to safely finish any job streams.
   *
   * @method stop
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @fires Clusterluck.NetKernel#NetKernel:_stop
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  stop() {
    debug("Stopping network kernel on " + this._id);
    this._ipc.server.stop();
    this._srcs = new Map();

    /**
     *
     * Emitted when this instance has stopped it's IPC server and stopped receiving messages from other nodes.
     *
     * @event Clusterluck.NetKernel#NetKernel:_stop
     * @memberof Clusterluck.NetKernel
     *
     */
    this.emit("_stop");
    return this;
  }

  /**
   *
   * Acts as a getter/setter for the IPC instance of this netkernel. Only safe to do when this instance is in an unstarted state.
   *
   * @method ipc
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {IPC} [ipc] - IPC instance to set on this netkernel.
   *
   * @return {IPC} IPC instance of this kernel.
   *
   */
  ipc(ipc) {
    if (ipc !== undefined) {
      this._ipc = ipc;
    }
    return this._ipc;
  }

  /**
   *
   * Acts as a getter/setter for the unique identifier of this netkernel, modifying the ID value of the `self` node if used as a setter.
   *
   * @method id
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {String} [id] - ID to set on this netkernel.
   *
   * @return {String} ID of this instance.
   *
   */
  id(id) {
    if (typeof id === "string") {
      this._id = id;
      this._self.id(id);
    }
    return this._id;
  }

  /**
   *
   * Acts as a getter/setter for the hostname of this netkernel, modifying the host value of the `self` node if used as a setter. Can be either a shortname (host) or a longname (ip).
   *
   * @method host
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {String} [host] - Host to set on this netkernel.
   *
   * @return {String} Host of this instance.
   *
   */
  host(host) {
    if (typeof host === "string") {
      this._host = host;
      this._self.host(host);
    }
    return this._host;
  }

  /**
   *
   * Acts as a getter/setter for the port of this netkernel, modifying the port value of the `self` node if used as a setter.
   *
   * @method port
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Number} [port] - Port to set on this netkernel.
   *
   * @return {Number} Port of this instance.
   *
   */
  port(port) {
    if (typeof port === "number") {
      this._port = port;
      this._self.port(port);
    }
    return this._port;
  }

  /**
   *
   * Acts as a getter/setter for the node representing this netkernel, modifying the id, port, and host of this instance if used as a setter.
   *
   * @method self
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} [node] - Node to set as "self" on this netkernel.
   *
   * @return {Clusterluck.Node} Self-referencing node of this instance.
   *
   */
  self(node) {
    if (typeof node === "object") {
      this._self = node;
      this._id = node.id();
      this._port = node.port();
      this._host = node.host();
    }
    return this._self;
  }

  /**
   *
   * Acts as a getter/setter for the source map this netkernel has sockets receiving data from.
   *
   * @method sources
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Map} [sources] - Map of (node => socket connections) to set on this netkernel.
   *
   * @return {Map} Map of socket connections of this instance.
   *
   */
  sources(sources) {
    if (sources instanceof Map) {
      this._srcs = sources;
    }
    return this._srcs;
  }

  /**
   *
   * Acts as a getter/setter for the sink map this netkernel has external connections to.
   *
   * @method sinks
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Map} [sinks] - Map of (node id => node) to set on this netkernel.
   *
   * @return {Map} Map of external node connections of this instance.
   *
   */
  sinks(sinks) {
    if (sinks instanceof Map) {
      this._sinks = sinks;
    }
    return this._sinks;
  }

  /**
   *
   * Acts as a getter/setter for the cookie this netkernel uses for cluster member verification.
   *
   * @method cookie
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {String} [cookie] - Cookie to set on this netkernel.
   *
   * @return {String} Cookie of this instance.
   *
   */
  cookie(cookie) {
    if (typeof cookie === "string") {
      this._cookie = cookie;
    }
    return this._cookie;
  }

  /**
   *
   * Creates a socket connection to external node `node`, which this netkernel can use to send messages to `node`. If `node` is identical to `this.self()` or a connection already exists, this will immediately return. Otherwise, creates the connection and inserts `node` into this netkernel's sink map.
   *
   * @method connect
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} node - Node to create socket connection with.
   * @param {Function} [cb] - Callback to call once this process has finished.
   *
   */
  connect(node, cb) {
    if (node.id() === this._id || this._sinks.has(node.id())) {
      if (typeof cb === "function") return cb();
      return this;
    }
    debug("Connecting to IPC server on node " + node.id());
    var conn = new Connection(this._ipc, node);
    conn.start();
    this._sinks.set(node.id(), conn);
    if (typeof cb === "function") {
      conn.once("connect", cb);
    }
    return this;
  }

  /**
   *
   * Disconnects socket connection to `node`. If `node` is identical to `this.self()` or a connection doesn't exist, this will immediately return. Otherwise, terminates the connection and removes `node` from this netkernel's sink and source map.
   *
   * @method disconnect
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} node
   * 
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  disconnect(node, force = false) {
    if (node.id() === this._id || !this._sinks.has(node.id())) return this;
    debug("Disconnecting from IPC server on node " + node.id());
    this._sinks.get(node.id()).stop(force);
    this._sinks.delete(node.id());
    return this;
  }

  /**
   *
   * Checks if a connections exists with `node`.
   *
   * @method isConnected
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} node - Node to check existing connection to.
   * 
   * @return {Boolean} Whether an existing connection exists.
   *
   */
  isConnected(node) {
    if (node.id() === this._id) return true;
    return this._sinks.has(node.id());
  }

  /**
   *
   * Grabs the connection object between this netkernel and `node`.
   *
   * @method connection
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} node - Node to grab connection of.
   *
   * @return {Clusterluck.Connection} Connection of `node`, or null if it doesn't exist.
   *
   */
  connection(node) {
    if (node.id() === this._id) return null;
    return this._sinks.get(node.id()) || null;
  }

  /**
   *
   * Makes a synchronous call to external node `node`, streaming `data` over and then waits for a complete response. To accomplish this, a tag is passed that uniquely identifies the returnee. This netkernel then listens for messages with the event ID'd as this tag and passes data into the return stream, closing the stream when a `done` message is passed for this tag. If `cb` is passed, the return stream is collected into a single Buffer and then returned. Otherwise, the return stream is given to the caller to do manual data collection and error handling.
   *
   * @method call
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} node - Node to send `data` to.
   * @param {String} event - Event this message is sent under.
   * @param {Stream|Buffer|String} data - Data to send with this message. `data` is coerced into a stream format, throwing on failed coersion.
   * @param {Function} [cb] - Optional callback to collect stream data and handle error reporting. Useful for smaller payloads with minimal memory footprints. Has two parameters: the first is error for when an error occurs at any point in the request, and the second is a returned Buffer on successful completion.
   * 
   * @return {Stream} If `cb` is not passed, the return stream.
   *
   * @example
   * // with callback
   * kernel.call(node, "job", "hello", (err, data) => {
   *   // data is a Buffer on success
   *   // ...
   * });
   *
   * @example
   * // w/o callback
   * var rstream = kernel.call(node, "job", "hello");
   * rstream.on("data", (data) => {
   *   // data handler ...
   * }).on("error", (error) => {
   *   // error handler ...
   * }).on("end", () => {
   *   // end handler ...
   * });
   *
   */
  call(node, event, data, cb, timeout=Infinity) {
    var id = uuid.v4();
    data = NetKernel._coerceStream(data);
    if (node.id() === this._id) {
      this._streamLocal(event, id, data);
    }
    else if (!this._sinks.has(node.id())) return null;
    else {
      this._streamData(node, event, id, data);
    }
    var rstream = this._rstream(node, id, timeout);
    if (typeof cb === "function") {
      return NetKernel._collectStream(rstream, cb);
    }
    return rstream;
  }

  /**
   *
   * Makes a synchronous call to a list of external nodes `nodes`, streaming `data` over and then waits for a complete response from all nodes (using `this.call(...)` on each node in `nodes`). If `cb` is passed, each return stream is collected into a single Buffer and then returned. Otherwise, the list of return streams is given to the caller to do manual data collection and error handling. If `cb` is passed and any node in the list incurs an error at any point, it's called with this error and any further processing on the other nodes is ignored.
   *
   * @method multicall
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Array} nodes - Nodes to send `data` to.
   * @param {String} event - Event this message is sent under.
   * @param {Stream|Buffer|String} data - Data to send with each message. `data` is coerced into a stream format, throwing on failed coersion.
   * @param {Function} [cb] - Optional callback to wait for collect stream data and handle error reporting on each node in `nodes`. Useful for smaller payloads with minimal memory footprints. Has two parameters: the first is an error for when an error occurs at any point in the request for any node, and the second is an array of returned Buffers on successful completion.
   * 
   * @return {Array} If `cb` is passed, an array of return streams.
   *
   * @example
   * // with callback
   * kernel.multicall([node1, node2], "job", "hello", (err, data) => {
   *   // data is an array of Buffers on success
   *   // ...
   * });
   *
   * @example
   * // w/o callback
   * var rstreamList = kernel.multicall([node1, node2], "job", "hello");
   * rstreamList.forEach((rstream) => {
   *   rstream.on("data", (data) => {
   *     // data handler ...
   *   }).on("error", (error) => {
   *     // error handler ...
   *   }).on("end", () => {
   *     // end handler ...
   *   });
   * });
   *
   */
  multicall(nodes, event, data, cb, timeout=Infinity) {
    data = NetKernel._coerceStream(data);
    if (typeof cb !== "function") {
      return nodes.map(_.partialRight(this.call, event, data, _, timeout).bind(this));
    }
    return async.map(nodes, _.partialRight(this.call, event, data, _, timeout).bind(this), cb);
  }

  /**
   *
   * Replies to a synchronous message received on this netkernel.
   *
   * @method reply
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Object} from - From object received on synchronous message. This contains a tag to uniquely identity the request on the sender's end.
   * @param {String} from.tag - Unique identifier for request.
   * @param {Clusterluck.Node} from.node - Node representing the sender.
   * @param {Stream|Buffer|String} data - Data to send with this message. `data` is coerced into a stream format, throwing on failed coersion.
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  reply(from, data) {
    var err;
    if (typeof from.tag !== "string") {
      err = new Error("Cannot reply to message with no callback tag.");
      throw err;
    }
    this.cast(from.node, from.tag, data);
    return this;
  }

  /**
   *
   * Makes an asynchronous call to external node `node`, streaming `data` over. Ignores any error handling or if no connection exists to `node`.
   *
   * @method cast
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Clusterluck.Node} node - Node to send `data` to.
   * @param {String} event - Event this message is sent under.
   * @param {Stream|Buffer|String} data - Data to send with this message. `data` is coerced into a stream format, throwing on failed coersion.
   * 
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  cast(node, event, data) {
    data = NetKernel._coerceStream(data);
    if (node.id() === this._id) {
      return this._streamLocal(event, null, data);
    }
    if (!this._sinks.has(node.id())) return;
    this._streamData(node, event, null, data);
    return this;
  }

  /**
   *
   * Makes an asynchronous call to an array of external nodes `nodes`, streaming `data` over. Ignores any error handling or if no connection exists to any node in `nodes`.
   *
   * @method abcast
   * @memberof Clusterluck.NetKernel
   * @instance
   *
   * @param {Array} nodes - Nodes to send `data` to.
   * @param {String} event - Event this message is sent under.
   * @param {Stream|Buffer|String} data - Data to send with this message. `data` is coerced into a stream format, throwing on failed coersion.
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  abcast(nodes, event, data) {
    // to avoid creating N duplicates of data
    data = NetKernel._coerceStream(data);
    nodes.forEach(_.partial(this.cast, _, event, data).bind(this));
    return this;
  }

  /**
   *
   * @method _startData
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @fires Clusterluck.NetKernel#NetKernel:_ready
   * @fires Clusterluck.NetKernel#NetKernel:user_defined
   *
   */
  _startData() {
    this._ipc.server.on("message", (data, socket) => {
      data = NetKernel._decodeMsg(this._cookie, data);
      if (data instanceof Error) {
        return this._skipMsg(data);
      }
      this._addSocket(socket);
      var nData = NetKernel._decodeBuffer(data.data);
      debug("Received message on net kernel with stream:", data.stream.stream + ",", "event:", data.id + ",", "from:", data.from.id);

      /**
       *
       * Emitted whenever this instance has received a message that has passed security checks and ready to be routed to a GenServer/event handler.
       *
       * @event Clusterluck.NetKernel#NetKernel:user_defined
       * @memberof Clusterluck.NetKernel
       * @property {String} id - Local target of this message (corresponding to a GenServer).
       * @property {Buffer} data - Data buffer of message.
       * @property {Object} stream - Stream object for this message.
       * @property {String} stream.stream - ID of stream.
       * @property {Boolean} stream.done - Whether this stream has more data coming or not.
       * @property {Object} from - From object received on a message. This contains a tag to uniquely identity the request on the sender's end.
       * @property {String} from.tag - Unique identifier for request.
       * @property {Clusterluck.Node} from.node - Node representing the sender.
       *
       */
      this.emit(data.id, nData, data.stream, {
        tag: data.tag || null,
        socket: socket,
        node: Node.from(data.from)
      });
    });
    this._ipc.server.on("socket.disconnected", this._disconnectSocket.bind(this));

    /**
     *
     * Emitted when this instance's IPC server has started, and ready to receive messages from other nodes.
     *
     * @event Clusterluck.NetKernel#NetKernel:_ready
     * @memberof Clusterluck.NetKernel
     *
     */
    this.emit("_ready");
  }

  /**
   *
   * @method _streamLocal
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {String} event
   * @param {String} tag
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _streamLocal(event, tag, data) {
    var streamID = uuid.v4();
    var rstream = {stream: streamID, done: false};
    debug("Streaming data locally,", "stream: " + streamID + ",", "event: " + event + ",", "tag: " + tag);
    var dataListener = _.partial(this._sendLocal, event, tag, rstream).bind(this);
    data.on("data", dataListener);
    data.once("end", _.partial(this._finishLocal, event, tag, rstream).bind(this));
    data.on("error", (err) => {
      data.removeListener("data", dataListener);
      if (data.resume) data.resume();
      this._sendLocalError(event, tag, rstream, err);
    });
    return this;
  }

  /**
   *
   * @method _sendLocal
   * @memberof NetKernel.Netkernel
   * @instance
   * @private
   *
   * @fires Clusterluck.NetKernel#NetKernel:user_defined
   *
   * @param {String} event
   * @param {String} tag
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _sendLocal(event, tag, stream, data) {
    this.emit(event, data, _.clone(stream), {
      tag: tag,
      node: this._self
    });
    return this;
  }

  /**
   *
   * @method _finishLocal
   * @memberof NetKernel.Netkernel
   * @instance
   * @private
   *
   * @param {String} event
   * @param {String} tag
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _finishLocal(event, tag, stream) {
    if (stream.done) return this;
    stream.done = true;
    return this._sendLocal(event, tag, stream, null);
  }

  /**
   *
   * @method _sendLocalError
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {String} event
   * @param {String} tag
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _sendLocalError(event, tag, stream, data) {
    if (stream.done) return this;
    stream.done = true;
    stream.error = NetKernel._encodeError(data);
    data = null;
    return this._sendLocal(event, tag, stream, data);
  }

  /**
   *
   * @method _streamData
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {Clusterluck.Node} node
   * @param {String} event
   * @param {String} tag
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _streamData(node, event, tag, data) {
    var streamID = uuid.v4();
    var rstream = {stream: streamID, done: false};
    var conn = this.connection(node);
    debug("Streaming data to " + node.id() + ",", "stream: " + streamID + ",", "event: " + event + ",", "tag: " + tag);
    var dataListener = _.partial(this._sendData, data, conn, event, tag, rstream).bind(this);
    data.on("data", dataListener);
    data.once("end", _.partial(this._finishData, data, conn, event, tag, rstream).bind(this));
    data.on("error", (err) => {
      data.removeListener("data", dataListener);
      if (data.resume) data.resume();
      this._sendError(data, conn, event, tag, rstream, err);
    });
    // mark conn to not be idle
    conn.initiateStream(rstream);
    return this;
  }

  /**
   *
   * @method _sendData
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {Clusterluck.Connection} conn
   * @param {String} event
   * @param {String} tag
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _sendData(source, conn, event, tag, stream, data) {
    data = NetKernel._encodeBuffer(data);
    data = NetKernel._encodeMsg(this._cookie, {
      id: event,
      tag: tag,
      from: this._self.toJSON(true),
      stream: _.clone(stream),
      data: data
    });
    var out = conn.send("message", data);
    if (out instanceof Error) source.emit("error", out);
    return this;
  }

  /**
   *
   * @method _finishData
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {Clusterluck.Connection} conn
   * @param {String} event
   * @param {String} tag
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _finishData(source, conn, event, tag, stream) {
    if (stream.done) return this;
    stream.done = true;
    return this._sendData(source, conn, event, tag, stream, null);
  }

  /**
   *
   * @method _sendError
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {Clusterluck.Connection} conn
   * @param {String} event
   * @param {String} tag
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   * @param {Stream} data
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _sendError(source, conn, event, tag, stream, data) {
    if (stream.done) return this;
    stream.done = true;
    stream.error = NetKernel._encodeError(data);
    data = null;
    return this._sendData(source, conn, event, tag, stream, data);
  }

  /**
   *
   * @method _addSocket
   * @memberof Clusterluck.NetKernel
   * @private
   * @instance
   *
   * @param {Socket} socket - Socket to add into internal state.
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _addSocket(socket) {
    if (this._srcs.has(socket.id)) return this;
    this._srcs.set(socket.id, socket);
    return this;
  }

  /**
   *
   * @method _disconnectSocket
   * @memberof Clusterluck.NetKernel
   * @private
   * @instance
   *
   * @param {Socket} socket - Socket to disconnect from.
   * @param {String} id - ID of socket to remove.
   *
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _disconnectSocket(socket, id) {
    this._srcs.delete(id);
    return this;
  }

  /**
   *
   * @method _rstream
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {Clusterluck.Node} node
   * @param {String} id
   *
   * @return {Stream}
   *
   */
  _rstream(node, id, timeout=Infinity) {
    var pstream = new stream.PassThrough();
    this.on(id, _.partial(this._rcvData, node, id, pstream).bind(this));
    if (timeout === Infinity || typeof timeout !== "number") return pstream;
    var t = setTimeout(() => {
      this.removeAllListeners(id);
      pstream.emit("error", new Error("Timeout."));
    }, timeout);
    pstream.once("finish", _.partial(clearTimeout, t));
    return pstream;
  }

  /**
   *
   * @method _rcvData
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @param {Clusterluck.Node} node
   * @param {String} id
   * @param {Stream} pstream
   * @param {Buffer} data
   * @param {Object} stream
   * @param {String} stream.stream
   * @param {Boolean} stream.done
   * @param {Object} from
   * @param {Node} from.node
   * @param {String} from.tag
   *
   */
  _rcvData(node, id, pstream, data, stream, from) {
    if (from.node.id() !== node.id()) {
      return this._skipMsg(_.extend(new Error("Synchronous response node mismatch."), {
        type: "INVALID_REPLY",
        actual: from.node.id(),
        expected: node.id()
      }));
    }
    if (stream.error) {
      pstream.emit("error", stream.error);
    }
    if (stream.done) {
      this.removeAllListeners(id);
      pstream.end();
    }
    else pstream.write(data);
  }

  /**
   *
   * @method _skipMsg
   * @memberof Clusterluck.NetKernel
   * @instance
   * @private
   *
   * @fires Clusterluck.NetKernel#NetKernel:_skip
   *
   * @param {Object} data
   * 
   * @return {Clusterluck.NetKernel} This instance.
   *
   */
  _skipMsg(data) {
    debug("Skipping message", data);

    /**
     *
     * Emitted whenever this instance has received a message that a) contained invalid JSON or b) had a mismatched HMAC checksum in the body.
     *
     * @event Clusterluck.NetKernel#NetKernel:_skip
     * @memberof Clusterluck.NetKernel
     * @property {Object} data - Contents of skipped message.
     *
     */
    this.emit("_skip", data);
    return this;
  }

  /**
   *
   * @method _coerceStream
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {Stream|Buffer|String} data
   * 
   * @return {Stream}
   *
   */
  static _coerceStream(data) {
    if (data instanceof stream.Stream) {
      return data;
    }
    else if (data instanceof Buffer || typeof data === "string") {
      var nstream = new stream.PassThrough();
      async.nextTick(() => {
        nstream.write(typeof data === "string" ? Buffer.from(data) : data);
        nstream.end();
      });
      return nstream;
    }
    else {
      var err = new Error("Input data could not be coerced into a stream.");
      throw err;
    }
  }

  /**
   *
   * @method _collectStream
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {Stream} rstream
   * @param {Function} cb
   *
   */
  static _collectStream(rstream, cb, limit = 10000000) {
    var acc = Buffer.from("");
    var called = false;
    var dataHandler = (data) => {
      acc = Buffer.concat([acc, data], acc.length + data.length);
      if (acc.length > limit) rstream.emit("error", new Error("Buffer limit exceeded."));
    };
    rstream.on("data", dataHandler);
    rstream.once("end", () => {
      if (called) return;
      called = true;
      return cb(null, acc);
    });
    rstream.on("error", (err) => {
      if (called) return;
      rstream.removeListener("data", dataHandler);
      if (rstream.resume) rstream.resume();
      called = true;
      return cb(err);
    });
  }
  
  /**
   *
   * @method _encodeBuffer
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {Buffer} buf
   *
   * @return {Object}
   *
   */
  static _encodeBuffer(buf) {
    if (buf === null) return null;
    return buf.toJSON();
  }

  /**
   *
   * @method _decodeBuffer
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {Object} data
   *
   * @return {Buffer}
   *
   */
  static _decodeBuffer(data) {
    if (data === null) return null;
    // buffer.toJSON returns {type: "Buffer", data: [array of bytes...]}
    return Buffer.from(data.data);
  }

  /**
   *
   * @method _encodeError
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {Error} error
   *
   * @return {Object}
   *
   */
  static _encodeError(error) {
    if (error === null) return null;
    return utils.errorToObject(error);
  }

  /**
   *
   * @method _encodeMsg
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {String} key
   * @param {Object} data
   *
   * @return {Object}
   *
   */
  static _encodeMsg(key, data) {
    if (key === null) return data;
    var checkSum = NetKernel._hmacData(key, JSON.stringify(data));
    return _.defaults({checkSum: checkSum}, data);
  }

  /**
   *
   * @method _decodeMsg
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {String} key
   * @param {Object} data
   *
   * @return {Object}
   *
   */
  static _decodeMsg(key, data) {
    if (key === null) return data;
    var checkSum = data.checkSum;
    data = _.omit(data, ["checkSum"]);
    var calculated = NetKernel._hmacData(key, JSON.stringify(data));
    if (checkSum !== calculated) {
      return _.extend(new Error("Checksum failure."), {
        type: "INVALID_CHECKSUM",
        sent: checkSum,
        calculated: calculated
      });
    }
    return data;
  }

  /**
   *
   * @method _hmacData
   * @memberof Clusterluck.NetKernel
   * @private
   * @static
   *
   * @param {String} key
   * @param {String} data
   *
   * @return {String}
   *
   */
  static _hmacData(key, data) {
    return crypto.createHmac("sha256", key).update(data).digest("hex");
  }
}

module.exports = NetKernel;

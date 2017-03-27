var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    debug = require("debug")("clusterluck:lib:conn"),
    EventEmitter = require("events").EventEmitter,
    Queue = require("./queue");

class Connection extends EventEmitter {
  /**
   *
   * Connection abstraction class. Handles reconnection logic when the client IPC socket disconnects, internal message buffering during reconnection, and state management for safe connection closure.
   *
   * @class Connection
   * @memberof Clusterluck
   *
   * @param {IPC} ipc - IPC module to create connection over.
   * @param {Clusterluck.Node} node - Node this connection communicates with.
   *
   */
  constructor(ipc, node) {
    super();
    this._ipc = ipc;
    this._node = node;
    this._queue = new Queue();
    this._connecting = false;
    this._active = false;
    this._streams = new Map();
  }

  /**
   *
   * Initializes IPC client socket to `node`, along with listeners for socket disconnects.
   *
   * @method start
   * @memberof Clusterluck.Connection
   * @instance
   *
   */
  start() {
    // maybe add routine for removing old messages still in queue to avoid backup
    // on catastrophic neighbor failures
    var node = this._node;
    this._active = true;
    this._connecting = true;
    this._ipc.connectToNet(node.id(), node.host(), node.port());
    this._ipc.of[node.id()].on("connect", this._handleConnect.bind(this));
    this._ipc.of[node.id()].on("disconnect", this._handleDisconnect.bind(this));
  }

  /**
   *
   * Closes IPC client socket to `node`. Can be done synchronously using the force option, or asynchronously by waiting for an idle/connected state to occur.
   *
   * @method stop
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @param {Boolean} [force] - Whether to forcibly close this connection or not. If true, will bypass waiting for an 'idle' state, immediately flushing the internal message buffer and clobeering state about which streams are still active over this connection. Otherwise, this will asynchronously close, waiting for all messages and streams to finish first.
   *
   * @return {Clusterluck.Connection} This instance.
   *
   */
  stop(force = false) {
    debug("Stopping connection to node " + this._node.id() + (force ? " forcefully" : " gracefully"));
    if (!this.idle() && force !== true) {
      this.once("idle", this.stop.bind(this));
      return this;
    }
    if (this._connecting === true && force !== true) {
      this.once("connect", this.stop.bind(this));
      return this;
    }
    this._connecting = false;
    this._active = false;
    this._queue.flush();
    this._streams = new Map();
    this._ipc.disconnect(this._node.id());
    return this;
  }

  /**
   *
   * Acts as a getter for the node this connection communicates with.
   *
   * @method node
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @return {Clusterluck.Node} Node this instance communicates with.
   *
   */
  node() {
    return this._node;
  }

  /**
   *
   * Acts as a getter for the internal message buffer.
   *
   * @method queue
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @return {Queue} Internal message buffer of this instance.
   *
   */
  queue() {
    return this._queue;
  }
  
  /**
   *
   * Returns whether this connection has been stopped or not.
   *
   * @method active
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @return {Boolean} Whether this connection is active or not.
   *
   */
  active() {
    return this._active;
  }

  /**
   *
   * Returns whether this connection is in a reconnection state or not.
   *
   * @method connecting
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @return {Boolean} Whether this connection is in the middle of reconnection logic.
   *
   */
  connecting() {
    return this._connecting;
  }
  
  /**
   *
   * Returns whether this connection is in an idle state.
   *
   * @method idle
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @return {Boolean} Whether this connection is currently idle.
   *
   */
  idle() {
    return this._streams.size === 0 && this._queue.size() === 0;
  }

  /**
   *
   * Sends message `data` under event `event` through this IPC socket.
   *
   * @method send
   * @memberof Clusterluck.Connection
   * @instance
   *
   * @param {String} event - Event to identify IPC message with.
   * @param {Object} data - Data to send with this IPC message.
   *
   * @return {Clusterluck.Connection} This instance.
   *
   */
  send(event, data) {
    if (this._active === false) {
      return new Error("Cannot write to inactive connection.");
    }
    if (this._connecting === true) {
      this._queue.enqueue({
        event: event,
        data: data
      });
      return this;
    }
    this._ipc.of[this._node.id()].emit(event, data);
    this.emit("send", event, data);
    this._updateStream(data.stream);
    return this;
  }

  initiateStream(stream) {
    this._streams.set(stream.stream, true);
    return this;
  }

  /**
   *
   * Handler for when this connection has finished reconnection logic.
   *
   * @method _handleConnect
   * @memberof Clusterluck.Connection
   * @private
   * @instance
   *
   */
  _handleConnect() {
    debug("Connected to TCP connection to node " + this._node.id());
    this._connecting = false;
    this.emit("connect");
    // flush queue after emitting "connect"
    var out = this._queue.flush();
    out.forEach((msg) => {
      this.send(msg.event, msg.data);
    });
  }

  /**
   *
   * Handler for when this connection has entered reconnection logic.
   *
   * @method _handleDisconnect
   * @memberof Clusterluck.Connection
   * @private
   * @instance
   *
   */
  _handleDisconnect() {
    debug("Disconnected from TCP connection to node " + this._node.id());
    if (this._active) {
      this._connecting = true;
    }
    else {
      this._connecting = false;
    }
    this.emit("disconnect");
  }

  /**
   *
   * Updates the stream state of this instance. If the stream is finished, removes the stream ID. If no stream IDs are left, then an idle event is emitted.
   *
   * @method _updateStream
   * @memberof Clusterluck.Connection
   * @private
   * @instance
   *
   * @param {Object} stream - Stream to update internal state about.
   *
   */
  _updateStream(stream) {
    if (stream.done) {
      this._streams.delete(stream.stream);
      if (this._streams.size === 0) {
        this.emit("idle");
      }
    }
    else {
      this._streams.set(stream.stream, true);
    }
  }
}

module.exports = Connection;

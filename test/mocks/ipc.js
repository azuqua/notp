var _ = require("lodash"),
    uuid = require("uuid"),
    async = require("async"),
    EventEmitter = require("events").EventEmitter,
    sinon = require("sinon");

var network = new Map();

class Socket extends EventEmitter {
  constructor(id) {
    super();
    this.id = id;
  }
}

class MockIPC {
  constructor(opts) {
    this.config = opts || {};
    this._id = null;
    this._host = null;
    this._port = null;
    this.of = {};
    this.server = null;
  }

  serveNet(host, port, cb) {
    this._id = this.config.id;
    this._host = host;
    this._port = port;
    if (network.has(this._id)) {
      return async.nextTick(cb);
    }
    this.server = new Server(this._id, this._host, this._port);
    return async.nextTick(cb);
  }

  connectToNet(id, host, port, cb) {
    var server = network.get(id);
    var conn = new Conn(server);
    this.of[id] = conn;
    server.on(conn.id(), (event, data) => {
      var socket = server.getSocket(conn._id);
      server.emit(event, data, socket);
    });
    async.nextTick(() => {
      conn.emit("connect");
      if (typeof cb === "function") return cb();
    });
  }

  disconnect(id) {
    var conn = this.of[id];
    var server = network.get(id);
    server.removeSocket(conn.id());
    delete this.of[id];
    async.nextTick(() => {
      conn.emit("disconnect");
      conn.removeAllListeners();
    });
  }

  network() {
    return network;
  }
}

class Server extends EventEmitter {
  constructor(id, host, port) {
    super();
    this._id = id;
    this._host = host;
    this._port = port;
    this._sockets = new Map();
  }

  start() {
    network.set(this._id, this);
    return this;
  }

  stop() {
    this._sockets = new Map();
    this.removeAllListeners();
    network.delete(this._id);
    return this;
  }

  addSocket(id) {
    this._sockets.set(id, new Socket(id));
    return this;
  }

  getSocket(id) {
    return this._sockets.get(id);
  }

  removeSocket(id) {
    var socket = this._sockets.get(id);
    this._sockets.delete(id);
    this.emit("socket.disconnected", socket, id);
    return this;
  }
}

class Conn extends EventEmitter {
  constructor(server) {
    super();
    this._id = uuid.v4();
    this._server = server;
    this._server.addSocket(this._id);
    this.socket = new Socket();
  }

  id() {
    return this._id;
  }

  emit(event) {
    var args = _.drop(Array.from(arguments));
    this._server.emit.apply(this._server, [this._id, event].concat(args));
    return super.emit.apply(this, [event].concat(args));
  }
}

module.exports = MockIPC;

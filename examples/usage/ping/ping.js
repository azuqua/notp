const cl = require("../../../index"),
      _ = require("lodash");

class PingServer extends cl.GenServer {
  constructor(kernel) {
    super(kernel);
  }

  start(name) {
    super.start(name);

    const handler = this._doPing.bind(this);
    this.on("ping", handler);
    this.once("stop", _.partial(this.removeListener, "ping", handler).bind(this));

    return this;
  }

  ping(node, cb) {
    this.call({node: node, id: this._id}, "ping", null, cb);
  }

  _doPing(data, from) {
    return this.reply(from, "pong");
  }
}

module.exports = PingServer;

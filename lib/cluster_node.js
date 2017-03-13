var _ = require("lodash"),
    async = require("async"),
    EventEmitter = require("events").EventEmitter,
    utils = require("./utils");

class ClusterNode extends EventEmitter {
  constructor(kernel, gossip) {
    super();
    this._kernel = kernel;
    this._gossip = gossip;
  }

  kernel(kernel) {
    if (kernel !== undefined) {
      this._kernel = kernel;
    }
    return this._kernel;
  }

  gossip(gossip) {
    if (gossip !== undefined) {
      this._gossip = gossip;
    }
    return this._gossip;
  }

  load(cb) {
    this._gossip.load((err) => {
      if (err) return cb(err);
      this._gossip.ring().nodes().forEach((node) => {
        if (node.id() === this._kernel.self().id()) return;
        this._kernel.connect(node);
      });
      return cb();
    });
  }

  start(cookie, ringID, cb) {
    // if ring was successfully read from disk and ring ID different than input, error out
    if (typeof this._gossip._ringID === "string" &&
        this._gossip._ringID !== ringID) {
      return cb(new Error("Loaded ring ID '" + this._gossip._ringID + "' does not match '" + ringID + "'"));
    }
    this._gossip.start(ringID);
    this._kernel.start({cookie: cookie});
    this._kernel.once("_ready", () => {this.emit("ready");});
    if (typeof cb === "function") {
      this.on("ready", cb);
    }
    return this;
  }

  stop(force) {
    this._gossip.once("stop", () => {
      this._kernel.stop();
      this.emit("stop");
    });
    this._gossip.stop(force);
  }
}

module.exports = ClusterNode;

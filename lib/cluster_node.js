var _ = require("lodash"),
    async = require("async"),
    EventEmitter = require("events").EventEmitter,
    utils = require("./utils");

/** @namespace Clusterluck */

class ClusterNode extends EventEmitter {
  /**
   *
   * Cluster wrapper class. Used to start/load/stop the cluster, both network kernel and gossip ring.
   *
   * @class ClusterNode ClusterNode
   * @memberof Clusterluck
   *
   * @param {Clusterluck.NetKernel} kernel - Network kernel to use to communicate with other nodes.
   * @param {Clusterluck.GossipRing} gossip - Gossip ring to use to route messages to across a cluster.
   *
   */
  constructor(kernel, gossip, comms) {
    super();
    this._kernel = kernel;
    this._gossip = gossip;
    this._comms = comms;
  }

  /**
   *
   * Acts as a getter/setter for the netkernel of this instance.
   *
   * @method kernel
   * @memberof ClusterLuck.ClusterNode
   * @instance
   *
   * @param {Clusterluck.NetKernel} [kernel] - Network kernel to set on this instance.
   *
   * @return {Clusterluck.NetKernel} Network kernel of this instance.
   *
   */
  kernel(kernel) {
    if (kernel !== undefined) {
      this._kernel = kernel;
    }
    return this._kernel;
  }

  /**
   *
   * Acts as a getter/setter for the gossip ring of this instance.
   *
   * @method gossip
   * @memberof Clusterluck.ClusterNode
   * @instance
   *
   * @param {Clusterluck.GossipRing} [gossip] - Gossip ring to set on this instance.
   *
   * @return {Clusterluck.GossipRing} Gossip ring of this instance.
   *
   */
  gossip(gossip) {
    if (gossip !== undefined) {
      this._gossip = gossip;
    }
    return this._gossip;
  }

  /**
   *
   * Acts as a getter/setter for the command handler of this instance.
   *
   * @method commandServer
   * @memberof Clusterluck.ClusterNode
   * @instance
   *
   * @param {Clusterluck.CommandServer} [commandServer] - Command handler to set on this instance.
   *
   * @return {Clusterluck.CommandServer} Command handler of this instance.
   *
   */
  commandServer(comm) {
    if (comm !== undefined) {
      this._comms = comm;
    }
    return this._comms;
  }

  /**
   *
   * Loads gossip state from disk and establishes all node connections derived from
   * the newly loaded hash ring.
   *
   * @method load
   * @memberof Clusterluck.ClusterNode
   * @instance
   *
   * @param {Function} cb - Function to call once state has been loaded.
   *
   */
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

  /**
   *
   * Starts a network kernel and gossip ring on this node.
   *
   * @method start
   * @memberof Clusterluck.ClusterNode
   * @instance
   *
   * @param {String} cookie - Distributed cookie to use when communicating with other nodes and signing payloads.
   * @param {String} ringID - Ring ID to start gossip ring on.
   * @param {Function} [cb] - Optional callback; called when network kernel has been fully started and listening for IPC messages.
   *
   * @return {Clusterluck.ClusterNode} This instance.
   *
   */
  start(cookie, ringID, cb) {
    // if ring was successfully read from disk and ring ID different than input, error out
    if (typeof this._gossip._ringID === "string" &&
        this._gossip._ringID !== ringID) {
      return cb(new Error("Loaded ring ID '" + this._gossip._ringID + "' does not match '" + ringID + "'"));
    }
    this._comms.start("command");
    // clear ring ID so that we can start the gossip server (we know they're the same, so it's a noop)
    this._gossip.ringID(null);
    this._gossip.start(ringID);
    this._kernel.start({cookie: cookie});

    /**
     *
     * Emitted when the command line server, gossip ring server, and network kernel have all started and are ready to start processing messages.
     *
     * @event Clusterluck.ClusterNode#ClusterNode:ready
     * @memberof Clusterluck.ClusterNode
     *
     */
    this._kernel.once("_ready", () => {this.emit("ready");});
    if (typeof cb === "function") {
      this.on("ready", cb);
    }
    return this;
  }

  /**
   *
   * Stops the gossip ring and network kernel, as well as closing all network connections with any external nodes.
   *
   * @method stop
   * @memberof Clusterluck.ClusterNode
   * @instance
   *
   * @fires Clusterluck.ClusterNode#ClusterNode:stop
   * @listens Clusterluck.CommandServer#CommandServer:stop
   * @listens Clusterluck.GossipRing#GossipRing:stop
   *
   * @param {Boolean} [force] - Whether to forcibly stop this node or not.
   *
   * @return {Clusterluck.ClusterNode} This instance.
   *
   */
  stop(force) {
    async.series([
      (next) => {
        this._comms.once("stop", next);
        this._comms.stop(force);
      },
      (next) => {
        this._gossip.once("stop", next);
        this._gossip.stop(force);
      },
      (next) => {
        this._kernel.stop();
        this._kernel.sinks().forEach((val) => {
          this._kernel.disconnect(val.node(), true);
        });

        /**
         *
         * Emitted when the command line server, gossip ring server, and network kernel have all stopped processing messages.
         *
         * @event Clusterluck.ClusterNode#ClusterNode:stop
         * @memberof Clusterluck.ClusterNode
         *
         */
        this.emit("stop");
      }
    ]);
  }
}

module.exports = ClusterNode;

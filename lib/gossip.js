var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    EventEmitter = require("events").EventEmitter,
    microtime = require("microtime"),
    fs = require("fs"),
    util = require("util"),
    debug = require("debug")("clusterluck:lib:gossip");

var GenServer = require("./gen_server"),
    CHash = require("./chash"),
    VectorClock = require("./vclock"),
    utils = require("./utils");

class GossipRing extends GenServer {
  /**
   *
   * Gossip ring implementation. Maintains a consistent hash ring to manage cluster membership and external connections in the netkernel. Maintains a vector clock to compare the most up-to-date information about the cluster. Listens for messages from the netkernel about cluster joins/departures/updates, with an internal map to register message streams. On an interval, sends messages to the cluster about current state and trims it's internal vector clock. To handle vector clock conflicts, which are possible in this architecture, a LWW approach is adopted. This is largely because insertions/deletions (the deciding factor for LWW) are centralized from a task-management standpoint (the system administrator will be the one administering these operations), therefore time is a practically controllable dimension. In addition, for future work, metrics will be emitted on a routine basis from the same source (a node sharing work-load information across the cluster), meaning any conflict that occurs with this data can easily be resolved.
   *
   * @class GossipRing GossipRing
   * @memberof Clusterluck
   *
   * @param {Clusterluck.NetKernel} kernel - Network kernel to communicate with other nodes.
   * @param {Clusterluck.CHash} chash - Consistent hash ring to represent cluster membership.
   * @param {Clusterluck.VectorClock} vclock - Vector clock to represent current state of ring relative to other nodes in the cluster.
   * @param {Object} opts - Options object for state management.
   * @param {Number} opts.interval - Internval in ms to send ring to members of cluster.
   * @param {Number} opts.wquorum - Write quorum for distributed writes. Prototypical member of this class.
   * @param {Object} opts.vclockOpts - Options for periodic trimming of internal vector clock.
   *
   */
  constructor(kernel, chash, vclock, opts) {
    super(kernel);
    this._ring = chash;
    this._vclock = vclock;
    this._poll = opts.interval;
    this._flushPoll = opts.flushInterval;
    this._flushPath = opts.flushPath;
    this._vclockOpts = opts.vclockOpts;
    this._interval = null;
    this._flush = null;
    this._actor = null;
    this._ringID = null;
  }

  /**
   *
   * Starts the gossip handler: listens for events related to `ringID` on the netkernel, and sets the internal interval to send messages about this instance's state. When this process leaves the current ring ID, will remove the previously created kernel event listener.
   *
   * @method start
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {String} ringID - Ring ID to listen for events on.
   *
   * @listens Clusterluck.GenServer#GenServer:stop
   * @listens Clusterluck.GossipRing#GossipRing:ring
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  start(ringID) {
    this._id = ringID;
    this.join(ringID);
    this._interval = setInterval(this.poll.bind(this), this._poll);
    this._flush = setInterval(this.flush.bind(this), this._flushPoll);

    /**
     *
     * Event emitted when any ring updates occur and are gossipped around the cluster. Since the GossipRing class extends the GenServer class, this event is emitted whenever a message stream is parsed with event "ring".
     *
     * @event Clusterluck.GossipRing#GossipRing:ring
     * @memberof Clusterluck.GossipRing
     * @type {Object}
     * @property {Clusterluck.CHash} data - Consistent hash ring sent with this ring update message.
     * @property {Clusterluck.VectorClock} vclock - Vector clock associated with this message. Based on comparison between this value and this instance's own vector clock, the message may be ignored.
     * @property {String} actor - Actor associated with the most recent update to `vclock`.
     * @property {String} type - Update type. Can be either "join" (meet request) or "update" (anything else).
     *
     */
    var events = [
      {event: "ring", method: "_updateRing"},
    ];
    events.forEach((event) => {
      var handle = this[event.method].bind(this);
      this.on(event.event, handle);
      this.once("stop", _.partial(this.removeListener, event.event, handle).bind(this));
    });
    return this;
  }

  /**
   *
   * Stops this handler. Provides the option to forcibly stop, meaning all internal message streams will be cleared and this node will immediately leave it's current cluster.
   *
   * @method stop
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:stop
   * @listens Clusterluck.GossipRing#GossipRing:close
   *
   * @param {Boolean} [force] - Whether to forcibly stop this process. If false, will wait for an idle state before leaving the ring and clearing the internal message stream map. Otherwise, will immediately clear any pending updates and leave the ring. Defaults to false.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  stop(force = false) {
    debug("Stopping gossip event handler " + (force ? "forcefully" : "gracefully"));
    this.once("close", _.partial(this.emit, "stop").bind(this));
    this.leave(force);
  }

  /**
   *
   * Pauses the GossipRing's message processing. As a reuslt, any messages routed from the network kernel to this instance will be missed until `this.resume()` is called.
   *
   * @method pause
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:pause
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  pause() {
    debug("Pausing gossip event handler");
    clearInterval(this._interval);
    clearInterval(this._flush);
    this._interval = null;
    this._flush = null;
    super.pause();
    return this;
  }

  /**
   *
   * Resumes the processing of message streams on this instance. Any messages missed in between pausing and now will result in failed message parsing (since all JSON contents are sent in one message).
   *
   * @method resume
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GenServer#GenServer:resume
   * @listens Clusterluck.GenServer#GenServer:pause
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  resume() {
    this._interval = setInterval(this.poll.bind(this), this._poll);
    this._flush = setInterval(this.flush.bind(this), this._flushPoll);
    super.resume();
    return this;
  }

  /**
   *
   * Parses a fully memoized message stream into an object containing a key/value pair. If we fail to parse the job buffer (invalid JSON, etc), we just return an error and this GenServer will skip emitting an event. Otherwise, triggers user-defined logic for the parsed event.
   *
   * @method decodeJob
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   * @param {Buffer} job - Memoized buffer that represents complete message stream.
   *
   * @return {Object} Object containing an event and data key/value pair, which are used to emit an event for user-defined logic.
   *
   */
  decodeJob(buf) {
    var out = super.decodeJob(buf);
    if (out instanceof Error) return out;
    var val = out.data;
    out.data = {
      type: val.type,
      data: GossipRing._decodeRing(val.data),
      vclock: (new VectorClock()).fromJSON(val.vclock),
      round: val.round,
      actor: val.actor
    };
    return out;
  }

  /**
   *
   * Loads existing gossip ring from disk. If no path is specified or the file does not exist, we simple do nothing. Otherwise, the result JSON is parsed read into this instance's hash ring, vector clock, ring ID, and current actor.
   *
   * @method load
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Function} cb - Called when loading state has finished. On error, will operate as `cb(err)`. Otherwise, will operate as `cb()`.
   *
   */
  load(cb) {
    if (typeof this._flushPath !== "string") {
      return async.nextTick(cb);
    }
    fs.readFile(this._flushPath, (err, data) => {
      if (err && err.code !== "ENOENT") return cb(err);
      if (err && err.code === "ENOENT") return cb();
      data = utils.safeParse(data);
      if (data instanceof Error || !util.isObject(data)) {
        return cb(new Error("File at path '" + this._flushPath + "' contains invalid JSON blob."));
      }
      this._ringID = data.ring;
      this._ring = this._ring.fromJSON(data.chash);
      this._vclock = this._vclock.fromJSON(data.vclock);
      this._actor = data.actor;
      return cb();
    });
  }

  /**
   *
   * Acts as a getter/setter for the current actor of this instance.
   *
   * @method actor
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {String} [actor] - Actor to set on this instance.
   *
   * @return {String} Actor of this instance.
   *
   */
  actor(actor) {
    if (actor) {
      this._actor = actor;
    }
    return this._actor;
  }

  /**
   *
   * Acts as a getter/setter for the hash ring of this instance.
   *
   * @method ring
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Clusterluck.CHash} [ring] - Consistent hash ring to set on this instance.
   *
   * @return {Clusterluck.CHash} Consistent hash ring of this instance.
   *
   */
  ring(ring) {
    if (ring) {
      this._ring = ring;
    }
    return this._ring;
  }

  /**
   *
   * Acts as a getter/setter for the vector clock of this instance.
   *
   * @method vclock
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Clusterluck.VectorClock} [vclock] - Vector clock to set on this instance.
   *
   * @return {Clusterluck.VectorClock} Vector clock of this instance.
   *
   */
  vclock(vclock) {
    if (vclock) {
      this._vclock = vclock;
    }
    return this._vclock;
  }

  /**
   *
   * Acts as a getter/setter for the ring ID of this instance.
   *
   * @method ringID
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {String} [ring] - Ring ID to set on this instance.
   *
   * @return {Clusterluck.VectorClock} Ring ID of this instance.
   *
   */
  ringID(ring) {
    if (ring !== undefined) {
      this._ringID = ring;
    }
    return this._ringID;
  }

  /**
   *
   * Joins the ring `ringID` if not already a member of a ring. Otherwise, will return an error noting that this instance already belongs to a ring.
   *
   * @method join
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @listens Clusterluck.NetKernel#NetKernel:user_defined
   * @listens Clusterluck.GenServer#GenServer:pause
   *
   * @param {String} ringID - Ring ID for this node to join.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  join(ringID) {
    debug("Joining ring " + ringID);
    if (typeof this._ringID === "string") {
      return new Error("Node already belongs to ring '" + this._ringID + "'");
    }
    this._ringID = ringID;
    var handler = this._parse.bind(this);
    this._kernel.on(this._ringID, handler);
    this.once("pause", _.partial(this._kernel.removeListener, this._ringID, handler).bind(this._kernel));
    return this;
  }

  /**
   *
   * Joins the cluster present at `node`, using it as a seed node for inserting this instance's node identifier in to the cluster. This process occurs asynchronously, thus this node's cluster state won't be updated until future rounds of gossip.
   *
   * @method meet
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:send
   *
   * @param {Clusterluck.Node} node - Node to seed ring-join with.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  meet(node) {
    if (this._ring.isDefined(node)) return this;
    debug("Meeting node in cluster:", node.id());
    this._kernel.connect(node);
    var msg = this._ring.toJSON(true);
    // we don't reflect new message in this vector clock; if we did, it'd possibly negate
    // any future rounds of gossip from the node we're meeting (since joins don't introduce
    // a new actor receive-side, only on the sender's side)
    this.cast({id: this._ringID, node: node}, "ring", {
      type: "join",
      actor: uuid.v4(),
      data: msg,
      vclock: this._vclock.toJSON(true),
      round: 0
    });

    /**
     *
     * Emitted whenever this instance sends a message to another node in the cluster.
     *
     * @event Clusterluck.GossipRing#GossipRing:send
     * @memberof Clusterluck.GossipRing
     * @property {Clusterluck.VectorClock} vclock - Vector clock of message sent.
     * @property {String} event - Event name of message sent.
     * @property {Object} msg - Message contents.
     *
     */
    this.emit("send", this._vclock, "ring", msg);
    return this;
  }
  
  /**
   *
   * Inserts `node` into this node's cluster, asynchronously propagating this new state through the cluster. Provides the option to forcibly insert `node`, meaning it will precede any existing internal message streams pending completion.
   *
   * @method insert
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:process
   * @listens Clusterluck.GenServer#GenServer:idle
   *
   * @param {Clusterluck.Node} node - Node to insert into this gossip ring's cluster.
   * @param {Boolean} [force] - Whether to forcibly add `node` into the current state of this ring, or wait for an idle state. Defaults to false.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  insert(node, force = false) {
    if (this._ring.isDefined(node)) return this;
    debug("Inserting node into cluster:", node.id());
    if (this.idle() || force === true) {
      var oldRing = (new CHash(this._ring.rfactor(), this._ring.pfactor(), this._ring.tree()));
      this._ring.insert(node);
      this._actor = uuid.v4();
      this._vclock.increment(this._actor);
      this._kernel.connect(node);
      this.sendRing(GossipRing.maxMsgRound(this._ring));

      /**
       *
       * Emitted whenever this instance changes it's consistent hash ring, either based on events generated by the CLI or
       * by gossipped messages.
       *
       * @event Clusterluck.GossipRing#GossipRing:process
       * @memberof Clusterluck.GossipRing
       * @property {Clusterluck.CHash} oldRing - Previous ring before this event is fired.
       * @property {Clusterluck.CHash} currRing - Current ring when this event is fired.
       *
       */
      this.emit("process", oldRing, this._ring);
      return this;
    }
    this.once("idle", _.partial(this.insert, node).bind(this));
    return this;
  }

  /**
   *
   * Inserts `nodes` into this node's cluster, asynchronously propagating this new state through the cluster. Provides the option to forcibly insert `nodes`, meaning it will precede any existing internal message streams pending completion.
   *
   * @method minsert
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:process
   * @listens Clusterluck.GenServer#GenServer:idle
   *
   * @param {Array} nodes - Nodes to insert into this gossip ring's cluster.
   * @param {Boolean} [force] - Whether to forcibly add `nodes` into the current state of this ring, or wait for an idle state. Defaults to false.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  minsert(nodes, force = false) {
    if (_.every(nodes, this._ring.isDefined.bind(this._ring))) return this;
    debug("Inserting multiple nodes into cluster:", _.map(nodes, "_id"));
    if (this.idle() || force === true) {
      var oldRing = (new CHash(this._ring.rfactor(), this._ring.pfactor(), this._ring.tree()));
      nodes.forEach(_.ary(this._ring.insert.bind(this._ring), 1));
      this._actor = uuid.v4();
      this._vclock.increment(this._actor);
      nodes.forEach(_.ary(this._kernel.connect.bind(this._kernel), 1));
      this.sendRing(GossipRing.maxMsgRound(this._ring));
      this.emit("process", oldRing, this._ring);
      return this;
    }
    this.once("idle", _.partial(this.minsert, nodes, force).bind(this));
    return this;
  }

  /**
   *
   * Leaves the current cluster, removing all nodes from it's hash ring and creating a new vector clock for this new ring. Triggers the removal of this ring's message handler on the network kernel. Provides the option to forcibly leave the cluster, meaning this function won't wait for an idle state to execute and execute immediately. Otherwise, will wait for message streams to complete.
   *
   * @method leave
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:leave
   * @fires Clusterluck.GossipRing#GossipRing:close
   * @listens Clusterluck.GenServer#GenServer:idle
   *
   * @param {Boolean} [force] - Whether to forcibly leave this ring or not. If false, will wait for an idle state before leaving the ring and clearing the internal message stream map. Otherwise, will immediately clear any pending updates and leave the ring. Defaults to false.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  leave(force = false) {
    debug("Leaving ring " + this._ringID);
    this.pause();
    if (this._ring.size() <= this._ring.rfactor()) {
      this.emit("leave", this._ring);
      this._streams.clear();
      this._ringID = null;
      this.emit("close");
      return this; 
    }
    if (this.idle() || force === true) {
      this._streams.clear();
      this._closeRing();
      return this;
    }
    this.once("idle", this._closeRing.bind(this));
    return this;
  }

  /**
   *
   * Removes `node` from this node's cluster, asynchronously propagating this new state through the cluster. Provides the option to forcibly remove `node`, meaning it will precede any existing internal message streams pending completion.
   *
   * @method remove
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:process
   * @listens Clusterluck.GenServer#GenServer:idle
   *
   * @param {Node} node - Node to remove from this gossip ring's cluster.
   * @param {Boolean} [force] - Whether to forcibly remove `node` from the current state of this ring, or wait for an idle state. Defaults to false.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  remove(node, force = false) {
    if (!this._ring.isDefined(node)) return this;
    debug("Removing node from cluster:", node.id());
    if (this.idle() || force === true) {
      var oldRing = (new CHash(this._ring.rfactor(), this._ring.pfactor(), this._ring.tree()));
      this._ring.remove(node);
      this._actor = uuid.v4();
      this._vclock.increment(this._actor);
      this._kernel.disconnect(node);
      this.sendRing(GossipRing.maxMsgRound(this._ring));
      this.emit("process", oldRing, this._ring);
      return this;
    }
    this.once("idle", _.partial(this.remove, node).bind(this));
    return this;
  }

  /**
   *
   * Removes `nodes` from this node's cluster, asynchronously propagating this new state through the cluster. Provides the option to forcibly remove `nodes`, meaning it will precede any existing internal message streams pending completion.
   *
   * @method mremove
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:process
   * @listens Clusterluck.GenServer#GenServer:idle
   *
   * @param {Array} nodes - Nodes to remove from this gossip ring's cluster.
   * @param {Boolean} [force] - Whether to forcibly remove `nodes` from the current state of this ring, or wait for an idle state. Defaults to false.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  mremove(nodes, force = false) {
    if (!_.some(nodes, this._ring.isDefined.bind(this._ring))) return this;
    debug("Removing multiple nodes from cluster:", _.map(nodes, "_id"));
    if (this.idle() || force === true) {
      var oldRing = (new CHash(this._ring.rfactor(), this._ring.pfactor(), this._ring.tree()));
      nodes.forEach(_.ary(this._ring.remove.bind(this._ring), 1));
      this._actor = uuid.v4();
      this._vclock.increment(this._actor);
      nodes.forEach(_.ary(this._kernel.disconnect.bind(this._kernel), 1));
      this.sendRing(GossipRing.maxMsgRound(this._ring));
      this.emit("process", oldRing, this._ring);
      return this;
    }
    this.once("idle", _.partial(this.mremove, nodes, force).bind(this));
    return this;
  }

  /**
   *
   * Finds the bucket of this instance's hash ring that `data` routes to.
   *
   * @method find
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Buffer} data - Data to find hash bucket of.
   *
   * @return {Array} Array of nodes responsible for the bucket `data` hashes to.
   *
   */
  find(data) {
    var node = this._ring.find(data);
    return [node].concat(this._ring.next(node));
  }

  /**
   *
   * Function that executes on this cluster's internal interval. Trims this instance's vector clock and sends the ring to random recipients in the cluster.
   *
   * @method poll
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  poll() {
    if (this._actor === null) return this;
    var time = microtime.now();
    this._vclock.trim(time, this._vclockOpts);
    this.sendRing(1);
    return this;
  }

  /**
   *
   * Function that executes on this cluster's internal flush interval. Flushes this node's ring state to disk.
   *
   * @method flush
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:flushing
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  flush() {
    /**
     *
     * Emitted when this instance begins to flush state to disk. NOT a reliable indicator for when state is actually flushed to disk, as this event is fired whenever `this.flush()` is called (as opposed to firing when it makes an fs call).
     *
     * @event Clusterluck.GossipRing#GossipRing:flushing
     * @memberof Clusterluck.GossipRing
     *
     */
    this.emit("flushing");
    if (this._actor === null || !this._flushPath) return this;
    debug("Flushing gossip ring to disk");
    fs.writeFile(this._flushPath, JSON.stringify({
      ring: this._ringID,
      actor: this._actor,
      chash: this._ring.toJSON(true),
      vclock: this._vclock.toJSON(true)
    }), (err) => {
      if (err) debug("Error writing ring to disk:", err);
    });
    return this;
  }

  /**
   *
   * Sends the state of this ring at round `n` as type 'update', selecting several nodes to propagate this gossip message to if the round number is non-zero and the internal ring is non-trivial.
   *
   * @method sendRing
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Number} [n] - Number of rounds to send this data in the cluster. Defauls to 1.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  sendRing(n = 1) {
    if (this._ring.size() <= this._ring.rfactor() || !this._ringID) return this;
    return this.send("ring", this._ring.toJSON(true), this._actor, this._vclock, n);
  }

  /**
   *
   * Selects `n` random nodes, different than this node, from it's hash ring.
   *
   * @method selectRandom
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Number} [n] - Number of nodes to randomly select from this ring. Defaults to 1.
   *
   * @return {Array} Array of nodes selected from this instance's hash ring.
   *
   */
  selectRandom(n = 1) {
    return _.sampleSize(this._ring.nodes().filter((node) => {
      return node.id() !== this._kernel.self().id();
    }), n);
  }

  /**
   *
   * Selects `n` random nodes from a list of nodes.
   *
   * @method selectRandomFrom
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {Array} nodes - Array of nodes to select random nodes from.
   * @param {Number} [n] - Number of nodes to randomly select from this ring. Defaults to 1.
   *
   * @return {Array} Array of nodes selected.
   *
   */
  selectRandomFrom(nodes, n = 1) {
    return _.sampleSize(nodes, n);
  }

  /**
   *
   * Sends `event` of this ring at round `n` with message `msg`, selecting several nodes to propagate this gossip message to if the round number is non-zero.
   *
   * @method send
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @param {String} event - Event to send.
   * @param {Object} msg - Value to send with this message.
   * @param {String} actor - Actor representing the snapshot of this message.
   * @param {Clusterluck.VectorClock} clock - Vector clock to send this message with.
   * @param {Number} [n] - Gossip round of this message. Defaults to 1.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  send(event, msg, actor, clock, n = 1) {
    return this.route(this.selectRandom(2), event, msg, actor, clock, n);
  }

  /**
   *
   * Sends `event` of this ring at round `n` with message `msg`, sending it to `nodes` if the round number is greater than zero.
   *
   * @method route
   * @memberof Clusterluck.GossipRing
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:send
   *
   * @param {Array} nodes - Nodes to send this message to.
   * @param {String} event - Event to send.
   * @param {Object} msg - Value to send with this message.
   * @param {String} actor - Actor representing the snapshot of this message.
   * @param {Clusterluck.VectorClock} clock - Vector clock to send this message with.
   * @param {Number} [n] - Gossip round of this message. Defaults to 1.
   *
   * @return {Clusterluck.GossipRing} This instance.
   *
   */
  route(nodes, event, msg, actor, clock, n = 1) {
    if (n === 0) return this;
    debug("Routing message on gossip process with actor:", actor + ",", "event:", event + ",", "round:", n);
    this.abcast(nodes, this._ringID, event, {
      type: "update",
      actor: actor,
      data: msg,
      vclock: clock.toJSON(true),
      round: --n
    });
    this.emit("send", clock, event, msg);
    return this;
  }

  /**
   *
   * @method _updateRing
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:process
   *
   */
  _updateRing(data) {
    var oldRing = (new CHash(this._ring.rfactor(), this._ring.pfactor(), this._ring.tree()));
    var nodes = this._mergeRings(data);
    var nRound = this._updateRound(data);
    this._makeConnects(nodes[0]);
    // make this true so we don't wait for a ring update that involves node departures on every node
    this._makeDisconnects(nodes[1], true);
    // to avoid expensive connection filtering
    if (nodes[0].length > 0 || nodes[1].length > 0) {
      this.emit("process", oldRing, this._ring);
    }
    
    return this.sendRing(nRound);
  }

  /**
   *
   * @method _mergeRings
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _mergeRings(data) {
    var nodes = [[], []];
    // if type is "join", just merge the two rings and increment actor, then return
    if (data.type === "join") {
      nodes = this._joinNewRing(data);
    }
    // check if data.vclock is descendant of this._vclock -> set ring to data.vclock, increment actor
    else if (data.vclock.descends(this._vclock)) {
      nodes = this._imposeRing(data);
    }
    // check if conflict -> if value same, merge clocks and send off; else add to conflict log
    else if (!this._vclock.descends(data.vclock)) {
      nodes = this._handleRingConflict(data);
    }
    this._vclock.increment(data.actor);
    this._actor = data.actor;
    return nodes;
  }

  /**
   *
   * @method _joinNewRing
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _joinNewRing(data) {
    this._vclock.merge(data.vclock);
    this._ring.merge(data.data);
    return [data.data.nodes(), []];
  }

  /**
   *
   * @method _imposeRing
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _imposeRing(data) {
    var nodes = GossipRing._ringDiff(data.data, this._ring);
    this._vclock = data.vclock;
    this._ring = data.data;
    return nodes;
  }

  /**
   *
   * @method _handleRingConflict
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:conflict
   *
   */
  _handleRingConflict(data) {
    /**
     *
     * Emitted when the ring state of another node, based off a received message off the network kernel, is in conflict with the current state of this instance. More for documenting purposes in this class, since all ring conflicts are handled with a "Last Write Wins" approach. However, in any new classes that may use similar logic to this, will be useful for custom state conflict.
     *
     * @event Clusterluck.GossipRing#GossipRing:conflict
     * @memberof Clusterluck.GossipRing
     * @property {Clusterluck.CHash} RecRing - Received ring in conflict with the current ring.
     * @property {Clusterluck.VectorClock} RecClock - Received vector clock conflicting with the current vector clock.
     *
     */
    this.emit("conflict", data.data, data.vclock);
    var oldRing = this._ring;
    // use LWW to handle conflict automatically, but this can be abstracted
    // for different conflict handlers
    debug("Ring conflict on ring " + this._ringID + " being resolved by LWW");
    this._ring = GossipRing.LWW(this._ring, this._vclock, data.data, data.vclock);
    this._vclock.merge(data.vclock);
    var nodes = GossipRing._ringDiff(this._ring, oldRing);
    return nodes;
  }

  /**
   *
   * @method _updateRound
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _updateRound(data) {
    if (data.type === "join") {
      return GossipRing.maxMsgRound(this._ring);
    }
    return data.round;
  }

  /**
   *
   * @method _makeConnects
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _makeConnects(nodes) {
    nodes.forEach(this._kernel.connect.bind(this._kernel));
    return this;
  }

  /**
   *
   * @method _makeDisconnects
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _makeDisconnects(nodes, force = false) {
    nodes.forEach(_.partial(this._kernel.disconnect, _, force).bind(this._kernel));
    return this;
  }

  /**
   *
   * @method _closeRing
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   * @fires Clusterluck.GossipRing#GossipRing:send
   * @fires Clusterluck.GossipRing#GossipRing:leave
   * @fires Clusterluck.GossipRing#GossipRing:close
   *
   */
  _closeRing() {
    var nodes = this.selectRandom(2);
    var actor = uuid.v4();
    var sendClock = this._leaveClock(actor);
    var sendRing = this._leaveRing();
    var msg = sendRing.toJSON(true);
    this.abcast(nodes, this._ringID, "ring", {
      type: "leave",
      actor: actor,
      data: msg,
      vclock: sendClock.toJSON(true),
      round: GossipRing.maxMsgRound(sendRing)-1
    });
    this.emit("send", sendClock, "ring", msg);

    /**
     *
     * Emitted when the ring state on this instance has been cleared, and the resulting ring with this node removed has been sent.
     *
     * @event Clusterluck.GossipRing#GossipRing:leave
     * @memberof Clusterluck.GossipRing
     * @property {Clusterluck.CHash} sendRing - Ring that will be sent to all nodes in the old cluster except this node (since we're leaving when this event has fired).
     *
     */
    this.emit("leave", sendRing);
    this._ringID = null;
    this._makeDisconnects(sendRing.nodes());

    /**
     *
     * Emitted when this instance has left it's current ring, overwritten it's ring ID to `null`, and disconnected from all other nodes in the cluster.
     *
     * @event Clusterluck.GossipRing#GossipRing:close
     * @memberof Clusterluck.GossipRing
     *
     */
    this.emit("close");
  }

  /**
   *
   * @method _leaveClock
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _leaveClock(actor) {
    var sendClock = this._vclock.increment(actor);
    this._actor = uuid.v4();
    this._vclock = new VectorClock(this._actor, 1);
    return sendClock;
  }

  /**
   *
   * @method _leaveRing
   * @memberof Clusterluck.GossipRing
   * @private
   * @instance
   *
   */
  _leaveRing() {
    var sendRing = this._ring.remove(this._kernel.self());
    this._ring = new CHash(this._ring.rfactor(), this._ring.pfactor());
    this._ring.insert(this._kernel.self());
    return sendRing;
  }

  /**
   *
   * @method _decodeRing
   * @memberof Clusterluck.GossipRing
   * @private
   * @static
   *
   */
  static _decodeRing(data) {
    return (new CHash()).fromJSON(data);
  }

  /**
   *
   * @method _encodeState
   * @memberof Clusterluck.GossipRing
   * @private
   * @static
   *
   */
  static _encodeState(state) {
    return Buffer.from(JSON.stringify(state));
  }

  /**
   *
   * @method _ringDiff
   * @memberof Clusterluck.GossipRing
   * @private
   * @static
   *
   */
  static _ringDiff(ringa, ringb) {
    var aNodes = ringa.nodes();
    var bNodes = ringb.nodes();
    var add = _.differenceWith(aNodes, bNodes, (a, b) => {
      return a.equals(b);
    });
    var rem = _.differenceWith(bNodes, aNodes, (a, b) => {
      return a.equals(b);
    });
    return [add, rem];
  }

  /**
   *
   * @method maxMsgRound
   * @memberof Clusterluck.GossipRing
   * @private
   * @static
   *
   */
  static maxMsgRound(ring) {
    if (ring.size() === 0) return 0;
    if (ring.size() === ring.rfactor()) return 1;
    return Math.ceil(Math.log2(ring.size()/ring.rfactor()));
  }

  /**
   *
   * Function to automatically handle ring conflicts, adopting a 'last-write-wins' approach (last insertion time stamp) between this instance's vector clock and `clock`. If `clock` has the last insertion, then this handler's ring will become `ring`. Otherwise, the current state is kept.
   *
   * @method LWW
   * @memberof Clusterluck.GossipRing
   * @private
   * @static
   *
   */
  static LWW(state1, clock1, state2, clock2) {
    var maxIn = clock1.nodes().reduce((memo, val) => {
      return Math.max(memo, clock1.getInsert(val));
    }, 0);
    var maxData = clock2.nodes().reduce((memo, val) => {
      return Math.max(memo, clock2.getInsert(val));
    }, maxIn);
    return maxData === maxIn ? state1 : state2;
  }
}

module.exports = GossipRing;

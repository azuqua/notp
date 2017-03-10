var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    EventEmitter = require("events").EventEmitter,
    microtime = require("microtime"),
    fs = require("fs"),
    shortid = require("shortid"),
    debug = require("debug")("clusterluck:lib:table");

var GenServer = require("./gen_server"),
    VectorClock = require("./vclock"),
    TableTerm = require("./table_term"),
    utils = require("./utils");

class StateTable extends GenServer {
  /**
   *
   * @class StateTable
   * @memberof NetKernel
   *
   * @param {NetKernel.GossipRing} gossip
   * @param {Object} opts
   *
   */
  constructor(kernel, gossip, opts) {
    super(kernel);
    this._gossip = gossip;
    this._values = new Map();
    this._vclockOpts = opts.vclockOpts;
    this._pollOpts = opts.pollOpts;
    this._pollCursor = null;
    this._pollInterval = null;
    this._flushOpts = opts.flushOpts;
    this._flushCursor = null;
    this._flushInterval = null;
    this._migrateCursor = null;
    this._migrations = new Map();
    this._purgeCursor = null;
    this._purgeCount = 0;
    this._purgeMax = opts.purgeMax || 5;
    this._stopped = true;
  }

  /**
   *
   * @method start
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {StateTable} This instance.
   *
   */
  start(name) {
    if (this._stopped === false) return this;
    super.start(name);
    this._stopped = false;
    this._gossip.registerTable(this);
    this._pollInterval = setInterval(this.poll.bind(this), this._pollOpts.interval);
    this._flushInterval = setInterval(this.flush.bind(this), this._flushOpts.interval);

    var handler = this.update.bind(this);
    this.on("state", handler);
    var migHandler = this.updateSync.bind(this);
    this.on("migrate", migHandler);

    var procHandler = this.migrate.bind(this);
    this._gossip.on("process", procHandler);
    var leaveHandler = _.partial(async.nextTick, this.purge.bind(this));
    this._gossip.once("leave", leaveHandler);

    this.once("stop", _.partial(this.removeListener, "state", handler).bind(this));
    this.once("stop", _.partial(this.removeListener, "migrate", migHandler).bind(this));
    this.once("stop", _.partial(this._gossip.removeListener, "process", procHandler).bind(this._gossip));
    this.once("stop", _.partial(this._gossip.removeListener, "leave", leaveHandler).bind(this._gossip));
    return this;
  }

  /**
   *
   * @method stop
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {Boolean} [force]
   *
   * @return {StateTable} This instance.
   *
   */
  stop(force = false) {
    if (this._stopped) return this;
    clearInterval(this._pollInterval);
    clearInterval(this._flushInterval);
    this._pollInterval = null;
    this._flushInterval = null;
    this._stopped = true;
    if (this.idle() || force === true) {
      [
        {cursor: "_pollCursor", next: "_nextPoll"},
        {cursor: "_migrateCursor", next: "_nextMigrate"},
        {cursor: "_flushCursor", next: "_nextFlush"}
      ].forEach((ent) => {
        if (this[ent.cursor] !== null) {
          this[ent.cursor] = null;
          if (this[ent.next]) {
            clearTimeout(this[ent.next]);
            this[ent.next] = null;
          }
        }
      });
      this._gossip.unregisterTable(this);
      this._streams = new Map();
      this.emit("stop");
      this._id = shortid.generate();
      return this;
    }
    this.once("idle", _.partial(this.emit, "stop").bind(this));
    return this;
  }

  decodeJob(buf) {
    var out = super.decodeJob(buf);
    var val = out.data;
    var vclock = (new VectorClock()).fromJSON(val.vclock);
    out.data = {
      id: val.data.id,
      term: new TableTerm(val.data.value, val.data.state, vclock, val.actor),
      round: val.round
    };
    return out;
  }

  /**
   *
   * Acts as a getter/setter for the gossip ring of this instance.
   *
   * @method gossip
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {NetKernel.GossipRing} [gossip] - GossipRing to set on this instance.
   *
   * @return {NetKernel.GossipRing} GossipRing of this instance.
   *
   */
  gossip(gossip) {
    if (gossip) {
      this._gossip = gossip;
    }
    return this._gossip;
  }

  /**
   *
   * Returns whether this handler is in an idle state or not.
   *
   * @method idle
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {Boolean} Whether this instance is idle or not.
   *
   */
  idle() {
    return super.idle() &&
      this._pollCursor === null &&
      this._migrateCursor === null &&
      this._flushCursor === null;
  }

  /**
   *
   * Returns the value stored at `key` in this instance.
   *
   * @method get
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {String} key - Key to select from this instance.
   *
   * @return {Object} Value stored at `key` in this instance.
   *
   */
  get(key) {
    return this._values.get(key);
  }

  /**
   *
   * @method set
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {String} key - Key to set `value` on in this instance.
   * @param {Object} value - Value to set under `key` on this instance.
   *
   * @return {StateTable} This instance.
   *
   */
  set(key, value) {
    this._values.set(key, value);
    return this;
  }

  /**
   *
   * Deletes the value stored at `key` in this instance.
   *
   * @method delete
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {String} key - Key to delete from this instance.
   *
   * @return {StateTable} This instance.
   *
   */
  delete(key) {
    this._values.delete(key);
    return this;
  }

  /**
   *
   * Returns whether `key` has a defined value in this instance.
   *
   * @method has
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {String} key - Key to check for existence in this instance.
   *
   * @return {Boolean} Whether `key` exists in this instance.
   *
   */
  has(key) {
    return this._values.has(key);
  }

  /**
   *
   * Creates an iterator over the key-space stored in this table.
   *
   * @method keys
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {MapIterator} Iterator over the keys stored in this table.
   *
   */
  keys() {
    return this._values.keys();
  }

  /**
   *
   * Creates an iterator over the value-space stored in this table.
   *
   * @method values
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {MapIterator} Iterator over the values stored in this table.
   *
   */
  values() {
    return this._values.values();
  }

  /**
   *
   * Creates an iterator over the key/value-space stored in this table.
   *
   * @method iterator
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {MapIterator} Iterator over the key/value pairs stored in this table.
   *
   */
  iterator() {
    return this._values.entries();
  }

  /**
   *
   * @method update
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {Object} data - Data to update state with on this instance.
   *
   * @return {StateTable} This instance.
   *
   */
  update(data) {
    var bucket = this._gossip.find(data.id);
    var rand = this._gossip.selectRandomFrom(bucket, 2);
    var conn = this._values.get(data.id) || data.term;
    this._mergeState(conn, data.id, data.term);

    conn.vclock().increment(data.term.actor());
    conn.actor(data.term.actor());
    this._values.set(data.id, conn);
    this.route(rand, "state", {
      id: data.id,
      state: conn.state(),
      value: conn.value()
    }, conn.actor(), conn.vclock(), data.round);
    return this;
  }

  /**
   *
   * @method updateSync
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {Object} data -Data to update state with on this instance.
   *
   * @return {StateTable} This instance.
   *
   */
  updateSync(data, from) {
    this.update(data);
    this._migrations.delete(data.id);
    this.reply(from, JSON.stringify({ok: true}));
  }

  /**
   *
   * @method migrate
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {NetKernel.CHash} oldRing
   * @param {NetKernel.CHash} newRing
   *
   * @return {StateTable} This instance.
   *
   */
  migrate(oldRing, newRing) {
    if (this._migrateCursor !== null || this._stopped) return this;
    if (oldRing.equals(newRing)) return this;
    this.emit("migrating");
    if (this._pollCursor) {
      this._pollCursor = null;
      if (this._nextPoll) clearTimeout(this._nextPoll);
    }
    this._migrateCursor = this._values.entries();
    this._migrateState(oldRing, newRing);
    return this;
  }

  /**
   *
   * @method poll
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {StateTable} This instance.
   *
   */
  poll() {
    if (this._pollCursor !== null ||
        this._migrateCursor !== null ||
        this._stopped) return this;
    this.emit("poll");
    this._pollCursor = this._values.entries();
    this._pollState();
    return this;
  }

  /**
   *
   * @method flush
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {StateTable} This instance.
   *
   */
  flush() {
    if (this._flushCursor !== null ||
        this._stopped) return this;
    this.emit("flushing");
    return this;
  }

  /**
   *
   * @method flushSync
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @return {StateTable} This instance.
   *
   */
  flushSync() {
    if (this._flushCursor !== null ||
        this._stopped) return this;
    this.emit("flushing");
    return this;
  }

  /**
   *
   * @method purge
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {NetKernel.CHash} sendRing
   *
   * @return {StateTable} This instance.
   *
   */
  purge(sendRing) {
    if (this._purgeCursor !== null || this._stopped) return this;
    this.emit("purging");
    this.stop(true);
    this._purgeCursor = this._values.entries();
    this._purgeState(sendRing);
    return this;
  }

  /**
   *
   * Send `event` of this ring at round `n` with message `msg`, sending it to `nodes` if the round number is greater than zero.
   *
   * @method route
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {Array} nodes
   * @param {String} event
   * @param {Object} msg
   * @param {String} actor
   * @param {NetKernel.VectorClock} clock
   * @param {Number} [n]
   *
   * @return {StateTable} This instance.
   *
   */
  route(nodes, event, msg, actor, clock, n = 1) {
    if (n === 0) return this;
    this.abcast(nodes, this._id, JSON.stringify({
      event: "state",
      actor: actor,
      data: msg,
      vclock: clock.toJSON(true),
      round: --n
    }));
    this.emit("send", clock, event, msg);
    return this;
  }

  /**
   *
   * Send `event` of this ring at round `n` with message `msg`, sending it to `nodes` if the round number is greater than zero. Wait for a reply from each node in `nodes` before calling `cb`.
   *
   * @method routeSync
   * @memberof NetKernel.StateTable
   * @instance
   *
   * @param {Array} nodes
   * @param {String} event
   * @param {Object} msg
   * @param {String} actor
   * @param {NetKernel.VectorClock} clock
   * @param {Function} cb
   * @param {Number} [n]
   *
   * @return {StateTable} This instance.
   *
   */
  routeSync(nodes, event, msg, actor, clock, cb, n = 1) {
    if (n === 0) return this;
    this.multicall(nodes, this._id, JSON.stringify({
      event: event,
      actor: actor,
      data: msg,
      vclock: clock.toJSON(true),
      round: --n
    }), (err, data) => {
      if (err) return cb(err);
      return cb();
    }, 5000);
    this.emit("send", clock, event, msg);
    return this;
  }

  /**
   *
   * @method _mergeState
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   * @param {Object} conn
   * @param {Object} data
   *
   * @return {StateTable} This instance.
   *
   */
  _mergeState(conn, id, msg) {
    if (msg.vclock().descends(conn.vclock())) {
      conn.state(msg.state());
      conn.value(msg.value());
      conn.vclock(msg.vclock());
    }
    else if(!conn.vclock().descends(msg.vclock())) {
      debug("State conflict on entry " + id);
      this.emit("conflict", id, msg);
    }
    this.emit(conn.state(), id, conn.value());
    return this;
  }

  /**
   *
   * @method _migrateState
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   * @param {NetKernel.CHash} oldRing
   * @param {NetKernel.CHash} newRing
   *
   */
  _migrateState(oldRing, newRing) {
    var cursor = utils.scanIterator(this._migrateCursor, this._pollOpts.block);
    this._migrateCursor = cursor.iterator;
    this._nextMigrate = null;
    async.each(cursor.values, (entry, done) => {
      if (this._migrateCursor !== cursor.iterator) return done(new Error("Migration stopped prematurely."));
      var val = entry[1],
          key = entry[0];
      this._migrateClock(cursor, val, key, 1, oldRing, newRing, (err, out) => {
        if (err) return done(err);
        return done();
      });
    }, (err) => {
      if (err) return this;
      if (this._migrateCursor === cursor.iterator &&
          cursor.done !== true) {
        this._nextMigrate = setTimeout(_.partial(this._migrateState, oldRing, newRing).bind(this), 10);
      }
      else if (this._migrateCursor === cursor.iterator &&
          cursor.done === true) {
        this._migrateCursor = null;
        if (this.idle()) this.emit("idle");
      }
    });
  }

  /**
   *
   * @method _pollState
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   */
  _pollState() {
    var time = microtime.now();
    var cursor = utils.scanIterator(this._pollCursor, this._pollOpts.block);
    this._pollCursor = cursor.iterator;
    this._nextPoll = null;
    async.each(cursor.values, (entry, done) => {
      if (this._pollCursor !== cursor.iterator) return done(new Error("Polling stopped prematurely."));
      var val = entry[1],
          key = entry[0];
      val.vclock().trim(time, this._vclockOpts);
      this._filterClock(cursor, val, key, 1, (err, out) => {
        if (err) return done(err);
        if (out) return done();
        if (val.vclock().size() === 0 && val.state() === "destroy") {
          this._values.delete(key);
          return done();
        }
        var bucket = this._gossip.selectRandom(this._gossip.find(key), 2);
        this.route(bucket, "state", {
          id: key,
          state: val.state(),
          value: val.value()
        }, val.actor(), val.vclock(), 1);
        return done();
      });
    }, (err) => {
      if (err) return this;
      if (this._pollCursor === cursor.iterator &&
          cursor.done !== true) {
        this._nextPoll = setTimeout(_.partial(this._pollState).bind(this), 10);
      }
      else if (this._pollCursor === cursor.iterator &&
          cursor.done === true) {
        this._pollCursor = null;
        if (this.idle()) this.emit("idle");
      }
    });
  }

  /**
   *
   * @method _purgeState
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   * @param {NetKernel.CHash} sendRing
   *
   */
  _purgeState(sendRing) {
    var corrupt = this._purgeCursor.corrupt || false;
    var cursor = utils.scanIterator(this._purgeCursor, this._pollOpts.block);
    this._purgeCursor = cursor.iterator;
    this._nextPurge = null;
    async.each(cursor.values, (entry, done) => {
      var val = entry[1],
          key = entry[0];
      this._purgeClock(val, key, 1, sendRing, (out) => {
        if (out === false) corrupt = true;
        return done();
      });
    }, (err) => {
      if (this._purgeCursor === cursor.iterator && cursor.done !== true) {
        this._purgeCursor.corrupt = corrupt;
        this._nextPurge = setTimeout(_.partial(this._purgeState, sendRing).bind(this), 10);
      }
      // restart purge if some values couldn't successfully be resharded
      else if (corrupt) {
        if ((++this._purgeCount) >= this._purgeMax) {
          // emit error, but also just restart for practical reasons
          this.emit("error", new Error("Could not complete state purge."));
          this.emit("purge");
        }
        this._purgeCursor = this._values.entries();
        setTimeout(_.partial(this._purgeState, sendRing).bind(this), 10);
      }
      else {
        this._purgeCount = 0;
        this.emit("purge");
      }
    });
  }

  /**
   *
   * @method _migrateClock
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   * @param {Object} val
   * @param {String} key
   * @param {Number} n
   * @param {NetKernel.CHash} oldRing
   * @param {NetKernel.CHash} newRing
   * @param {Function} cb
   *
   */
  _migrateClock(cursor, val, key, n, oldRing, newRing, cb) {
    var self = this._kernel.self().id();
    var node = oldRing.find(key);
    var oldBucket = [node].concat(oldRing.next(node));
    var bucket = this._gossip.find(key);
    var add = _.differenceWith(bucket, oldBucket, (a, b) => {
      return a.equals(b);
    });
    this._migrations.set(key, add);
    this.routeSync(add, "migrate", {
      id: key,
      state: val.state(),
      value: val.value()
    }, val.actor(), val.vclock(), (err) => {
      if (this._migrateCursor !== cursor.iterator) return cb(new Error("Migration stopped prematurely."));
      if (err) return cb(null, false);
      var afterBucket = this._gossip.find(key).map((node) => {return node.id();});
      if (afterBucket.indexOf(self) > -1) return cb(null, false);
      if (!this._migrations.has(key)) return cb(null, false);
      this._migrations.delete(key);
      this._values.delete(key);
      return cb(null, true);
    }, n);
  }

  /**
   *
   * @method _filterClock
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   * @param {Object} val
   * @param {String} key
   * @param {Number} n
   * @param {Function} cb
   *
   */
  _filterClock(cursor, val, key, n, cb) {
    var self = this._kernel.self().id();
    var bucket = this._gossip.find(key);
    var names = bucket.map((node) => {return node.id();});
    if (names.indexOf(self) > -1) return cb(null, false);
    this._migrations.set(key, names);
    this.routeSync(this._gossip.selectRandom(bucket, 1), "migrate", {
      id: key,
      state: val.state(),
      value: val.value()
    }, val.actor(), val.vclock(), (err) => {
      if (this._pollCursor !== cursor.iterator) return cb(new Error("Polling stopped prematurely."));
      if (err) return cb(null, false);
      var afterBucket = this._gossip.find(key).map((node) => {return node.id();});
      if (afterBucket.indexOf(self) > -1) return cb(null, false);
      if (!this._migrations.has(key)) return cb(null, false);
      this._migrations.delete(key);
      this._values.delete(key);
      return cb(null, true);
    }, n);
  }

  /**
   *
   * @method _purgeClock
   * @memberof NetKernel.StateTable
   * @private
   * @instance
   *
   * @param {Object} val
   * @param {String} key
   * @param {Number} n
   * @param {NetKernel.CHash} sendRing
   * @param {Function} cb
   *
   */
  _purgeClock(val, key, n, sendRing, cb) {
    var self = this._kernel.self().id();
    var node = sendRing.find(key);
    var bucket = [node].concat(sendRing.next(node));
    this.routeSync(bucket, "migrate", {
      id: key,
      state: val.state(),
      value: val.value()
    }, val.actor(), val.vclock(), (err) => {
      if (err) return cb(false);
      this._values.delete(key);
      return cb(true);
    }, n);
  }
}

module.exports = StateTable;

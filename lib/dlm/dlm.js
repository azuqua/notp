const _ = require("lodash"),
      microtime = require("microtime"),
      util = require("util"),
      async = require("async"),
      debug = require("debug")("clusterluck:lib:dlm");

const GenServer = require("../gen_server"),
      DTable = require("../dtable"),
      MTable = require("../mtable"),
      Lock = require("./lock"),
      utils = require("../utils"),
      consts = require("../consts");

const mcsToMs = 1000;
const serverDefaults = consts.dlmOpts;

const lockTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."},
  {key: "holder", valid: [util.isString], str: "Must be a JSON string."},
  {key: "timeout", valid: [utils.isPositiveNumber], str: "Must be a positive JSON number."}
];

const unlockTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."},
  {key: "holder", valid: [util.isString], str: "Must be a JSON string."}
];

const parseTypes = {
  "rlock": lockTypes,
  "wlock": lockTypes,
  "runlock": unlockTypes,
  "wunlock": unlockTypes
};

class DLMServer extends GenServer {
  /**
   *
   * Distributed lock manager server, implemented using the Redlock algorithm {@link https://redis.io/topics/distlock}.
   *
   * @class DLMServer
   * @memberof Clusterluck
   *
   * @param {Clusterluck.GossipRing} gossip - Gossip ring to coordinate ring state from.
   * @param {Clusterluck.NetKernel} kernel - Network kernel to communicate with other nodes.
   * @param {Object} [opts] Options object containing information about read/write quorums, disk persistence options, and wait times for retry logic on lock requests.
   * @param {Number} [opts.rquorum] - Quorum for read lock requests.
   * @param {Number} [opts.wquorum] - Quorum for write lock requests.
   * @param {Number} [opts.rfactor] - Replication factor for number of nodes to involve in a quorum.
   * @param {Number} [opts.minWaitTimeout] - Minimum amount of time in milliseconds to wait for a retry on a locking request.
   * @param {Number} [opts.maxWaitTimeout] - Maximum amount of time in milliseconds to wait for a retry on a locking request.
   * @param {Boolean} [opts.disk] - Whether to persist lock state to disk. If `true` is passed, the following options will be read.
   * @param {String} [opts.path] - Path for underlying DTable instance to flush state to.
   * @param {Number} [opts.writeThreshold] - Write threshold of underlying DTable instance.
   * @param {Number} [opts.autoSave] - Autosave interval of underlying DTable instance.
   * @param {Number} [opts.fsyncInterval] - Fsync interval of underlying DTable instance.
   *
   */
  constructor(gossip, kernel, opts = serverDefaults) {
    super(kernel);
    opts = _.defaults(_.clone(opts), serverDefaults);
    this._gossip = gossip;
    this._kernel = kernel;
    this._locks = new Map();
    this._rquorum = opts.rquorum;
    this._wquorum = opts.wquorum;
    this._rfactor = opts.rfactor;
    this._minWaitTimeout = opts.minWaitTimeout;
    this._maxWaitTimeout = opts.maxWaitTimeout;
    this._disk = opts.disk;
    if (this._disk === true) {
      this._table = new DTable(_.pick(opts, [
        "path",
        "writeThreshold",
        "autoSave",
        "compress"
      ]));
    } else {
      this._disk = false;
      this._table = new MTable();
    }
    this._stopped = true;
  }

  /**
   *
   * Starts a DLM handler: listens for events related to lock and unlock requests on the netkernel. Also starts the underlying table storing locks and lock holders.
   *
   * @method start
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} [name] - Name to register this handler with instead of the unique id attached. Any message received on the network kernwl with id `name` will be routed to this instance for message stream parsing, and possible event firing.
   *
   * @return {Clusterluck.DLMServer} This instance.
   *
   */
  start(name) {
    super.start(name);
    this._stopped = false;
    
    const jobs = [
      {event: "rlock", method: "_doRLock"},
      {event: "wlock", method: "_doWLock"},
      {event: "runlock", method: "_doRUnlock"},
      {event: "wunlock", method: "_doWUnlock"}
    ];
    jobs.forEach((job) => {
      const handler = this[job.method].bind(this);
      this.on(job.event, handler);
      this.once("stop", _.partial(this.removeListener, job.event, handler).bind(this));
    });

    // listen for 'idle' event on table to update this gen_server's idle state
    this._table.start(name);
    const handler = () => {
      if (this.idle()) this.emit("idle");
    };
    this._table.on("idle", handler);
    this.once("stop", _.partial(this._table.removeListener, "idle", handler).bind(this._table));
    return this;
  }

  /**
   *
   * Stops this handler. If the table is idle, this function will transition into clearing all locks and table state, and stopping the underlying table. Otherwise, this function will wait to complete until this instance is in an idle state.
   *
   * @method stop
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @return {Clusterluck.DLMServer} This instance.
   *
   */
  stop() {
    if (this.idle()) {
      debug("Stopping DLM server %s.", this._id);
      this._locks.forEach((lock) => {
        if (lock.type() === "write") {
          clearTimeout(lock.timeout());
        } else {
          lock.timeout().forEach((t) => {
            clearTimeout(t);
          });
        }
      });
      this._locks.clear();
      this._table.clear();
      this._stopLogging();
      return this;
    }
    debug("Waiting for DLM server %s to become idle before stopping...", this._id);
    this.once("idle", _.partial(this.stop).bind(this));
    return this;
  }

  /**
   *
   * Loads state from disk for the underlying table this instance uses for state persistence. If the `disk` option is set to false on construction, this function will immediately return and call `cb`. NOTE: this function should be called after `start` is called, as the underlying table needs to be started before any files can be read from disk.
   *
   * @method load
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Function} cb - Function of the form `function (err) {...}`, where `err` will be passed if an error occurs loading state from disk.
   *
   */
  load(cb) {
    debug("Loading table from disk for %s...", this._id);
    this._table.load((err) => {
      if (err) return cb(err);
      this._table.forEach((key, val, next) => {
        let lock;
        if (val instanceof Map) {
          lock = new Lock("read", key, new Map());
          val.forEach((innerV, innerK) => {
            const nTime = Math.max(0, innerV.timeout+innerV.start-Date.now());
            const timeout = setTimeout(_.partial(this._clearRLock, key, innerK).bind(this), nTime);
            lock.timeout().set(innerK, timeout);
          });
        } else {
          const nTime = Math.max(0, val.timeout+val.start-Date.now());
          const timeout = setTimeout(_.partial(this._clearWLock, key, val.holder).bind(this), nTime);
          lock = new Lock("write", key, timeout);
        }
        this._locks.set(key, lock);
        async.nextTick(next);
      }, cb);
    });
  }

  /**
   *
   * Returns whether this instance is idle or not. Checks for both active requests as well as the underlying table's state for idleness.
   *
   * @method idle
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @return {Boolean} Whether this instance is idle or not.
   *
   */
  idle() {
    return super.idle() && this._table.idle();
  }

  /**
   *
   * Makes a read lock request against `id` with `holder` identifying the requester of this lock (think actor). `holder` should be a randomly generated string if looking for different requests to represent different actions, such as a UUID or the result of a `crypto.randomBytes` call. The lock will last for `timeout` milliseconds before being automatically released on the other nodes this lock routes to. The algorithm consists of:
   *
   *   - Use the internal gossip server to find the set of nodes responsible for `id` on the hash ring.
   *   - Make a request to the DLM server on these other nodes to execute the read lock command.
   *   - Based on the responses, if a read quorum is met and the response is returned within `timeout` milliseconds, then the request was successful and we return the set of nodes holding this lock.
   *   - Otherwise, we asynchronously unlock this rlock on the successful nodes and set a random timeout to retry the request. If we've retried `retries` number of times, then an error is returned and retry logic ceases.
   *
   * The main difference between read locks and write locks is that write locks enforce exclusivity (they're equivalent to mutexes). Read locks, conversely, allow concurrency of other read lock requests.
   *
   * @method rlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id - ID of resource to lock.
   * @param {String} holder - ID of actor/requester for this lock.
   * @param {Number} timeout - How long the lock will last on each node holding this lock, in milliseconds.
   * @param {Function} cb - Function of form `function (err, nodes) {...}`, where `nodes` is the array of nodes holding this lock. `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a lock attempt before considering the request errored. Defaults to `Infinity`.
   * @param {Number} [retries] - Number of times to retry this request. Defaults to `Infinity`.
   *
   */
  rlock(id, holder, timeout, cb, reqTimeout=Infinity, retries=Infinity) {
    const nodes = this._gossip.range(id, this._rfactor);
    const time = microtime.now();
    debug("Making rlock request on %s against %s with holder %s...", this._id, id, holder);
    this.multicall(nodes, this._id, "rlock", {
      id: id,
      holder: holder,
      timeout: timeout
    }, (err, data) => {
      if (err) {
        debug("Error making rlock request on %s against %s, error: %s", this._id, id, err.message);
        return cb(err);
      }
      const delta = (microtime.now()-time)/mcsToMs;
      const nData = DLMServer.findLockPasses(nodes, data);
      if (nData.length/data.length >= this._rquorum && delta < timeout) {
        debug("rlock request on %s against %s succeeded.", this._id, id);
        return cb(null, nData);
      } else {
        this.runlockAsync(id, holder);
        if (--retries < 0) {
          debug("Failed to grab rlock on %s for %s after exhausting retry count.", this._id, id);
          return cb(new Error("Failed to acquire rlock, retry count exhausted."));
        }
        const wtime = DLMServer.calculateWaitTime(this._minWaitTimeout, this._maxWaitTimeout);
        debug("rlock request on %s against %s failed, retrying after %i milliseconds...", this._id, id, wtime);
        setTimeout(_.partial(this.rlock, id, holder, timeout, cb, reqTimeout, retries).bind(this), wtime);
      }
    }, reqTimeout);
  }

  /**
   *
   * Makes a write lock request against `id` with `holder` identifying the requester of this lock (think actor). `holder` should be a randomly generated string if looking for different requests to represent different actions, such as a UUID or the result of a `crypto.randomBytes` call. The lock will last for `timeout` milliseconds before being automatically released on the other nodes this lock routes to. The algorithm consists of:
   *
   *   - Use the internal gossip server to find the set of nodes responsible for `id` on the hash ring.
   *   - Make a request to the DLM server on these other nodes to execute the write lock command.
   *   - Based on the responses, if a write quorum is met and the response is returned within `timeout` milliseconds, then the request was successful and we return the set of nodes holding this lock.
   *   - Otherwise, we asynchronously unlock this wlock on the successful nodes and set a random timeout to retry the request. If we've retried `retries` number of times, then an error is returned and retry logic ceases.
   *
   * The main difference between read locks and write locks is that write locks enforce exclusivity (they're equivalent to mutexes). Read locks, conversely, allow concurrency of other read lock requests.
   *
   * @method wlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id - ID of resource to lock.
   * @param {String} holder - ID of actor/requester for this lock.
   * @param {Number} timeout - How long the lock will last on each node holding this lock, in milliseconds.
   * @param {Function} cb - Function of form `function (err, nodes) {...}`, where `nodes` is the array of nodes holding this lock. `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a lock attempt before considering the request errored. Defaults to `Infinity`.
   * @param {Number} [retries] - Number of times to retry this request. Defaults to `Infinity`.
   *
   */
  wlock(id, holder, timeout, cb, reqTimeout=Infinity, retries=Infinity) {
    const nodes = this._gossip.range(id, this._rfactor);
    const time = microtime.now();
    debug("Making wlock request on %s against %s with holder %s...", this._id, id, holder);
    this.multicall(nodes, this._id, "wlock", {
      id: id,
      holder: holder,
      timeout: timeout
    }, (err, data) => {
      if (err) {
        debug("Error making wlock request on %s against %s, error: %s", this._id, id, err.message);
        return cb(err);
      }
      const delta = (microtime.now()-time)/mcsToMs;
      const nData = DLMServer.findLockPasses(nodes, data);
      if (nData.length/data.length >= this._wquorum && delta < timeout) {
        debug("wlock request on %s against %s succeeded.", this._id, id);
        return cb(null, nData);
      } else {
        this.wunlockAsync(id, holder);
        if (--retries < 0) {
          debug("Failed to grab wlock on %s for %s after exhausting retry count.", this._id, id);
          return cb(new Error("Failed to acquire wlock, retry count exhausted."));
        }
        const wtime = DLMServer.calculateWaitTime(this._minWaitTimeout, this._maxWaitTimeout);
        debug("wlock request on %s against %s failed, retrying after %i milliseconds...", this._id, id, wtime);
        setTimeout(_.partial(this.wlock, id, holder, timeout, cb, reqTimeout, retries).bind(this), wtime);
      }
    }, reqTimeout);
  }

  /**
   *
   * Unlocks the read lock `id` with holder `holder`. If the request takes longer than `reqTimeout`, `cb` is called with a timeout error. Otherwise, `cb` is called with no arguments. The algorithm consists of:
   *   - Use the internal gossip server to find the set of nodes responsible for `id` on the hash ring.
   *   - Make a request to the DLM server on these other nodes to execute the read unlock command.
   *   - If an error is returned, call `cb` with that error.
   *   - Otherwise, call `cb` with no arguments.
   *
   * @method runlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id - ID of resource to unlock.
   * @param {String} holder - ID of actor/requester for this lock.
   * @param {Function} cb - Function of form `function (err) {...}`, where `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for an unlock attempt before considering the request errored. Defaults to `Infinity`.
   *
   */
  runlock(id, holder, cb, reqTimeout=Infinity) {
    const nodes = this._gossip.range(id, this._rfactor);
    debug("Unlocking rlock %s on %s with holder %s...", id, this._id, holder);
    this.multicall(nodes, this._id, "runlock", {
      id: id,
      holder: holder
    }, (err, res) => {
      if (err) return cb(err);
      return cb();
    }, reqTimeout);
  }

  /**
   *
   * Asynchronously unlocks the read lock `id` with holder `holder`. The algorithm consists of:
   *   - Use the internal gossip server to find the set of nodes responsible for `id` on the hash ring.
   *   - Make an asynchronous request to the DLM server on these other nodes to execute the read unlock command.
   *
   * @method runlockAsync
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id - ID of resource to unlock.
   * @param {String} holder - ID of actor/requester for this lock.
   *
   */
  runlockAsync(id, holder) {
    const nodes = this._gossip.range(id, this._rfactor);
    debug("Asynchronously unlocking rlock %s on %s with holder %s...", id, this._id, holder);
    this.abcast(nodes, this._id, "runlock", {
      id: id,
      holder: holder
    });
  }

  /**
   *
   * Unlocks the write lock `id` with holder `holder`. If the request takes longer than `reqTimeout`, `cb` is called with a timeout error. Otherwise, `cb` is called with no arguments. The algorithm consists of:
   *   - Use the internal gossip server to find the set of nodes responsible for `id` on the hash ring.
   *   - Make a request to the DLM server on these other nodes to execute the write unlock command.
   *   - If an error is returned, call `cb` with that error.
   *   - Otherwise, call `cb` with no arguments.
   *
   * @method wunlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id - ID of resource to unlock.
   * @param {String} holder - ID of actor/requester for this lock.
   * @param {Function} cb - Function of form `function (err) {...}`, where `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for an unlock attempt before considering the request errored. Defaults to `Infinity`.
   *
   */
  wunlock(id, holder, cb, reqTimeout=Infinity) {
    const nodes = this._gossip.range(id, this._rfactor);
    debug("Unlocking wlock %s on %s with holder %s...", id, this._id, holder);
    this.multicall(nodes, this._id, "wunlock", {
      id: id,
      holder: holder
    }, (err, res) => {
      if (err) return cb(err);
      return cb();
    }, reqTimeout);
  }

  /**
   *
   * Asynchronously unlocks the write lock `id` with holder `holder`. The algorithm consists of:
   *   - Use the internal gossip server to find the set of nodes responsible for `id` on the hash ring.
   *   - Make an asynchronous request to the DLM server on these other nodes to execute the write unlock command.
   *
   * @method wunlockAsync
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id - ID of resource to unlock.
   * @param {String} holder - ID of actor/requester for this lock.
   *
   */
  wunlockAsync(id, holder) {
    const nodes = this._gossip.range(id, this._rfactor);
    debug("Asynchronously unlocking wlock %s on %s with holder %s...", id, this._id, holder);
    this.abcast(nodes, this._id, "wunlock", {
      id: id,
      holder: holder
    });
  }

  /**
   *
   * Parses a fully memoized message stream into an object containing a key/value pair. If we fail to parse the job buffer (invalid JSON, etc), we just return an error and this GenServer will skip emitting an event. Otherwise, triggers user-defined logic for the parsed event.
   *
   * @method decodeJob
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Buffer} buf - Memoized buffer that represents complete message stream.
   *
   * @return {Object|Error} Object containing an event and data key/value pair, which are used to emit an event for user-defined logic.
   *
   */
  decodeJob(buf) {
    const out = super.decodeJob(buf);
    if (out instanceof Error) return out;
    const data = DLMServer.parseJob(out.data, out.event);
    if (data instanceof Error) {
      return data;
    }
    out.data = data;
    return out;
  }

  /**
   *
   * Parses a singleton message stream into an object containing a key/value pair. If we fail to parse the job object (invalid format for given event value, etc.), we just return an error and this GenServer will skip emitting an event. Otherwise, triggers user-defined logic for the parsed event.
   *
   * @method decodeSingleton
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Object} data - Message to be processed with `event` and `data` parameters.
   *
   * @return {Object|Error} Object containing an event and data key/value pair, which are used to emit an event for user-defined logic.
   *
   */
  decodeSingleton(data) {
    data = super.decodeSingleton(data);
    const nData = DLMServer.parseJob(data.data, data.event);
    if (nData instanceof Error) return nData;
    data.data = nData;
    return data;
  }

  /**
   *
   * @method _doRLock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DLMServer}
   *
   */
  _doRLock(data, from) {
    let lock = this._locks.get(data.id);
    if (lock && lock.type() === "write") {
      debug("rlock request on %s against %s failed, lock already exists as a write lock.", this._id, data.id);
      return this.reply(from, {ok: false});
    } else if (lock && lock.timeout().has(data.holder)) {
      debug("Holder %s already exists on rlock %s on %s", data.holder, data.id, this._id);
      return this.reply(from, {ok: true});
    }

    const timeout = setTimeout(_.partial(this._clearRLock, data.id, data.holder).bind(this), data.timeout);
    const created = Date.now();
    if (!lock) {
      lock = new Lock("read", data.id, new Map());
    }
    lock.timeout().set(data.holder, timeout);
    this._locks.set(data.id, lock);
    this._writeToLog("hset", data.id, data.holder, {start: created, timeout: data.timeout});
    debug("Added holder %s to rlock %s on %s.", data.holder, data.id, this._id);
    return this.reply(from, {ok: true});
  }

  /**
   *
   * @method _doWLock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DLMServer}
   *
   */
  _doWLock(data, from) {
    if (this._locks.has(data.id)) {
      debug("wlock request on %s against %s failed, lock already exists.", this._id, data.id);
      return this.reply(from, {ok: false});
    }

    const timeout = setTimeout(_.partial(this._clearWLock, data.id, data.holder).bind(this), data.timeout);
    const created = Date.now();
    this._locks.set(data.id, new Lock("write", data.id, timeout));
    this._writeToLog("set", data.id, {holder: data.holder, start: created, timeout: data.timeout});
    debug("added holder %s to wlock %s on %s.", data.holder, data.id, this._id);
    return this.reply(from, {ok: true});
  }

  /**
   *
   * @method _doRUnlock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DLMServer}
   *
   */
  _doRUnlock(data, from) {
    const lock = this._locks.get(data.id);
    if (!lock || lock.type() !== "read") {
      debug("runlock request on %s against %s failed, lock isn't an rlock.", this._id, data.id);
      return this._safeReply(from, {ok: false});
    }

    const timeouts = lock.timeout();
    if (timeouts.has(data.holder)) {
      clearTimeout(timeouts.get(data.holder));
      timeouts.delete(data.holder);

      this._writeToLog("hdel", data.id, data.holder);
      if (timeouts.size === 0) {
        this._locks.delete(data.id);
      }
      debug("removed holder %s from rlock %s on %s.", data.holder, data.id, this._id);
      return this._safeReply(from, {ok: true});
    } else {
      debug("runlock request on %s against %s failed, holder %s is no longer associated.", this._id, data.id, data.holder);
      return this._safeReply(from, {ok: false});
    }
  }

  /**
   *
   * @method _doWUnlock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DLMServer}
   *
   */
  _doWUnlock(data, from) {
    const lock = this._locks.get(data.id);
    if (!lock || lock.type() !== "write") {
      debug("wunlock request on %s against %s failed, lock isn't a wlock.", this._id, data.id);
      return this._safeReply(from, {ok: false});
    }

    const entry = this._table.get(data.id);
    if (entry.holder === data.holder) {
      clearTimeout(lock.timeout());
      this._writeToLog("del", data.id);
      this._locks.delete(data.id);
      debug("removed holder %s from wlock %s on %s.", data.holder, data.id, this._id);
      return this._safeReply(from, {ok: true});
    } else {
      debug("wunlock request on %s against %s failed, holder %s is no longer associated.", this._id, data.id, data.holder);
      return this._safeReply(from, {ok: false});
    }
  }

  /**
   *
   * @method _clearRLock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {String} id
   * @param {String} holder 
   *
   */
  _clearRLock(id, holder) {
    const lock = this._locks.get(id);
    if (!lock || lock.type() === "write") {
      return;
    }
    const member = lock.timeout();
    if (member.has(holder)) {
      member.delete(holder);
      this._writeToLog("hdel", id, holder);
      if (member.size === 0) {
        this._locks.delete(id);
      }
    }
  }

  /**
   *
   * @method _clearWLock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {String} id
   * @param {String} holder
   *
   */
  _clearWLock(id, holder) {
    const lock = this._locks.get(id);
    const entry = this._table.get(id);
    if (!lock || entry.holder !== holder) {
      return;
    }
    this._writeToLog("del", id);
    this._locks.delete(id);
  }

  /**
   *
   * @method _writeToLog
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {String} action
   *
   */
  _writeToLog(action, ...args) {
    this._table[action].apply(this._table, args);
  }

  /**
   *
   * @method _stopLogging
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   */
  _stopLogging() {
    // remove method listeners first, so that we don't have requests running in the middle
    // of stopping the table
    this.pause();
    // then stop the table
    this._table.stop(() => {
      super.stop();
      this._stopped = true;
    });
  }

  /**
   *
   * Parse and validate `job` for correct structure and type adherence.
   *
   * @method parseJob
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Object} job - Job to parse/validate.
   * @param {String} command - Command `job` corresponds to. This determines which set of static type definitions to validate `job` against.
   *
   * @return {Object|Error} An object if successfully parsed/validated, otherwise an Error indicating the reason for failure.
   *
   */
  static parseJob(job, command) {
    if (!util.isObject(job)) {
      return new Error("Malformed job; must be an object.");
    } else if (!parseTypes[command]) {
      return new Error("Malformed job; unrecognized command '" + command + "'");
    }
    const bads = parseTypes[command].reduce((memo, check) => {
      const valid = _.some(check.valid, (fn) => {
        return fn(job[check.key]);
      });
      if (!valid) memo[check.key] = check.str;
      return memo;
    }, {});
    if (_.size(bads) > 0) {
      return _.extend(new Error("Malformed job for command '" + command + "'."), {
        fails: bads
      });
    }
    return job;
  }

  /**
   *
   * Returns the set of nodes with successful responses according to `data`, where each node in `nodes` has a corresponding response in `data` according to index.
   *
   * @method findLockPasses
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Array} nodes - Array of nodes.
   * @param {Array} data - Array of responses, with a 1-1 correspondance to `nodes` based on index.
   *
   * @return {Array} Array of nodes with successful responses.
   *
   */
  static findLockPasses(nodes, data) {
    return data.reduce((memo, val, idx) => {
      if (val.ok === true) {
        memo.push(nodes[idx]);
      }
      return memo;
    }, []);
  }

  /**
   *
   * Calculates wait time for retry functionality in rlock and wlock requests.
   *
   * @method calculateWaitTime
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Number} min - Minimum wait time.
   * @param {Number} max - Maximum wait time.
   *
   * @return {Number} Amount of time to wait.
   *
   */
  static calculateWaitTime(min, max) {
    return Math.random()*(max-min)+min;
  }
}

module.exports = DLMServer;

const _ = require("lodash"),
      microtime = require("microtime"),
      util = require("util"),
      async = require("async"),
      stream = require("stream"),
      debug = require("debug")("clusterluck:lib:dsm");

const GenServer = require("../gen_server"),
      DTable = require("../dtable"),
      MTable = require("../mtable"),
      Semaphore = require("./semaphore"),
      utils = require("../utils"),
      consts = require("../consts");

const mcsToMs = 1000;
const serverDefaults = consts.dsemOpts;
const semPrefix = "SEMAPHORES::";
const dataPrefix = "SEMAPHORE_HOLDERS::";

const createTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."},
  {key: "n", valid: [utils.isPositiveNumber], str: "Must be a positive JSON number."}
];

const readTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."}
];

const destroyTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."}
];

const postTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."},
  {key: "holder", valid: [util.isString], str: "Must be a JSON string."},
  {key: "timeout", valid: [utils.isPositiveNumber], str: "Must be a JSON number."}
];

const closeTypes = [
  {key: "id", valid: [util.isString], str: "Must be a JSON string."},
  {key: "holder", valid: [util.isString], str: "Must be a JSON string."}
];

const parseTypes = {
  "create": createTypes,
  "read": readTypes,
  "destroy": destroyTypes,
  "post": postTypes,
  "close": closeTypes
};

class DSMServer extends GenServer {
  /**
   *
   * Distributed semaphore-manager server. Semaphores are created/read/destroyed, as well as posted/closed (grabbing the semaphore/releasing the sempahore). Unlike the DLM, which uses a quorum to determine success of locking, this routes all requests against any semaphore requests to the singular node that 'owns' the semaphore, determined by the hash ring in the hash ring of `gossip`.
   *
   * @class DSMServer
   * @memberof Clusterluck
   *
   * @param {Clusterluck.GossipRing} gossip - Gossip ring to coordinate ring state from.
   * @param {Clusterluck.NetKernel} kernel - Network kernel to communicate with other nodes.
   * @param {Object} [opts] Options object containing information about disk persistence options, and wait times for retry logic on lock requests.
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
    this._semaphores = new Map();
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
   * Starts a DSM handler: listenes for events related to the creation/reading/destruction of semaphores, as well as grabbing/releasing semaphores. Also starts the underlying table storing semaphores and semaphore holders.
   *
   * @method start
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} [name] - Name to register this handler with instead of the unique id attached. Any message received on the network kernwl with id `name` will be routed to this instance for message stream parsing, and possible event firing.
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  start(name) {
    super.start(name);
    this._stopped = false;
    
    const jobs = [
      {event: "create", method: "_doCreate"},
      {event: "read", method: "_doRead"},
      {event: "destroy", method: "_doDestroy"},
      {event: "post", method: "_doPost"},
      {event: "close", method: "_doClose"}
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
    this._table.on("idle", handler.bind(this));
    this.once("stop", _.partial(this._table.removeListener, "idle", handler).bind(this._table));
    return this;
  }

  /**
   *
   * Stops this handler. If the table is idle, this function will transition into clearing all locks and table state, and stopping the underlying table. Otherwise, this function will wait to complete until this instance is in an idle state.
   *
   * @method stop
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  stop() {
    if (this.idle()) {
      debug("Stopping DSem server %s.", this._id);
      this._semaphores.forEach((sem) => {
        sem.timeouts().forEach((t) => {
          clearTimeout(t);
        });
      });
      this._semaphores.clear();
      this._table.clear();
      this._stopLogging();
      return this;
    }
    debug("Waiting for DSem server %s to become idle before stopping...", this._id);
    this.once("idle", _.partial(this.stop).bind(this));
    return this;
  }

  /**
   *
   * Loads state from disk for the underlying table this instance uses for state persistence. If the `disk` option is set to false on construction, this function will immediately return and call `cb`. NOTE: this function should be called after `start` is called, as the underlying table needs to be started before any files can be read from disk.
   *
   * @method load
   * @memberof Clusterluck.DSMServer
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
        let sem;
        if (typeof val === "number") {
          key = key.slice(semPrefix.length);
          sem = this._semaphores.get(key) || new Semaphore(key, val, new Map());
          sem.size(val);
        } else {
          key = key.slice(dataPrefix.length);
          sem = this._semaphores.get(key) || new Semaphore(key, 0, new Map());
          val.forEach((innerV, innerK) => {
            const nTime = Math.max(0, innerV.timeout+innerV.start-Date.now());
            const timeout = setTimeout(_.partial(this._clearSem, key, innerK).bind(this), nTime);
            sem.timeouts().set(innerK, timeout);
          });
        }
        this._semaphores.set(key, sem);
        async.nextTick(next);
      }, cb);
    });
  }

  /**
   *
   * Returns whether this instance is idle or not. Checks for both active requests as well as the underlying table's state for idleness.
   *
   * @method idle
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @return {Boolean}
   *
   */
  idle() {
    return super.idle() && this._table.idle();
  }

  /**
   *
   * Creates a semaphore with id `id` and concurrency limit `n`. The algorithm consists of:
   *
   *   - Use the internal gossip server to find the owning node responsible for `id` on the hash ring.
   *   - Make a request to the DSM server on that node to create the semaphore.
   *   - If no error is returned, the request is considered successful and we call `cb` with no arguments.
   *   - Otherwise, we return the error in `cb`.
   *
   * @method create
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} id - ID of semaphore to create.
   * @param {Number} n - Concurrency limit of semaphore.
   * @param {Function} cb - Function of form `function (err) {...}`. `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a response before considering the request errored. Defauls to `Infinity`.
   *
   */
  create(id, n, cb, reqTimeout=Infinity) {
    const node = this._gossip.ring().find(id);
    debug("Creating semaphore %s on %s with concurrency of %i...", id, this._id, n);
    this.call({node: node, id: this._id}, "create", {
      id: id,
      n: n
    }, (err, data) => {
      if (err) return cb(err);
      return cb();
    }, reqTimeout);
  }

  /**
   *
   * Grabs the current state of sempahore `id`, which consists of the concurrency limit and the number of active holders of the semaphore. The algorithm consists of:
   *
   *   - Use the internal gossip server to find the owning node responsible for `id` on the hash ring.
   *   - Make a request to the DSM server on that node to read metadata about the semaphore.
   *   - If no error is returned and the response is valid JSON, the request is considered successful and we call `cb` with a null error and the metadata returned in the response.
   *   - Otherwise, we return the error in `cb`. 
   *   
   *
   * @method read
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} id - ID of semaphore to read.
   * @param {Function} cb - Function of form `function (err, data) {...}`, where `data` is a JSON object of the form `{n: <concurrency limit>, active: <number of active holders>}`. `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a response before considering the request errored. Defaults to `Infinity`.
   *
   */
  read(id, cb, reqTimeout=Infinity) {
    const node = this._gossip.ring().find(id);
    debug("Reading semaphore %s on %s...", id, this._id);
    this.call({node: node, id: this._id}, "read", {
      id: id
    }, (err, data) => {
      if (err) return cb(err);
      return cb(null, data.data);
    }, reqTimeout);
  }

  /**
   *
   * Destroys sempahore `id`, along with all active holders The algorithm consists of:
   *
   *   - Use the internal gossip server to find the owning node responsible for `id` on the hash ring.
   *   - Make a request to the DSM server on that node to destroy the semaphore.
   *   - If no error is returned, the request is considered successful and we call `cb` with a null error.
   *   - Otherwise, we return the error in `cb`. 
   *
   * @method destroy
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} id - ID of semaphore to destroy.
   * @param {Function} cb - Function of form `function (err) {...}`. `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a response before considering the request errored. Defauls to `Infinity`.
   *
   */
  destroy(id, cb, reqTimeout=Infinity) {
    const node = this._gossip.ring().find(id);
    debug("Destroying semaphore %s on %s", id, this._id);
    this.call({node: node, id: this._id}, "destroy", {
      id: id
    }, (err, data) => {
      if (err) return cb(err);
      return cb();
    }, reqTimeout);
  }

  /**
   *
   * Makes a post request against semaphore `id` with `holder` identifying the requester of this semaphore (think actor). `holder` should be a randomly generated string if looking for different requests to represent different actions, such as a UUID or the result of a `crypto.randomBytes` call. The post will last for `timeout` milliseconds before being automatically released on the node this semaphore routes to. The algorithm consists of:
   *
   *   - Use the internal gossip server to find the node responsible for `id` on the hash ring.
   *   - Make a request to the DSM server on that node to execute the post command.
   *   - Based on the response, if an 'ok' response is returned within `timeout` milliseconds, then the request was successful and we return a null error.
   *   - Otherwise, we asynchronously close this semaphore on the routed node and set a random timeout to retry the request. If we've retried `retries` number of times, then an error is returned and retry logic ceases.
   *
   * @method post
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} id - ID of semaphore to post.
   * @param {String} holder - ID of actor/requester for this request.
   * @param {Number} timeout - How long the semaphore observance will last on the node holding this semaphore, in milliseconds.
   * @param {Function} cb - Function of form `function (err) {...}`. `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a post attempt before considering the request errored. Defaults to `Infinity`.
   * @param {Number} [retries] - Number of times to retry this request. Defaults to `Infinity`.
   *
   */
  post(id, holder, timeout, cb, reqTimeout=Infinity, retries=Infinity) {
    const node = this._gossip.ring().find(id);
    const time = microtime.now();
    debug("Grabbing semaphore %s on %s with holder %s...", id, this._id, holder);
    this.call({node: node, id: this._id}, "post", {
      id: id,
      holder: holder,
      timeout: timeout
    }, (err, data) => {
      if (err) {
        debug("Error making post request on %s against %s, error: %s", this._id, id, err.message);
        return cb(err);
      }
      const delta = (microtime.now()-time)/mcsToMs;
      if (delta < timeout && data.ok === true) {
        debug("post request on %s against %s succeeded.", this._id, id);
        return cb();
      } else {
        this.closeAsync(id, holder);
        if (--retries < 0) {
          debug("Failed to grab semahore on %s for %s after exhausting retry count.", this._id, id);
          const str = util.format("Failed to grab semaphore %s with holder %s.", id, holder);
          return cb(new Error(str));
        }
        const wtime = DSMServer.calculateWaitTime(this._minWaitTimeout, this._maxWaitTimeout);
        debug("post request on %s against %s failed, retrying after %i milliseconds...", this._id, id, wtime);
        setTimeout(_.partial(this.post, id, holder, timeout, cb, reqTimeout, retries).bind(this), wtime);
      }
    }, reqTimeout);
  }

  /**
   *
   * Closes the semaphore `id` with holder `holder`. If the request takes longer than `reqTimeout`, `cb` is called with a timeout error. Otherwise, `cb` is called with no arguments. The algorithm consists of:
   *   - Use the internal gossip server to find the node responsible for `id` on the hash ring.
   *   - Make a request to the DSM server on that node to execute the close command.
   *   - If an error is returned, call `cb` with that error.
   *   - Otherwise, call `cb` with no arguments.
   *
   * @method close
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} id - ID of semaphore to clsoe.
   * @param {String} holder - ID of actor/requester for this semaphore.
   * @param {Function} cb - Function of form `function (err) {...}`, where `err` is null if the request is successful, or an Error object otherwise.
   * @param {Number} [reqTimeout] - Amount of time, in milliseconds, to wait for a clsoe attempt before considering the request errored. Defaults to `Infinity`.
   *
   */
  close(id, holder, cb, reqTimeout=Infinity) {
    const node = this._gossip.ring().find(id);
    debug("Dropping semaphore %s on %s with holder %s...", id, this._id, holder);
    this.call({node: node, id: this._id}, "close", {
      id: id,
      holder: holder
    }, (err, res) => {
      if (err) return cb(err);
      return cb();
    }, reqTimeout);
  }

  /**
   *
   * Asynchronously closes the semaphore `id` with holder `holder`. The algorithm consists of:
   *   - Use the internal gossip server to find the node responsible for `id` on the hash ring.
   *   - Make an asynchronous request to the DSM server on that node to execute the close command.
   *
   * @method closeAsync
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {String} id
   * @param {String} holder
   *
   */
  closeAsync(id, holder) {
    const node = this._gossip.ring().find(id);
    debug("Asynchronously dropping semaphore %s on %s with holder %s...", id, this._id, holder);
    this.cast({node: node, id: this._id}, "close", {
      id: id,
      holder: holder
    });
  }

  /**
   *
   * Parses a fully memoized message stream into an object containing a key/value pair. If we fail to parse the job buffer (invalid JSON, etc), we just return an error and this GenServer will skip emitting an event. Otherwise, triggers user-defined logic for the parsed event.
   *
   * @method decodeJob
   * @memberof Clusterluck.DSMServer
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
    let data = DSMServer.parseJob(out.data, out.event);
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
   * @memberof Clusterluck.DSMServer
   * @instance
   *
   * @param {Object} data - Message to be processed with `event` and `data` parameters.
   *
   * @return {Object|Error} Object containing an event and data key/value pair, which are used to emit an event for user-defined logic.
   *
   */
  decodeSingleton(data) {
    data = super.decodeSingleton(data);
    const nData = DSMServer.parseJob(data.data, data.event);
    if (nData instanceof Error) return nData;
    data.data = nData;
    return data;
  }

  /**
   *
   * @method _doCreate
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  _doCreate(data, from) {
    let sem = this._semaphores.get(data.id);
    if (sem) {
      if (sem.size() !== data.n) {
        const str = util.format("Semaphore '%s' already exist with n equal to %i.", data.id, sem.size());
        const err = new Error(str);
        return this._errorReply(from, err);
      } else {
        return this.reply(from, {ok: true});
      }
    }
    sem = new Semaphore(data.id, data.n, new Map());
    this._semaphores.set(data.id, sem);
    this._writeToLog("set", semPrefix + data.id, data.n);
    return this.reply(from, {ok: true});
  }

  /**
   *
   * @method _doRead
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  _doRead(data, from) {
    const sem = this._semaphores.get(data.id);
    if (!sem) {
      const str = util.format("Semaphore '%s' does not exist.", data.id);
      const err = new Error(str);
      return this._errorReply(from, err);
    }
    return this.reply(from, {
      ok: true,
      data: {n: sem.size(), active: sem.timeouts().size}
    });
  }

  /**
   *
   * @method _doDestroy
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  _doDestroy(data, from) {
    const sem = this._semaphores.get(data.id);
    if (!sem) {
      return this.reply(from, {ok: true});
    }
    const timeouts = sem.timeouts();
    timeouts.forEach((timeout) => {
      clearTimeout(timeout);
    });
    this._semaphores.delete(data.id);
    this._writeToLog("del", semPrefix + data.id);
    return this.reply(from, {ok: true});
  }

  /**
   *
   * @method _doPost
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  _doPost(data, from) {
    const sem = this._semaphores.get(data.id);
    if (!sem) {
      const str = util.format("Semaphore '%s' does not exist.", data.id);
      const err = new Error(str);
      return this._errorReply(from, err);
    } else if (sem.timeouts().has(data.holder)) {
      return this.reply(from, {ok: true});
    } else if (sem.timeouts().size === sem.size()) {
      return this.reply(from, {ok: false});
    }
    const timeout = setTimeout(_.partial(this._clearSem, data.id, data.holder).bind(this), data.timeout);
    const created = Date.now();
    sem.timeouts().set(data.holder, timeout);
    this._semaphores.set(data.id, sem);
    this._writeToLog("hset", dataPrefix + data.id, data.holder, {start: created, timeout: data.timeout});
    return this.reply(from, {ok: true});
  }

  /**
   *
   * @method _doClose
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   * @return {Clusterluck.DSMServer} This instance.
   *
   */
  _doClose(data, from) {
    const sem = this._semaphores.get(data.id);
    if (!sem) {
      const str = util.format("Semaphore '%s' does not exist.", data.id);
      const err = new Error(str);
      return this._errorReply(from, err);
    }

    const timeouts = sem.timeouts();
    if (timeouts.has(data.holder)) {
      clearTimeout(timeouts.get(data.holder));
      timeouts.delete(data.holder);

      this._writeToLog("hdel", dataPrefix + data.id, data.holder);
      return this._safeReply(from, {ok: true});
    } else {
      return this._safeReply(from, {ok: false});
    }
  }

  /**
   *
   * @method _clearSem
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {String} id
   * @param {String} holder
   *
   */
  _clearSem(id, holder) {
    const sem = this._semaphores.get(id);
    if (!sem) {
      return;
    }
    const timeouts = sem.timeouts();
    if (timeouts.has(holder)) {
      timeouts.delete(holder);
      this._writeToLog("hdel", dataPrefix + id, holder);
    }
  }

  /**
   *
   * @method _writeToLog
   * @memberof Clusterluck.DSMServer
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
   * @memberof Clusterluck.DSMServer
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
   * @method _errorReply
   * @memberof Clusterluck.DSMServer
   * @instance
   * @private
   *
   * @param {Object} from
   * @param {Error} error
   *
   * @return {Clusterluck.DSMServer}
   *
   */
  _errorReply(from, err) {
    const rstream = new stream.PassThrough();
    this._safeReply(from, rstream);
    rstream.emit("error", err);
    rstream.resume();
    rstream.end();
    return rstream;
  }

  /**
   *
   * Parse and validate `job` for correct structure and type adherence.
   *
   * @method parseJob
   * @memberof Clusterluck.DSMServer
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
      if (!valid) {
        memo[check.key] = check.str;
      }
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
   * Calculates wait time for retry functionality in post requests.
   *
   * @method calculateWaitTime
   * @memberof Clusterluck.DSMServer
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

module.exports = DSMServer;

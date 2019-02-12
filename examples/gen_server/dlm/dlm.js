var _ = require("lodash"),
    microtime = require("microtime"),
    cl = require("../../../index"),
    util = require("util"),
    debug = require("debug")("notp:examples:dlm");

var GenServer = cl.GenServer;
const mcsToMs = 1000;

class Lock {
  constructor(type, id, holder, timeout) {
    this._type = type;
    this._id = id;
    this._holder = holder;
    this._timeout = timeout;
  }

  type(type) {
    if (type !== undefined) {
      this._type = type;
    }
    return this._type;
  }

  id(id) {
    if (id !== undefined) {
      this._id = id;
    }
    return this._id;
  }

  holder(holder) {
    if (holder !== undefined) {
      this._holder = holder;
    }
    return this._holder;
  }

  timeout(timeout) {
    if (timeout !== undefined) {
      this._timeout = timeout;
    }
    return this._timeout;
  }
}

class DLMServer extends GenServer {
  /**
   *
   * @class DLMServer
   * @memberof Clusterluck
   *
   * @param {Clusterluck.GossipRing} gossip
   * @param {Clusterluck.NetKernel} kernel
   * @param {Object} [opts]
   * @param {Number} [opts.rquorum]
   * @param {Number} [opts.wquorum]
   *
   */
  constructor(gossip, kernel, opts = {rquorum: 0.51, wquorum: 0.51}) {
    super(kernel);
    this._gossip = gossip;
    this._kernel = kernel;
    this._locks = new Map();
    this._rquorum = opts.rquorum;
    this._wquorum = opts.wquorum;
  }

  /**
   *
   * @method start
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} [name]
   *
   * @return {Clusterluck.DLMServer}
   *
   */
  start(name) {
    super.start(name);
    
    var jobs = [
      {event: "rlock", method: "doRLock"},
      {event: "wlock", method: "doWLock"},
      {event: "runlock", method: "doRUnlock"},
      {event: "wunlock", method: "doWUnlock"}
    ];
    jobs.forEach((job) => {
      var handler = this[job.method].bind(this);
      this.on(job.event, handler);
      this.once("stop", _.partial(this.removeListener, job.event, handler).bind(this));
    });
    return this;
  }

  /**
   *
   * @method stop
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Boolean} [force]
   *
   * @return {Clusterluck.DLMServer}
   *
   */
  stop(force = false) {
    if (this.idle() || force === true) {
      this._locks.clear();
      super.stop();
      return this;
    }
    this.once("idle", _.partial(this.stop, force).bind(this));
    return this;
  }

  /**
   *
   * @method rlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id
   * @param {String} holder
   * @param {Number} timeout
   * @param {Function} cb
   * @param {Number} [reqTimeout]
   *
   */
  rlock(id, holder, timeout, cb, reqTimeout=Infinity) {
    var nodes = this._gossip.find(id);
    var time = microtime.now();
    this.multicall(nodes, this._id, "rlock", {
      id: id,
      holder: holder,
      timeout: timeout
    }, (err, data) => {
      if (err) return cb(err);
      var delta = (microtime.now()-time)/mcsToMs;
      var nData = DLMServer.findLockPasses(nodes, data);
      if (nData.length/data.length >= this._rquorum && delta < timeout) {
        return cb(null, nData);
      } else {
        this.runlockAsync(nData, id, holder);
        return cb(new Error("Failed to achieve rlock quorum."));
      }
    }, reqTimeout);
  }

  /**
   *
   * @method wlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {String} id
   * @param {String} holderr
   * @param {Number} timeout
   * @param {Function} cb
   * @param {Number} [reqTimeout]
   *
   */
  wlock(id, holder, timeout, cb, reqTimeout=Infinity) {
    var nodes = this._gossip.find(id);
    var time = microtime.now();
    this.multicall(nodes, this._id, "wlock", {
      id: id,
      holder: holder,
      timeout: timeout
    }, (err, data) => {
      if (err) return cb(err);
      var delta = (microtime.now()-time)/mcsToMs;
      var nData = DLMServer.findLockPasses(nodes, data);
      if (nData.length/data.length >= this._wquorum && delta < timeout) {
        return cb(null, nData);
      } else {
        this.wunlockAsync(nData, id, holder);
        return cb(new Error("Failed to achieve wlock quorum."));
      }
    }, reqTimeout);
  }

  /**
   *
   * @method runlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Array} nodes
   * @param {String} id
   * @param {String} holder
   * @param {Function} cb
   * @param {Number} [reqTimeout]
   *
   */
  runlock(nodes, id, holder, cb, reqTimeout=Infinity) {
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
   * @method runlockAsync
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Array} nodes
   * @param {String} id
   * @param {String} holder
   *
   */
  runlockAsync(nodes, id, holder) {
    this.abcast(nodes, this._id, "runlock", {
      id: id,
      holder: holder
    });
  }

  /**
   *
   * @method wunlock
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Array} nodes
   * @param {String} id
   * @param {String} holder
   * @param {Function} cb
   * @param {Number} [reqTimeout]
   *
   */
  wunlock(nodes, id, holder, cb, reqTimeout=Infinity) {
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
   * @method wunlockAsync
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Array} nodes
   * @param {String} id
   * @param {String} holder
   *
   */
  wunlockAsync(nodes, id, holder) {
    this.abcast(nodes, this._id, "wunlock", {
      id: id,
      holder: holder
    });
  }

  /**
   *
   * @method decodeJob
   * @memberof Clusterluck.DLMServer
   * @instance
   *
   * @param {Buffer} buf
   *
   * @return {Object|Error}
   *
   */
  decodeJob(buf) {
    var out = super.decodeJob(buf);
    if (out instanceof Error) return out;
    var data = out.data;
    if (out.event.endsWith("unlock")) {
      data = DLMServer.parseUnlockJob(data);
    } else {
      data = DLMServer.parseLockJob(data);
    }
    if (data instanceof Error) {
      return data;
    }
    out.data = data;
    return out;
  }

  /**
   *
   * @method doRLock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   */
  doRLock(data, from) {
    var lock = this._locks.get(data.id);
    if (lock && lock.type() === "write") {
      return this.reply(from, DLMServer.encodeResp({ok: false}));
    } else if (lock && lock.holder().has(data.holder)) {
      return this.reply(from, DLMServer.encodeResp({ok: true}));
    }
    var timeout = setTimeout(() => {
      var lock = this._locks.get(data.id);
      if (!lock || lock.type() === "write") {
        return;
      }
      var holder = lock.holder();
      holder.delete(data.holder);
      if (holder.size === 0) {
        this._locks.delete(data.id);
      }
    }, data.timeout);
    if (!lock) {
      lock = new Lock("read", data.id, new Set(), new Map());
    }
    lock.holder().add(data.holder);
    lock.timeout().set(data.holder, timeout);
    this._locks.set(data.id, lock);
    this.reply(from, DLMServer.encodeResp({ok: true}));
  }

  /**
   *
   * @method doWLock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   */
  doWLock(data, from) {
    if (this._locks.has(data.id)) {
      return this.reply(from, DLMServer.encodeResp({ok: false}));
    }
    var timeout = setTimeout(() => {
      var lock = this._locks.get(data.id);
      if (!lock || lock.holder() !== data.holder) {
        return;
      }
      this._locks.delete(data.id);
    }, data.timeout);
    this._locks.set(data.id, new Lock("write", data.id, data.holder, timeout));
    this.reply(from, DLMServer.encodeResp({ok: true}));
  }

  /**
   *
   * @method doRUnlock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   */
  doRUnlock(data, from) {
    var lock = this._locks.get(data.id);
    if (!lock || lock.type() !== "read") {
      return this._safeReply(from, DLMServer.encodeResp({ok: false}));
    }

    var holders = lock.holder();
    var timeouts = lock.timeout();
    holders.delete(data.holder);
    clearTimeout(timeouts.get(data.holder));
    timeouts.delete(data.holder);

    if (holders.size === 0) {
      this._locks.delete(data.id);
    }
    this._safeReply(from, DLMServer.encodeResp({ok: true}));
  }

  /**
   *
   * @method doWUnlock
   * @memberof Clusterluck.DLMServer
   * @instance
   * @private
   *
   * @param {Object} data
   * @param {Object} from
   *
   */
  doWUnlock(data, from) {
    var lock = this._locks.get(data.id);
    if (!lock || lock.type() !== "write") {
      return this._safeReply(from, DLMServer.encodeResp({ok: false}));
    }

    var holder = lock.holder();
    if (holder === data.holder) {
      clearTimeout(lock.timeout());
      this._locks.delete(data.id);
    }
    this._safeReply(from, DLMServer.encodeResp({ok: true}));
  }

  /**
   *
   * @method parseLockJob
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Object} job
   *
   * @return {Object|Error}
   *
   */
  static parseLockJob(job) {
    if (!(util.isObject(job) &&
      util.isString(job.id) &&
      util.isString(job.holder) &&
      util.isNumber(job.timeout))) {
      return new Error("Malformed lock job.");
    }
    return job;
  }

  /**
   *
   * @method parseUnlockJob
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Object} job
   *
   * @return {Object|Error}
   *
   */
  static parseUnlockJob(job) {
    if (!(util.isObject(job) &&
      util.isString(job.id) &&
      util.isString(job.holder))) {
      return new Error("Malformed unlock job.");
    }
    return job;
  }

  /**
   *
   * @method encodeResp
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Any} res
   *
   * @return {String}
   *
   */
  static encodeResp(res) {
    return JSON.stringify(res);
  }

  /**
   *
   * @method findLockPasses
   * @memberof Clusterluck.DLMServer
   * @static
   *
   * @param {Array} nodes
   * @param {Array} data
   *
   * @return {Array}
   *
   */
  static findLockPasses(nodes, data) {
    return data.reduce((memo, val, idx) => {
      val = JSON.parse(val);
      if (util.isObject(val) && val.ok === true) {
        memo.push(nodes[idx]);
      }
      return memo;
    }, []);
  }
}

module.exports = DLMServer;

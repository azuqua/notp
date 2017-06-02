const _ = require("lodash"),
      fs = require("fs"),
      util = require("util"),
      debug = require("debug")("clusterluck:lib:dtable"),
      async = require("async"),
      path = require("path"),
      shortid = require("shortid"),
      readline = require("readline"),
      EventEmitter = require("events").EventEmitter;

const utils = require("./utils"),
      Queue = require("./queue"),
      consts = require("./consts");

const defaults = consts.dtableOpts;

class DTable extends EventEmitter {
  /**
   *
   * @class DTable
   * @memberof Clusterluck
   *
   * @param {Object} opts
   * @param {String} opts.path
   * @param {Number} [opts.writeThreshold]
   * @param {Number} [opts.autoSave]
   * @param {Number} [opts.fsyncInterval]
   *
   */
  constructor(opts = defaults) {
    super();
    opts = _.defaults(_.clone(opts), defaults);
    if (!util.isString(opts.path)) {
      throw new Error("Missing 'path' option in options object.");
    }
    this._path = path.join(opts.path, "DATA.SNAP");
    this._aofPath = path.join(opts.path, "LATEST.LOG");
    this._tmpAOFPath = path.join(opts.path, "PREV.LOG");
    this._autoSave = opts.autoSave;
    this._writeCount = 0;
    this._writeThreshold = opts.writeThreshold;
    this._table = new Map();
    this._idleTicks = 0;
    this._idleTickInterval = 1000;
    this._idleTickMax = this._autoSave/this._idleTickInterval;
    this._fsyncInterval = opts.fsyncInterval;
    this._queue = new Queue();
    this._flushing = false;
  }

  /**
   *
   * @method start
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} name
   *
   * @return {Clusterluck.DTable} This instance.
   *
   */
  start(name) {
    this._name = name;
    this._id = shortid.generate();
    this._setupDiskOps();
    return this;
  }

  /**
   *
   * @method stop
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb
   *
   */
  stop(cb) {
    if (this.idle() && this._fd) {
      this.emit("stop");
      this._id = null;
      return cb();
    } else if (this.idle() && !this._fd) {
      return this.once("open", _.partial(this.stop, cb).bind(this));
    }
    this.once("idle", _.partial(this.stop, cb).bind(this));
  }

  /**
   *
   * @method load
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb
   *
   */
  load(cb) {
    async.series([
      _.partial(this._loadState).bind(this),
      _.partial(this._loadAOF, this._tmpAOFPath).bind(this),
      _.partial(this._loadAOF, this._aofPath).bind(this)
    ], cb);
  }

  /**
   *
   * @method idle
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @return {Boolean}
   *
   */
  idle() {
    return this._flushing === false;
  }

  /**
   *
   * @method get
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   *
   * @return {Any}
   *
   */
  get(key) {
    return this._table.get(key);
  }

  /**
   *
   * @method smember
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {String} val
   *
   * @return {Boolean}
   *
   */
  smember(key, val) {
    const out = this._table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw DTable.invalidTypeError("smember", key, typeof out);
    }
    return out.has(val);
  }

  /**
   *
   * @method hget
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {String} hkey
   *
   * @return {JSON}
   *
   */
  hget(key, hkey) {
    var out = this._table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw new DTable.invalidTypeError("hget", key, typeof out);
    }
    return out.get(hkey);
  }

  /**
   *
   * @method set
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {Any} val 
   *
   * @return {Any}
   *
   */
  set(key, val) {
    this._table.set(key, val);
    this._writeToLog("set", key, val);
    this._updateWriteCount();
    return val;
  }

  /**
   *
   * @method sset
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {String} val
   *
   * @return {String}
   *
   */
  sset(key, val) {
    const out = this._table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw DTable.invalidTypeError("sset", key, typeof out);
    }
    out.add(val);
    this._table.set(key, out);
    this._writeToLog("sset", key, val);
    this._updateWriteCount();
    return out;
  }

  /**
   *
   * @method hset
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {String} hkey
   * @param {JSON} val
   *
   * @return {JSON}
   *
   */
  hset(key, hkey, val) {
    const out = this._table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw DTable.invalidTypeError("hset", key, typeof out);
    }
    out.set(hkey, val);
    this._table.set(key, out);
    this._writeToLog("hset", key, hkey, val);
    this._updateWriteCount();
    return out;
  }

  /**
   *
   * @method del
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   *
   * @return {Clusterluck.DTable}
   *
   */
  del(key) {
    this._table.delete(key);
    this._writeToLog("del", key);
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * @method sdel
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {String} val
   *
   * @return {Clusterluck.DTable}
   *
   */
  sdel(key, val) {
    const out = this._table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw DTable.invalidTypeError("sdel", key, typeof out);
    }
    out.delete(val);
    if (out.size === 0) this._table.delete(key);
    this._writeToLog("sdel", key, val);
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * @method hdel
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {String} hkey
   *
   * @return {Clusterluck.DTable}
   *
   */
  hdel(key, hkey) {
    const out = this._table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw DTable.invalidTypeError("hdel", key, typeof out);
    }
    out.delete(hkey);
    if (out.size === 0) this._table.delete(key);
    this._writeToLog("hdel", key, hkey);
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * @method forEach
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb
   * @param {Function} fin
   *
   */
  forEach(cb, fin) {
    const entries = this._table.entries();
    let done = false;
    async.whilst(() => {
      return done !== true;
    }, (next) => {
      const val = entries.next();
      if (val.done === true) {
        done = true;
        return next();
      }
      cb(val.value[0], val.value[1], next);
    }, fin);
  }

  /**
   *
   * @method forEachSync
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb
   *
   * @return {Clusterluck.DTable}
   *
   */
  forEachSync(cb) {
    this._table.forEach((val, key) => {
      cb(key, val);
    });
    return this;
  }

  /**
   * 
   * @method _setupDiskOps
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @return {Clusterluck.DTable}
   *
   */
  _setupDiskOps() {
    this._setupIdleFlushInterval();
    this._setupAOFSyncInterval();

    this.once("stop", () => {
      this._fd = null;
      this._fstream.end();
      this._fstream = null;
      clearInterval(this._idleInterval);
      this._idleInterval = null;
      clearInterval(this._syncInterval);
      this._syncInterval = null;
    });
    return this;
  }

  /**
   *
   * @method _setupIdleFlushInterval
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @return {Clusterluck.DTable}
   *
   */
  _setupIdleFlushInterval() {
    this._idleInterval = setInterval(() => {
      this._idleTicks++;
      if (this._idleTicks >= this._idleTickMax) {
        this._idleTicks = 0;
        this._flush();
      }
    }, this._idleTickInterval);
    return this;
  }

  /**
   *
   * @method _setupAOFSyncInterval
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @return {Clusterluck.DTable}
   *
   */
  _setupAOFSyncInterval() {
    this._fstream = fs.createWriteStream(this._aofPath, {
      flags: "a"
    });
    this._fstream.once("open", (fd) => {
      this._fd = fd;
      this._queue.flush().forEach((el) => {
        const args = [el.op].concat(el.args);
        this._writeToLog.apply(this, args);
      });
      this.emit("open");
    });
    this._syncInterval = setInterval(() => {
      if (!this._fd) return;
      fs.fsync(this._fd, (err) => {
        if (err) debug("failed to fsync underlying file descriptor for AOF file on table %s.", this._name);
      });
    }, this._fsyncInterval);
    return this;
  }

  /**
   *
   * @method _updateWriteCount
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @return {Clusterluck.DTable}
   *
   */
  _updateWriteCount() {
    this._idleTicks = 0;
    this._writeCount++;
    if (this._writeCount >= this._writeThreshold) {
      this._writeCount = 0;
      this._flush();
    }
    return this;
  }

  /**
   *
   * @method _flush
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   */
  _flush() {
    if (this._flushing || !this._fd) return;
    this._flushing = true;
    clearInterval(this._syncInterval);

    var obj = utils.mapToObject(this._table);
    this._fstream.once("close", () => {
      async.series([
        _.partial(this._flushAOF).bind(this),
        (next) => {
          this._setupAOFSyncInterval();
          async.nextTick(next);
        },
        _.partial(this._flushTable, obj).bind(this),
        _.partial(fs.unlink, this._tmpAOFPath)
      ], (err) => {
        this._flushing = false;
        this.emit("idle");
      });
    });
    this._fstream.end();
    this._fd = null;
    this.emit("close");
  }

  /**
   *
   * @method _flushAOF
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @param {Function} cb
   *
   */
  _flushAOF(cb) {
    let called = false;
    const wstream = fs.createWriteStream(this._tmpAOFPath, {
      flags: "a"
    });
    const rstream = fs.createReadStream(this._aofPath);
    rstream.pipe(wstream);
    wstream.on("close", () => {
      if (called) return;
      called = true;
      fs.unlink(this._aofPath, cb);
    });
    wstream.on("error", (err) => {
      if (called) return;
      called = true;
      rstream.resume();
      return cb(err);
    });
  }

  /**
   *
   * @method _flushTable
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @param {Object} obj
   * @param {Function} cb
   *
   */
  _flushTable(obj, cb) {
    let called = false;
    const wstream = fs.createWriteStream(this._path);
    wstream.on("error", (err) => {
      if (called) return;
      called = true;
      wstream.end();
      return cb(err);
    });
    wstream.on("close", () => {
      if (called) return;
      called = true;
      return cb();
    });
    async.eachLimit(Object.keys(obj), 4, (key, next) => {
      if (called) return next("Closed prematurely.");
      const val = obj[key];
      wstream.write(JSON.stringify({
        key: key,
        value: DTable.encodeValue(val)
      }) + "\n");
      async.nextTick(next);
    }, (err) => {
      if (called) return;
      wstream.end();
    });
  }

  /**
   *
   * @method _writeToLog
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @param {String} op
   *
   * @return {Clusterluck.DTable}
   *
   */
  _writeToLog(op, ...args) {
    if (!this._fd) {
      this._queue.enqueue({op: op, args: args});
      return;
    }
    args = args.map((val) => {
      return DTable.encodeValue(val);
    });
    const out = JSON.stringify({
      op: op,
      args: args
    }) + "\n";
    this._fstream.write(out);
    return this;
  }

  /**
   *
   * @method _loadState
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @param {Function} cb
   *
   */
  _loadState(cb) {
    fs.stat(this._path, (err) => {
      if (err && err.code !== "ENOENT") return cb(err);
      else if (err && err.code === "ENOENT") return cb();
      const rstream = fs.createReadStream(this._path);
      const rline = readline.createInterface({
        input: rstream
      });
      let called = false;
      rline.once("close", () => {
        if (called) return;
        called = true;
        return cb();
      });
      rstream.on("error", (err) => {
        if (called) return;
        called = true;
        rstream.resume();
        rline.close();
        return cb(err);
      });
      rline.on("line", (line) => {
        const lineObj = JSON.parse(line);
        const val = DTable.decodeValue(lineObj.value);
        this._table.set(lineObj.key, val);
      });
    });
  }

  /**
   *
   * @method _loadAOF
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @param {String} path
   * @param {Function} cb
   *
   */
  _loadAOF(path, cb) {
    fs.stat(path, (err) => {
      if (err && err.code !== "ENOENT") return cb(err);
      else if (err && err.code === "ENOENT") return cb();
      const rstream = fs.createReadStream(path);
      const rline = readline.createInterface({
        input: rstream
      });
      let called = false;
      rline.once("close", () => {
        if (called) return;
        called = true;
        return cb();
      });
      rstream.on("error", (err) => {
        if (called) return;
        called = true;
        rstream.resume();
        rline.close();
        return cb(err);
      });
      rline.on("line", (line) => {
        const lineObj = JSON.parse(line);
        lineObj.args = lineObj.args.map((val) => {
          return DTable.decodeValue(val);
        });

        try {
          this._table[lineObj.op].apply(this, lineObj.args);
        } catch (e) {
          debug("failed to complete operation %s with args " + JSON.stringify(lineObj.args) + " from %s", lineObj.op, path);
        }
      });
    });
  }

  /**
   *
   * @method invalidTypeError
   * @memberof Clusterluck.DTable
   * @static
   *
   * @param {String} command
   * @param {String} key
   * @param {String} type
   *
   * @return {Error}
   *
   */
  static invalidTypeError(command, key, type) {
    let msg = "Invalid command '" + command + "' against key '" + key + "'";
    msg += " of type '" + type +"'";
    return _.extend(new Error(msg), {
      type: "INVALID_TYPE"
    });
  }

  /**
   *
   * @method encodeValue
   * @memberof Clusterluck.DTable
   * @static
   *
   * @param {Map|Set|JSON} value
   *
   * @return {Object}
   *
   */
  static encodeValue(value) {
    if (value instanceof Set) {
      return {
        type: "Set",
        data: utils.setToList(value)
      };
    } else if (value instanceof Map) {
      return {
        type: "Map",
        data: utils.mapToList(value)
      };
    } else {
      return {
        type: typeof value,
        data: value
      };
    }
  }

  /**
   *
   * @method decodeValue
   * @memberof Clusterluck.DTable
   * @static
   *
   * @param {Object} value
   * @param {String} value.type
   * @param {JSON} value.data
   *
   * @return {Map|Set|JSON}
   *
   */
  static decodeValue(value) {
    if (value.type === "Set") {
      return new Set(value.data);
    } else if (value.type === "Map") {
      return new Map(value.data);
    } else {
      return value.data;
    }
  }
}

module.exports = DTable;

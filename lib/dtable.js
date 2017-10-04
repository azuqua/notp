const _ = require("lodash"),
      fs = require("fs"),
      util = require("util"),
      debug = require("debug")("clusterluck:lib:dtable"),
      async = require("async"),
      path = require("path"),
      shortid = require("shortid"),
      readline = require("readline"),
      zlib = require("zlib"),
      EventEmitter = require("events").EventEmitter;

const utils = require("./utils"),
      Queue = require("./queue"),
      consts = require("./consts");

const defaults = consts.dtableOpts;

const ops = {
  set: (key, val, table) => {
    table.set(key, val);
    return val;
  },
  sset: (key, val, table) => {
    const out = table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw DTable.invalidTypeError("sset", key, typeof out);
    }
    out.add(val);
    table.set(key, out);
    return out;
  },
  hset: (key, hkey, val, table) => {
    const out = table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw DTable.invalidTypeError("hset", key, typeof out);
    }
    out.set(hkey, val);
    table.set(key, out);
    return out;
  },
  del: (key, table) => {
    table.delete(key);
  },
  sdel: (key, val, table) => {
    const out = table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw DTable.invalidTypeError("sdel", key, typeof out);
    }
    out.delete(val);
    if (out.size === 0) table.delete(key);
  },
  hdel: (key, hkey, table) => {
    const out = table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw DTable.invalidTypeError("hdel", key, typeof out);
    }
    out.delete(hkey);
    if (out.size === 0) table.delete(key);
  },
  clear: (table) => {
    table.clear();
  }
};

class DTable extends EventEmitter {
  /**
   *
   * In-memory key/value storage. Uses a combination of an AOF file and storage snapshot for persistence. For every "write" action, a corresponding write to the AOF file LATEST.LOG occurs. On an interval, this file is fsynced to disk (by default, 1000 milliseconds). In addition, a storage snapshot is kept and rewritten based on usage of the table. If the table remains inactive for a configurable amount of time (default 180000 milliseconds) or receives a configurable number of write operations before this idle time occurs (default 100 writes), the storage snapshot is resaved based on current state at that point in time. The algorithm is as follows:
   *   - Close the file descriptor on the AOF file LATEST.LOG
   *   - Append contents of LATEST.LOG (AOF file) to PREV.LOG (backup AOF file)
   *   - Delete LATEST.LOG and reopen with a new file descriptor
   *   - Dump state of table to DATA_PREV.SNAP (storage snapshot)
   *   - If succesful, move this to DATA.SNAP, else delete DATA_PREV.SNAP
   *   - Delete PREV.LOG
   *
   * In the time that the AOF file LATEST.LOG is closed, log writes are stored in an internal queue. Once the file descriptor is reopened, the queue is flushed.
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
    this._prefix = opts.path;
    
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
    this._compress = opts.compress !== undefined ? opts.compress : false;
    this._encodeFn = typeof opts.encodeFn === "function" ? opts.encodeFn : DTable.encodeValue;
    this._decodeFn = typeof opts.decodeFn === "function" ? opts.decodeFn : DTable.decodeValue;
  }

  /**
   *
   * Starts dtable instance, which triggers an fopen call to LATEST.LOG, the fsync interval for this log file, as well as other internal intervals to check for storage snapshot flush conditions.
   *
   * @method start
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} name - Name of table, meant for debugging purposes.
   *
   * @return {Clusterluck.DTable} This instance.
   *
   */
  start(name) {
    this._name = name;
    this._path = path.join(this._prefix, this._name + "_DATA.SNAP");
    this._tmpDumpPath = path.join(this._prefix, this._name + "_DATA_PREV.SNAP");
    this._aofPath = path.join(this._prefix, this._name + "_LATEST.LOG");
    this._tmpAOFPath = path.join(this._prefix, this._name + "_PREV.LOG");
    this._id = shortid.generate();
    this._setupDiskOps();
    return this;
  }

  /**
   *
   * Stops the table, including all internal disk-based logic. If the table is idle and has an open file descriptor against LATEST.LOG, it will close immediately. If it's idle but the file descriptor against LATEST.LOG has been closed, this call will wait for a file descriptor to open again before continuing. Otherwise, the table isn't idle and therefore we wait for this condition.
   *
   * @method stop
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb - Callback called once the table has been stopped.
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
   * Loads state from disk. The algorithm is as follows:
   *   - DATA.SNAP is loaded and read into the current state of table.
   *   - PREV.LOG is loaded and rerun against the current state.
   *   - LATEST.LOG is loaded and also rerun against the current state.
   *
   * @method load
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb - Callback called once the snapshot and AOF files have been loaded and rerun.
   *
   */
  load(cb) {
    async.series([
      _.partial(this._loadState, this._path).bind(this),
      _.partial(this._loadState, this._tmpDumpPath).bind(this),
      _.partial(this._loadAOF, this._tmpAOFPath).bind(this),
      _.partial(this._loadAOF, this._aofPath).bind(this)
    ], cb);
  }

  /**
   *
   * Returns whether this table is in an idle state or not.
   *
   * @method idle
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @return {Boolean} Whether this table is idle or not.
   *
   */
  idle() {
    return this._flushing === false;
  }

  /**
   *
   * Acts as a getter/setter for whether this table instance compresses the state snapshot.
   *
   * @method compress
   * @memberof Clusterluck.Dtable
   * @instance
   *
   * @param {Boolean} bool - Boolean to change the compression flag of this instance to.
   *
   * @return {Boolean} Whether this instance compresses it's state snapshots.
   *
   */
  compress(bool) {
    if (bool !== undefined) {
      this._compress = bool;
    }
    return this._compress;
  }

  /**
   *
   * Retrieves value stored at `key`, returning `undefined` if no such data exists.
   *
   * @method get
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key to retrieve data from.
   *
   * @return {Map|Set|JSON} Value stored at `key`.
   *
   */
  get(key) {
    return this._table.get(key);
  }

  /**
   *
   * Returns whether `val` is a member of the set stored at `key`.
   *
   * @method smember
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key to retrieve set from.
   * @param {String} val - Value to check existence of in the set.
   *
   * @return {Boolean} Whether `val` is a member of the set stored at `key`.
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
   * Retrieves value stored at hash key `hkey` under storage key `key`, returning `undefined` if no such data exists.
   *
   * @method hget
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key to retrieve hash map from.
   * @param {String} hkey - Hash key to retrieve data from.
   *
   * @return {JSON} - Value stored under hash key `hkey` at the hash map stored under `key`.
   *
   */
  hget(key, hkey) {
    const out = this._table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw new DTable.invalidTypeError("hget", key, typeof out);
    }
    return out.get(hkey);
  }

  /**
   *
   * Sets value `value` under key `key`.
   *
   * @method set
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key
   * @param {Map|Set|JSON} val 
   *
   * @return {Map|Set|JSON}
   *
   */
  set(key, val) {
    ops.set(key, val, this._table);
    this._writeToLog("set", key, val);
    this._updateWriteCount();
    return val;
  }

  /**
   *
   * Inserts `val` into the set stored at key `key`.
   *
   * @method sset
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key which holds the set to insert `val` under.
   * @param {String} val - Value to insert into the set.
   *
   * @return {String} The set stored at `key`.
   *
   */
  sset(key, val) {
    const out = ops.sset(key, val, this._table);
    this._writeToLog("sset", key, val);
    this._updateWriteCount();
    return out;
  }

  /**
   *
   * Sets `value` under the hash key `hkey` in the hash map stored at `key`.
   *
   * @method hset
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key which holds the hash map.
   * @param {String} hkey - Hash key to insert `val` under.
   * @param {JSON} val - Value to set under `hkey` in the hash map.
   *
   * @return {Map} The map stored at `key`.
   *
   */
  hset(key, hkey, val) {
    const out = ops.hset(key, hkey, val, this._table);
    this._writeToLog("hset", key, hkey, val);
    this._updateWriteCount();
    return out;
  }

  /**
   *
   * Removes key `key` from this table.
   *
   * @method del
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key to remove from this table.
   *
   * @return {Clusterluck.DTable} This instance.
   *
   */
  del(key) {
    ops.del(key, this._table);
    this._writeToLog("del", key);
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * Deletes `val` from the set stored under `key`.
   *
   * @method sdel
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key which olds the set to remove `val` from.
   * @param {String} val - Value to remove from the set.
   *
   * @return {Clusterluck.DTable} This instance.
   *
   */
  sdel(key, val) {
    ops.sdel(key, val, this._table);
    this._writeToLog("sdel", key, val);
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * Removes the hash key `hkey` from the hash map stored under `key`.
   *
   * @method hdel
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {String} key - Key which holds the hash map that `hkey` will be removed from.
   * @param {String} hkey - The hash key to remove from the hash map.
   *
   * @return {Clusterluck.DTable} This instance.
   *
   */
  hdel(key, hkey) {
    ops.hdel(key, hkey, this._table);
    this._writeToLog("hdel", key, hkey);
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * Clears the contents of this table.
   *
   * @method clear
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @return {Clusterluck.DTable} This isntance.
   *
   */
  clear() {
    ops.clear(this._table);
    this._writeToLog("clear");
    this._updateWriteCount();
    return this;
  }

  /**
   *
   * Asynchronously iterates over each key/value pair stored in this table at the point of this call.
   *
   * @method forEach
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb - Function to call on each key/value pair. Has the signature `function (key, val, next) {...}`.
   * @param {Function} fin -Finishing callback to call once iteration has completed. Hash the signature `function (err) {...}`, where `err` is populated if passed into the `next` callback at any point of iteration..
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
   * Synchronously iterates over each key/value pair stored in this table.
   *
   * @method forEachSync
   * @memberof Clusterluck.DTable
   * @instance
   *
   * @param {Function} cb - Function call on each key/value pair. Has the signature `function (key, val) {...}`.
   *
   * @return {Clusterluck.DTable} This instance.
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

    const obj = utils.mapToObject(this._table);
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
    const {fstream, wstream} = this._createFlushStreams();
    fstream.on("error", (err) => {
      if (called) return;
      called = true;
      wstream.end();
      fs.unlink(this._tmpDumpPath, _.partial(cb, err));
    });
    fstream.on("close", () => {
      if (called) return;
      called = true;
      fs.rename(this._tmpDumpPath, this._path, cb);
    });
    async.eachLimit(Object.keys(obj), 4, (key, next) => {
      if (called) return next("Closed prematurely.");
      const val = obj[key];
      wstream.write(JSON.stringify({
        key: key,
        value: this._encodeFn(val)
      }) + "\n");
      delete obj[key];
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
      return this._encodeFn(val);
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
   * @method _createFlushStreams
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   */
  _createFlushStreams() {
    const fstream = fs.createWriteStream(this._tmpDumpPath);
    let wstream;
    if (this._compress) {
      wstream = zlib.createGzip();
      wstream.pipe(fstream);
    } else {
      wstream = fstream;
    }
    return {fstream, wstream};
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
  _loadState(location, cb) {
    fs.stat(location, (err) => {
      if (err && err.code !== "ENOENT") return cb(err);
      else if (err && err.code === "ENOENT") return cb();
      const {fstream, rstream} = this._createLoadStreams(location);
      
      const rline = readline.createInterface({
        input: rstream
      });
      let called = false;
      rline.once("close", () => {
        if (called) return;
        called = true;
        return cb();
      });
      fstream.on("error", (err) => {
        if (called) return;
        called = true;
        fstream.resume();
        rline.close();
        return cb(err);
      });
      rline.on("line", (line) => {
        const lineObj = JSON.parse(line);
        const val = this._decodeFn(lineObj.value);
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
          return this._decodeFn(val);
        });

        try {
          lineObj.args.push(this._table);
          ops[lineObj.op].apply(null, lineObj.args);
        } catch (e) {
          debug("failed to complete operation %s with args " + JSON.stringify(lineObj.args) + " from %s", lineObj.op, path);
        }
      });
    });
  }

  /**
   *
   * @method _CreateLoadStreams
   * @memberof Clusterluck.DTable
   * @instance
   * @private
   *
   * @param {String} location
   *
   */
  _createLoadStreams(location) {
    const fstream = fs.createReadStream(location);
    let rstream;
    if (this._compress) {
      rstream = zlib.createGunzip();
      fstream.pipe(rstream);
    } else {
      rstream = fstream;
    }
    return {fstream, rstream};
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

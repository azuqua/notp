const _ = require("lodash"),
      debug = require("debug")("clusterluck:lib:mtable"),
      async = require("async"),
      shortid = require("shortid"),
      EventEmitter = require("events").EventEmitter;

class MTable extends EventEmitter {
  /**
   *
   * In-memory key/value storage with the same data structure API as the DTable class. Does not include functionality to persist to disk.
   *
   * @class MTable
   * @memberof Clusterluck
   *
   * @param {Object} opts
   * @param {String} opts.path
   * @param {Number} [opts.writeThreshold]
   * @param {Number} [opts.autoSave]
   * @param {Number} [opts.fsyncInterval]
   *
   */
  constructor() {
    super();
    this._table = new Map();
  }

  /**
   *
   * Starts dtable instance, which triggers an fopen call to LATEST.LOG, the fsync interval for this log file, as well as other internal intervals to check for storage snapshot flush conditions.
   *
   * @method start
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {String} name - Name of table, meant for debugging purposes.
   *
   * @return {Clusterluck.MTable} This instance.
   *
   */
  start(name) {
    this._name = name;
    this._id = shortid.generate();
    return this;
  }

  /**
   *
   * Stops the table, including all internal disk-based logic. If the table is idle and has an open file descriptor against LATEST.LOG, it will close immediately. If it's idle but the file descriptor against LATEST.LOG has been closed, this call will wait for a file descriptor to open again before continuing. Otherwise, the table isn't idle and therefore we wait for this condition.
   *
   * @method stop
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {Function} cb - Callback called once the table has been stopped.
   *
   */
  stop(cb) {
    this.emit("stop");
    this._id = null;
    async.nextTick(cb);
  }

  /**
   *
   * Trivial call to load state.
   *
   * @method load
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {Function} cb - Function of the form `function (err) {...}`, where `err` will be passed if an error occurs loading state from disk.
   *
   */
  load(cb) {
    async.nextTick(cb);
  }

  /**
   *
   * Returns whether this table is in an idle state or not.
   *
   * @method idle
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @return {Boolean} Whether this table is idle or not.
   *
   */
  idle() {
    return true;
  }

  /**
   *
   * Retrieves value stored at `key`, returning `undefined` if no such data exists.
   *
   * @method get
   * @memberof Clusterluck.MTable
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
   * @memberof Clusterluck.MTable
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
      throw MTable.invalidTypeError("smember", key, typeof out);
    }
    return out.has(val);
  }

  /**
   *
   * Retrieves value stored at hash key `hkey` under storage key `key`, returning `undefined` if no such data exists.
   *
   * @method hget
   * @memberof Clusterluck.MTable
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
      throw new MTable.invalidTypeError("hget", key, typeof out);
    }
    return out.get(hkey);
  }

  /**
   *
   * Sets value `value` under key `key`.
   *
   * @method set
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {String} key
   * @param {Map|Set|JSON} val 
   *
   * @return {Map|Set|JSON}
   *
   */
  set(key, val) {
    this._table.set(key, val);
    return val;
  }

  /**
   *
   * Inserts `val` into the set stored at key `key`.
   *
   * @method sset
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {String} key - Key which holds the set to insert `val` under.
   * @param {String} val - Value to insert into the set.
   *
   * @return {String} The set stored at `key`.
   *
   */
  sset(key, val) {
    const out = this._table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw MTable.invalidTypeError("sset", key, typeof out);
    }
    out.add(val);
    this._table.set(key, out);
    return out;
  }

  /**
   *
   * Sets `value` under the hash key `hkey` in the hash map stored at `key`.
   *
   * @method hset
   * @memberof Clusterluck.MTable
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
    const out = this._table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw MTable.invalidTypeError("hset", key, typeof out);
    }
    out.set(hkey, val);
    this._table.set(key, out);
    return out;
  }

  /**
   *
   * Removes key `key` from this table.
   *
   * @method del
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {String} key - Key to remove from this table.
   *
   * @return {Clusterluck.MTable} This instance.
   *
   */
  del(key) {
    this._table.delete(key);
    return this;
  }

  /**
   *
   * Deletes `val` from the set stored under `key`.
   *
   * @method sdel
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {String} key - Key which olds the set to remove `val` from.
   * @param {String} val - Value to remove from the set.
   *
   * @return {Clusterluck.MTable} This instance.
   *
   */
  sdel(key, val) {
    const out = this._table.get(key) || new Set();
    if (!(out instanceof Set)) {
      throw MTable.invalidTypeError("sdel", key, typeof out);
    }
    out.delete(val);
    if (out.size === 0) this._table.delete(key);
    return this;
  }

  /**
   *
   * Removes the hash key `hkey` from the hash map stored under `key`.
   *
   * @method hdel
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {String} key - Key which holds the hash map that `hkey` will be removed from.
   * @param {String} hkey - The hash key to remove from the hash map.
   *
   * @return {Clusterluck.MTable} This instance.
   *
   */
  hdel(key, hkey) {
    const out = this._table.get(key) || new Map();
    if (!(out instanceof Map)) {
      throw MTable.invalidTypeError("hdel", key, typeof out);
    }
    out.delete(hkey);
    if (out.size === 0) this._table.delete(key);
    return this;
  }

  /**
   *
   * Clears the contents of this table.
   *
   * @method clear
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @return {Clusterluck.MTable} This instance.
   *
   */
  clear() {
    this._table.clear();
    return this;
  }

  /**
   *
   * Asynchronously iterates over each key/value pair stored in this table at the point of this call.
   *
   * @method forEach
   * @memberof Clusterluck.MTable
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
   * @memberof Clusterluck.MTable
   * @instance
   *
   * @param {Function} cb - Function call on each key/value pair. Has the signature `function (key, val) {...}`.
   *
   * @return {Clusterluck.MTable} This instance.
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
   * @method invalidTypeError
   * @memberof Clusterluck.MTable
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
}

module.exports = MTable;

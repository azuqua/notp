var _ = require("lodash"),
    async = require("async"),
    fs = require("fs"),
    JSONStream = require("JSONStream"),
    util = require("util"),
    assert = require("assert"),
    debug = require("debug")("clusterluck:lib:dtable");

var StateTable = require("./table"),
    TableTerm = require("./table_term"),
    utils = require("./utils");

class DiskTable extends StateTable {
  /**
   *
   * @class DiskTable
   * @memberof NetKernel
   *
   * @param {NetKernel.GossipRing} gossip
   * @param {Object} opts
   *
   */
  constructor(kernel, gossip, opts) {
    super(kernel, gossip, opts);
    this._flushOpts = opts.flushOpts;
    this._flushCursor = null;
    this._flushInterval = null;
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
    this._flushInterval = setInterval(this.flush.bind(this), this._flushOpts.interval);
    return this;
  }

  pause() {
    clearInterval(this._flushInterval);
    this._flushInterval = null;
    this._flushCursor = null;
    super.pause();
    return this;
  }

  resume() {
    this._flushInterval = setInterval(this.flush.bind(this), this._flushOpts.interval);
    super.resume();
    return this;
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
      this._flushCursor === null;
  }

  load(cb) {
    var fstream = fs.createReadStream(this._flushOpts.path);
    var jstream = JSONStream.parse("*");
    var called = false;
    var dataHandler = (data) => {
      if (!DiskTable.validDiskEntry(data)) {
        jstream.emit("error", new Error("Invalid entry in JSON array."));
        return;
      }
      var key = data.key;
      var val = data.value;
      var term = (new TableTerm()).fromJSON(val);
      this._values.set(data.key, term);
    };
    jstream.on("data", dataHandler);
    jstream.on("end", () => {
      if (called) return;
      called = true;
      return cb();
    });
    var handler = (err) => {
      if (called) return;
      called = true;
      jstream.removeListener("data", dataHandler);
      jstream.end();
      jstream.resume();
      return cb(err);
    };
    jstream.on("error", handler);
    fstream.on("error", handler);
    fstream.pipe(jstream);
    return this;
  }

  loadSync() {
    var blob = fs.readFileSync(this._flushOpts.path);
    blob = utils.safeParse(blob);
    if (blob instanceof Error || !Array.isArray(blob)) {
      throw new Error("Data loaded from '" + this._flushOpts.path + "' contained an invalid JSON format.");
    }
    blob.forEach((ent) => {
      assert.ok(DiskTable.validDiskEntry(ent), "Invalid entry in JSON array.");
      this._values.set(ent.key, (new TableTerm()).fromJSON(ent.value));
    });
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
    this.emit("flush");
    var jstream = JSONStream.stringify();
    var fstream = fs.createWriteStream(this._flushOpts.path);
    jstream.pipe(fstream);
    this._flushCursor = this._values.entries();
    var curr = this._flushCursor;
    var done = false;
    var called = false;

    var handle = (err) => {
      if (called) return;
      done = true;
      called = true;
      jstream.end();
      jstream.resume();
    };
    jstream.on("error", handle);
    fstream.on("error", handle);

    async.whilst(() => {
      return done !== true && this._flushCursor === curr;
    }, (next) => {
      var cursor = utils.scanIterator(this._flushCursor);
      this._flushCursor = cursor.iterator;
      curr = this._flushCursor;
      cursor.values.forEach((ent) => {
        jstream.write({key: ent[0], value: ent[1].toJSON(true)});
      });
      done = cursor.done;
      async.nextTick(next);
    }, () => {
      if (!called) {
        called = true;
        jstream.end();
      }
      if (this._flushCursor === curr) {
        this._flushCursor = null;
      }
    });
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
    this.emit("flush");
    var obj = [];
    this._values.forEach((val, key) => {
      obj.push({key: key, value: val.toJSON(true)});
    });
    fs.writeFileSync(this._flushOpts.path, JSON.stringify(obj));
    return this;
  }

  static validDiskEntry(ent) {
    return utils.isPlainObject(ent) &&
      typeof ent.key === "string" && TableTerm.validJSON(ent.value);
  }
}

module.exports = DiskTable;

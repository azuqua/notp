var _ = require("lodash"),
    async = require("async"),
    microtime = require("microtime"),
    assert = require("assert"),
    debug = require("debug")("clusterluck:lib:vclock");

var utils = require("./utils");

class VectorClock {
  /**
   *
   * Vector clock implementation. Maintains a map from node IDs to clock information, containing an updated UNIX timestamp and an atomic counter (node-side).
   *
   * @class VectorClock VectorClock
   * @memberof Clusterluck
   *
   * @param {String} [node] - Node to start vector clock with.
   * @param {Number} [count] - Count of `node` to initialize with.
   *
   */
  constructor(node, count) {
    this._vector = {};
    this._size = 0;
    if (typeof node === "string" && typeof count === "number") {
      var time = microtime.now();
      this._vector[node] = {
        count: count,
        insert: time,
        time: time
      };
      this._size = 1;
    }
  }

  /**
   *
   * Inserts `node` into this vector clock. Initializes node state with a 0 counter and the current UNIX timestamp on this node.
   *
   * @method insert
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to insert into this instance.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  insert(node) {
    if (this._vector[node]) return this;
    this._size++;
    var time = microtime.now();
    this._vector[node] = {
      count: 0,
      insert: time,
      time: time
    };
    return this;
  }

  /**
   *
   * Updates the state of `node` in this vector clock with a counter and an optional UNIX timestamp. If `time` isn't provided, uses current UNIX timestamp.
   *
   * @method update
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to insert into this vector clock.
   * @param {Number} count - Count to update node state with.
   * @param {Number} [time] - UNIX timestamp to update node state with.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  update(node, count, time) {
    if (!this._vector[node]) this._size++;
    var inTime = microtime.now();
    var res = this._vector[node] || {count: 0, insert: inTime};
    res.count = count;
    res.time = time === undefined ? inTime : time;
    this._vector[node] = res;
    return this;
  }

  /**
   *
   * Removes `node` from this vector clock.
   *
   * @method remove
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to remove from this vector clock.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  remove(node) {
    if (this._vector[node]) this._size--;
    delete this._vector[node];
    return this;
  }

  /**
   *
   * Increments the counter stored at `node` in this vector clock. If `node` doesn't exist, it's inserted and incremented. The timestamp stored at `node` is updated to the current UNIX timestamp.
   *
   * @method increment
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to increment counter of.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  increment(node) {
    if (!this._vector[node]) this._size++;
    var time = microtime.now();
    var res = this._vector[node] || {count: 0, insert: time};
    res.count++;
    res.time = time;
    this._vector[node] = res;
    return this;
  }

  /**
   *
   * Get state from this vector clock at `node`.
   *
   * @method get
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to return state of.
   *
   * @return {Object} State of `node`.
   *
   */
  get(node) {
    return this._vector[node];
  }

  /**
   *
   * Gets the count stored `node` in this vector clock.
   *
   * @method getCount
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to return count of.
   *
   * @return {Number} Count of `node`.
   *
   */
  getCount(node) {
    var res = this._vector[node];
    return res !== undefined ? res.count : undefined;
  }

  /**
   *
   * Gets the microsecond UNIX insert timestamp stored at `node` in this vector clock.
   *
   * @method getInsert
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to return insert time of.
   *
   * @return {Number} Microsecond UNIX timestamp of `node`.
   *
   */
  getInsert(node) {
    var res = this._vector[node];
    return res !== undefined ? res.insert : undefined;
  }

  /**
   *
   * Gets the UNIX timestamp stored at `node` in this vector clock.
   *
   * @method getTimestamp
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to return timestamp of.
   *
   * @return {Number} UNIX timestamp of `node`.
   *
   */
  getTimestamp(node) {
    var res = this._vector[node];
    return res !== undefined ? res.time : undefined;
  }

  /**
   *
   * Returns whether `node` exists in this vector clock.
   *
   * @method has
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {String} node - Node to check existence of.
   *
   * @return {Boolean} Whether `node` has an entry in this vector clock.
   *
   */
  has(node) {
    return !!this._vector[node];
  }

  /**
   *
   * Merges two vector clocks, using the following policy:
   *   - If `entry` exists in v1 but not in v2, entry in v1 doesn't change
   *   - If `entry` exists in v2 but not in v1, entry is added into v1 as is
   *   - If `entry` exists in both v1 and v2 but the count at v2 is smaller than v1, the state of v1 is kept
   *   - If `entry` exists in both v1 and v2 but the count at v1 is smaller than v2, the state of v2 is used
   *   - If `entry` exists in both v1 and v2 and the counts are the same, prioritize based on UNIX timestamp
   *
   * @method merge
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {Clusterluck.VectorClock} vclock - Vector clock to merge this instance with.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  merge(vclock) {
    if (vclock.size() === 0) return this;
    if (this.size() === 0) {
      this._vector = _.cloneDeep(vclock._vector);
      this._size = vclock.size();
      return this;
    }
    _.extendWith(this._vector, vclock._vector, (val, other) => {
      if (val === undefined) return _.clone(other);
      if (other === undefined) return val;

      if (val.count < other.count) {
        return _.clone(other);
      }
      if (val.count > other.count) {
        return val;
      }
      var max = Math.max(val.time, other.time);
      return max === val.time ? val : _.clone(other);
    });
    this._size = Object.keys(this._vector).length;
    return this;
  }

  /**
   *
   * Returns whether this vector clocks descends from `vclock`.
   *
   * @method descends
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {Clusterluck.VectorClock} vclock - Vector clock to check if this instance descends from.
   *
   * @return {Boolean} Whether this vector clock descends from `vclock`.
   *
   */
  descends(vclock) {
    // if input is trivial, this vector clock descends from it
    if (vclock.size() === 0) return true;
    // if this instance is trivial, it doesn't descend from input
    if (this.size() === 0) return false;

    return _.every(vclock._vector, (val, key) => {
      if (!this._vector[key]) return false;
      var node = this._vector[key];
      return node.count >= val.count;
    });
  }

  /**
   *
   * Returns whether this vector clock equals `vclock`.
   *
   * @method equals
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {Clusterluck.VectorClock} vclock - Vector clock to check equality with.
   *
   * @return {Boolean} Whether this vector clock equals `vclock`.
   *
   */
  equals(vclock) {
    if (this._size !== vclock.size()) return false;
    return _.every(this._vector, (val, key) => {
      if (!vclock._vector[key]) return false;
      var node = vclock._vector[key];
      return node.count === val.count &&
        node.insert === val.insert &&
        node.time === val.time;
    });
  }

  /**
   *
   * Trims this vector clock. Requires that the number of entries in this clock be greater than `lowerBound`, and the oldest entry be older than `threshold`-`youngBound`. Any entries older than `oldBound` will be trimmed, as well as any entries above the `upperBound` limit.
   *
   * @method trim
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @param {Number} threshold - UNIX timestamp to check clock entries against.
   * @param {Object} opts - Object containing trim parameters.
   * @param {Number} opts.lowerBound - Number of entries required from trimming to occur.
   * @param {Number} opts.youngBound - Minimum difference between oldest entry and `threshold` for trimming to occur.
   * @param {Number} opts.upperBound - Maximum number of entries this vector clock can contain.
   * @param {Number} opts.oldBound - Maximum difference between any entry and `threshold`.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  trim(threshold, opts) {
    opts = opts || {};
    if (this.size() <= opts.lowerBound) {
      return this;
    }
    var keys = Object.keys(this._vector).sort((a, b) => {
      var objA = this._vector[a].time;
      var objB = this._vector[b].time;
      return objA > objB ? 1 : (objA === objB ? 0 : -1);
    });
    var val = this._vector[keys[0]];
    if (threshold - val.time > opts.youngBound) {
      this._trimClock(keys.reverse(), threshold, opts);
      this._size = Object.keys(this._vector).length;
    }
    return this;
  }

  /**
   *
   * Returns the nodes present in this vector clock.
   *
   * @method nodes
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @return {Array} Node IDs in this vector clock.
   *
   */
  nodes() {
    return Object.keys(this._vector);
  }

  /**
   *
   * Returns the number of entries in this vector clock.
   *
   * @method size
   * @memberof Clusterluck.VectorClock
   * @instance
   *
   * @return {Number} Number of entries in this vector clock.
   *
   */
  size() {
    return this._size;
  }

  /**
   * 
   * @method toJSON
   * @memberof Netkernel.VectorClock
   * @instance
   *
   * @param {Boolean} [fast] - Whether to compute the JSON serialization of this isntance quickly or safely.
   *
   * @return {Object} JSON serialization of this instance.
   *
   */
  toJSON(fast) {
    return fast === true ? this._vector : _.cloneDeep(this._vector);
  }

  /**
   * 
   * @method fromJSON
   * @memberof Netkernel.VectorClock
   * @instance
   *
   * @param {Object} ent - JSON object to instantiate state from.
   *
   * @return {Clusterluck.VectorClock} This instance.
   *
   */
  fromJSON(ent) {
    assert.ok(VectorClock.validJSON(ent));
    this._vector = ent;
    this._size = Object.keys(ent).length;
    return this;
  }

  /**
   *
   * @method _trimClock
   * @memberof Clusterluck.VectorClock
   * @private
   * @instance
   *
   */
  _trimClock(keys, threshold, opts) {
    if (keys.length === 0) return;
    var val = this._vector[_.last(keys)];
    if (keys.length > opts.upperBound ||
        threshold - val.time > opts.oldBound) {
      delete this._vector[_.last(keys)];
      keys.pop();
      return this._trimClock(keys, threshold, opts);
    }
    else return;
  }

  static validJSON(data) {
    return utils.isPlainObject(data);
  }
}

module.exports = VectorClock;

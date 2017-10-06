const _ = require("lodash"),
      debug = require("debug")("clusterluck:lib:chash"),
      rbt = require("functional-red-black-tree"),
      LRU = require("lru-cache"),
      crypto = require("crypto"),
      Node = require("./node"),
      utils = require("./utils"),
      consts = require("./consts");

const chashOpts = consts.chashOpts;

class CHash {
  /**
   *
   * Consistent hash implementation. Maintains a red-black tree containing the hash ring (for ordering), with keys as hashes of node names and values as the nodes themselves. This implementation also contains an `rfactor` (replication factor) and `pfactor` (persistence factor). `rfactor` dictates how many times an element is inserted into the ring, while `pfactor` dictates how many nodes will be used for redundancy.
   *
   * @class CHash CHash
   * @memberof Clusterluck
   *
   * @param {Number} rfactor - Replication factor for node insertion.
   * @param {Number} pfactor - Persistence factor for redundancy in node-neighbor calculations.
   * @param {RBTree} [tree] - Existing red-black tree to instantiate local tree from.
   * @param {Object} [opts] - Options object for hash ring.
   * @param {Number} [opts.maxCacheSize] - Maximum cache size for caching hash values in hash ring. Defaults to 5000.
   *
   */
  constructor(rfactor, pfactor, tree, opts=chashOpts) {
    opts = _.defaults(opts, chashOpts);
    this._rfactor = rfactor;
    this._pfactor = pfactor;
    this._tree = tree || rbt();
    this._cache = new LRU({max: opts.maxCacheSize});
    this._calculateWeights();
  }

  /**
   * Inserts `node` into this ring, repeating this process `rfactor` number of times. Ignores insertions if `node` is already present in the hash ring.
   *
   * @method insert
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to insert into this instance.
   * @param {Number} [weight] - Number of times to insert this node. Defaults to `rfactor`.
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  insert(node, weight) {
    if (this._weights.has(node.id())) return this;
    weight = weight !== undefined ? weight : this._rfactor;
    _.times(weight, (n) => {
      this._tree = this._tree.insert(this._nodeName(node, n+1), node);
    });
    this._weights.set(node.id(), weight);
    return this;
  }

  /**
   * Removes `node` from this ring, repeating this process `rfactor` number of times. Ignores removals `node` doesn't exist in the ring.
   *
   * @method remove
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to remove from this instance.
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  remove(node) {
    if (this._weights.has(node.id()) === false) return this;
    const weight = this._weights.get(node.id());
    _.times(weight, (n) => {
      this._tree = this._tree.remove(this._nodeName(node, n+1));
    });
    this._weights.delete(node.id());
    return this;
  }

  /**
   * Gets `node` from the hash ring by class. Useful if the id is known but host and port need to be queried.
   *
   * @method get
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to access.
   * @param {Node} def - Default value to return if `node` doesn't exist in the hash ring.
   * 
   * @return {Node} Node with matching id.
   *
   */
  get(node, def) {
    const res = this._tree.get(this._nodeName(node, 1));
    return res === undefined ? def : res;
  }

  /**
   * Update the state of `node` in the hash ring. Useful if the host and port of a node need to be updated.
   *
   * @method update
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to add state to.
   * @param {Node} state - State to add to this hash ring.
   * @param {Number} weight - Weight to change `node` to in the ring.
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  update(node, state, weight) {
    weight = weight !== undefined ? weight : (this._weights.get(node.id()) || this._rfactor);
    this.remove(node);
    this._weights.set(node.id(), weight);
    _.times(weight, (n) => {
      this._tree = this._tree.insert(this._nodeName(node, n+1), state);
    });
    return this;
  }

  /**
   * Finds the next node in the hash ring based on the bucket placement of `data`. If no succeeding node exists, loops around to the beginning of the hash ring and picks the first node.
   *
   * @method find
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {String} data - Data to find the next node off of.
   * 
   * @return {Node} Next node in the hash ring.
   *
   */
  find(data) {
    const hash = this._findHash(data);
    const iter = this._tree.gt(hash);
    if (iter.valid === false) return this._tree.begin.value;
    return iter.value;
  }

  /**
   * Returns a list of neighbors of `node` in the hash ring. This is bounded by `pfactor`, which dictates the maximum number of neighbors a node can have (barring size limitations).
   *
   * @method next
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to find neighbors of.
   * 
   * @return {Array} Neighbors of `node` in the hash ring.
   *
   */
  next(node, range) {
    if (this._weights.size <= 1 || this._weights.has(node.id()) === false) return [];

    range = Math.min(range !== undefined ? range : this._pfactor, this._weights.size-1);
    const result = new Map();
    const name = this._nodeName(node, 1);
    const iter = this._tree.gt(name);
    this._successors(node, iter, result, range);
    return utils.mapValues(result);
  }
  
  rangeNext(data, range) {
    if (this.size() === 0) return [];

    range = Math.min(range !== undefined ? range : (this._pfactor+1), this._weights.size);
    const hash = this._findHash(data);
    const result = new Map();

    let iter = this._tree.gt(hash);
    if (iter.valid === false) iter = this._tree.begin;

    const node = iter.value;
    result.set(node.id(), node);
    iter.next();
    this._successors(node, iter, result, range);
    return utils.mapValues(result);
  }

  /**
   * Returns a list of preceding neighbors of `node` in the hash ring. This is bounded by `pfactor`, which dictates the maximum number of neighbors a node can have (barring size limitations).
   *
   * @method next
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to find neighbors of.
   * 
   * @return {Array} Neighbors of `node` in the hash ring.
   *
   */
  prev(node, range) {
    if (this.size() <= 1 || this._weights.has(node.id()) === false) return [];

    range = Math.min(range !== undefined ? range : this._pfactor, this._weights.size-1);
    const result = new Map();
    const name = this._nodeName(node, 1);
    const iter = this._tree.lt(name);
    this._precursors(node, iter, result, range);
    return utils.mapValues(result);
  }

  rangePrev(data, range) {
    if (this.size() === 0) return [];

    range = Math.min(range !== undefined ? range : (this._pfactor+1), this._weights.size);
    const hash = this._findHash(data);
    const result = new Map();

    let iter = this._tree.lt(hash);
    if (iter.valid === false) iter = this._tree.end;

    const node = iter.value;
    result.set(node.id(), node);
    iter.prev();
    this._precursors(node, iter, result, range);
    return utils.mapValues(result);
  }

  /**
   * Computes a set-union on two hash rings, inserting any nodes present in `chash` that aren't in this hash ring.
   *
   * @method merge
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Clusterluck.CHash} chash - Hash ring to merge with.
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  merge(chash) {
    chash.tree().forEach((key, value) => {
      if (this._tree.get(key) === undefined) {
        this._tree = this._tree.insert(key, value);
        if (this._weights.has(value.id())) {
          const weight = this._weights.get(value.id());
          this._weights.set(value.id(), weight+1);
        } else {
          this._weights.set(value.id(), 1);
        }
      }
    });
    return this;
  }

  /**
   * Computes a set-intersection on two hash rings, removing any nodes not found in both rings.
   *
   * @method intersect
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Clusterluck.CHash} chash - Hash ring to intersect with.
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  intersect(chash) {
    let tree = rbt();
    this._weights.clear();
    chash.tree().forEach((key, value) => {
      if (this._tree.get(key) !== undefined) {
        tree = tree.insert(key, value);
        if (this._weights.has(value.id())) {
          const weight = this._weights.get(value.id());
          this._weights.set(value.id(), weight+1);
        } else {
          this._weights.set(value.id(), 1);
        }
      }
    });
    this._tree = tree;
    return this;
  }

  /**
   * Acts as a getter for the number of elements in this hash ring.
   *
   * @method size
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @return {Number} Number of elements in this hash ring.
   *
   */
  size() {
    return this._tree.length;
  }

  numberNodes() {
    return this._weights.size;
  }

  weights() {
    return this._weights;
  }

  /**
   * Acts as a getter for the nodes in this hash ring.
   *
   * @method nodes
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @return {Array} List of nodes in this hash ring.
   *
   */
  nodes() {
    const memo = {};
    this._tree.forEach((key, node) => {
      memo[node.id()] = node;
    });
    return _.values(memo);
  }

  /**
   * Checks `node` is defined in this hash ring.
   *
   * @method isDefined
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @return {Boolean} Whether `node` is defined.
   *
   */
  isDefined(node) {
    return this._weights.has(node.id());
  }

  /**
   * Acts as a getter/setter for the red-black tree of this hash ring.
   *
   * @method tree
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {RBTree} [tree] - Tree to set on this instance.
   *
   * @return {RBTree} Red-black tree of this instance.
   *
   */
  tree(t) {
    if (t !== undefined) {
      this._tree = t;
    }
    return this._tree;
  }

  /**
   * Acts as a getter/setter for the rfactor of this hash ring.
   *
   * @method rfactor
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Number} [r] - rfactor to set on this ring.
   *
   * @return {Number} rfactor of this instance.
   *
   */
  rfactor(r) {
    if (typeof r === "number") {
      this._rfactor = r;
    }
    return this._rfactor;
  }

  /**
   * Acts as a getter/setter for the pfactor of this hash ring.
   *
   * @method pfactor
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Number} [p] - pfactor to set on this ring.
   *
   * @return {Number} pfactor of this instance.
   *
   */
  pfactor(p) {
    if (typeof p === "number") {
      this._pfactor = p;
    }
    return this._pfactor;
  }

  /**
   *
   * Returns whether two consistent hash rings are equal. This involves checking the following:
   *  - The hash rings have the same size
   *  - For every entry in this instance's RBT, the corresponding entry exists in the RBT of `chash`
   *
   * @method equals
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Clusterluck.CHash} chash - CHash to derive equality against.
   *
   * @return {Boolean} Whether this chash instance equals `chash`.
   *
   */
  equals(chash) {
    if (this._tree.length !== chash.tree().length) return false;
    const tree = chash.tree();
    const it = this.begin();
    let out;
    while (it.valid) {
      out = tree.get(it.key);
      if (!out) return false;
      if (!it.value.equals(out)) return false;
      it.next();
    }
    return true;
  }

  /**
   * Computes the JSON serialization of this instance. Useful when periodically flushing this ring to disk.
   *
   * @method toJSON
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Boolean} [fast] - Whether to compute the JSON serialization of this instance using a copy of internal state.
   *
   * @return {Object} JSON serialization of this instance.
   *
   */
  toJSON(fast) {
    const tree = [];
    this._tree.forEach((key, val) => {
      tree.push({key: key, value: val.toJSON(fast)});
    });
    const out = {
      rfactor: this._rfactor,
      pfactor: this._pfactor,
      tree: tree
    };
    return fast === true ? out : _.cloneDeep(out);
  }

  /**
   * Instantiates state from a JSON object.
   *
   * @method fromJSON
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Object} ent - JSON object to instantiate state from.
   *
   * @return {Clusterluck.CHash} This instance.
   *
   */
  fromJSON(ent) {
    this._rfactor = ent.rfactor;
    this._pfactor = ent.pfactor;
    this._tree = rbt();
    ent.tree.forEach((val) => {
      const node = new Node(val.value.id, val.value.host, val.value.port);
      this._tree = this._tree.insert(val.key, node);
    });
    this._calculateWeights();
    return this;
  }

  /**
   * Generates an iterator on this hash ring, starting at the first (smallest hash) node.
   *
   * @method begin
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @return {Iterator} Iterator over this instance.
   *
   */
  begin() {
    return this._tree.begin;
  }

  /**
   * Generates an iterator on this hash ring, starting at the last (largest hash) node.
   *
   * @method end
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @return {Iterator} Iterator over this instance.
   *
   */
  end() {
    return this._tree.end;
  }

  /**
   * Generates an iterator on this hash ring, starting at `node`.
   *
   * @method iterator
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to start iteration on.
   *
   * @return {Iterator} Iterator over this instance.
   *
   */
  iterator(node) {
    return this._tree.find(node);
  }

  /**
   *
   * @method _successors
   * @memberof Clusterluck.CHash
   * @private
   * @instance
   *
   */
  _successors(node, iter, memo, range) {
    // if we have more nodes than pfactor, pfactor will be number of elements returned
    // otherwise, return number of nodes other than node
    if (memo.size === range) {
      return memo;
    }
    if (iter.valid === false) {
      return this._successors(node, this._tree.begin, memo, range);
    }
    if (iter.value.id() === node.id()) {
      iter.next();
      return this._successors(node, iter, memo, range);
    }

    const val = iter.value;
    memo.set(val.id(), val);
    iter.next();
    return this._successors(node, iter, memo, range);
  }

  /**
   *
   * @method _precursors
   * @memberof Clusterluck.CHash
   * @private
   * @instance
   *
   */
  _precursors(node, iter, memo, range) {
    if (memo.size === range) {
      return memo;
    }
    if (iter.valid === false) {
      return this._precursors(node, this._tree.end, memo, range);
    }
    if (iter.value.id() === node.id()) {
      iter.prev();
      return this._precursors(node, iter, memo, range);
    }

    const val = iter.value;
    memo.set(val.id(), iter.value);
    iter.prev();
    return this._precursors(node, iter, memo, range);
  }

  /**
   *
   * @method _nodeName
   * @memberof Clusterluck.CHash
   * @instance
   * @private
   *
   */
  _nodeName(node, index) {
    return this._findHash(node.id() + "_" + index);
  }

  /**
   *
   * @method _findHash
   * @memberof Clusterluck.CHash
   * @instance
   * @private
   *
   */
  _findHash(data) {
    if (this._cache.has(data)) return this._cache.get(data);
    const value = crypto.createHash("sha256").update(data).digest("base64");
    this._cache.set(data, value);
    return value;
  }

  _calculateWeights() {
    const memo = new Map();
    this._tree.forEach((key, val) => {
      if (memo.has(val.id())) {
        const weight = memo.get(val.id());
        memo.set(val.id(), weight+1);
      } else {
        memo.set(val.id(), 1);
      }
    });
    this._weights = memo;
    return memo;
  }
}

module.exports = CHash;

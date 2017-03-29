var _ = require("lodash"),
    debug = require("debug")("clusterluck:lib:chash"),
    rbt = require("functional-red-black-tree"),
    crypto = require("crypto"),
    Node = require("./node"),
    utils = require("./utils");

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
   *
   */
  constructor(rfactor, pfactor, tree) {
    this._rfactor = rfactor;
    this._pfactor = pfactor;
    this._tree = tree || rbt();
  }

  /**
   * Inserts `node` into this ring, repeating this process `rfactor` number of times. Ignores insertions if `node` is already present in the hash ring.
   *
   * @method insert
   * @memberof Clusterluck.CHash
   * @instance
   *
   * @param {Node} node - Node to insert into this instance.
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  insert(node) {
    if (this._tree.get(CHash._nodeName(node, 1))) return this;
    _.times(this._rfactor, (n) => {
      this._tree = this._tree.insert(CHash._nodeName(node, n+1), node);
    });
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
    if (!this._tree.get(CHash._nodeName(node, 1))) return this;
    _.times(this._rfactor, (n) => {
      this._tree = this._tree.remove(CHash._nodeName(node, n+1));
    });
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
    var res = this._tree.get(CHash._nodeName(node, 1));
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
   * 
   * @return {Clusterluck.CHash} This instance.
   *
   */
  update(node, state) {
    this.remove(node);
    _.times(this._rfactor, (n) => {
      this._tree = this._tree.insert(CHash._nodeName(node, n+1), state);
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
    var hash = crypto.createHash("sha256").update(data).digest("base64");
    var iter = this._tree.gt(hash);
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
  next(node) {
    if (this.size() === 0) return [];
    if (this.size() === this._rfactor) return [];
    var result = new Map();
    _.times(this._rfactor, (n) => {
      var name = CHash._nodeName(node, n+1);
      var iter = this._tree.gt(name);
      this._successors(node, iter, result);
    });
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
  prev(node) {
    if (this.size() === 0) return [];
    if (this.size() === this._rfactor) return [];
    var result = new Map();
    _.times(this._rfactor, (n) => {
      var name = CHash._nodeName(node, n+1);
      var iter = this._tree.lt(name);
      this._precursors(node, iter, result);
    });
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
    var str;
    if (chash.rfactor() !== this._rfactor) {
      str = "rfactors " + chash.rfactor() +
        " and " + this._rfactor + " do not match.";
      throw new Error(str);
    }
    if (chash.pfactor() !== this._pfactor) {
      str = "pfactors " + chash.pfactor() +
        " and " + this._pfactor + " do not match.";
      throw new Error(str);
    }
    return this._merge(chash);
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
    var str;
    if (chash.rfactor() !== this._rfactor) {
      str = "rfactors " + chash.rfactor() +
        " and " + this._rfactor + " do not match.";
      throw new Error(str);
    }
    if (chash.pfactor() !== this._pfactor) {
      str = "pfactors " + chash.pfactor() +
        " and " + this._pfactor + " do not match.";
      throw new Error(str);
    }
    return this._intersect(chash);
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
    var memo = {};
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
    var name = CHash._nodeName(node, 1);
    return !!this._tree.get(name);
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
    var dims = this._rfactor === chash.rfactor() &&
      this._pfactor === chash.pfactor() &&
      this._tree.length === chash.tree().length;
    if (dims === false) return false;
    var tree = chash.tree();
    var it = this.begin();
    var out;
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
    var tree = [];
    this._tree.forEach((key, val) => {
      tree.push({key: key, value: val.toJSON(fast)});
    });
    var out = {
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
      var node = new Node(val.value.id, val.value.host, val.value.port);
      this._tree = this._tree.insert(val.key, node);
    });
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
  _successors(node, iter, memo) {
    // if we have more nodes than pfactor, pfactor will be number of elements returned
    // otherwise, return number of nodes other than node
    if (memo.size === Math.min(this.size()/this._rfactor - 1, this._pfactor)) {
      return memo;
    }
    if (iter.valid === false) {
      return this._successors(node, this._tree.begin, memo);
    }
    if (iter.value.id() === node.id()) {
      iter.next();
      return this._successors(node, iter, memo);
    }

    var val = iter.value;
    memo.set(val.id(), val);
    iter.next();
    return this._successors(node, iter, memo);
  }

  /**
   *
   * @method _precursors
   * @memberof Clusterluck.CHash
   * @private
   * @instance
   *
   */
  _precursors(node, iter, memo) {
    if (memo.size === Math.min(this.size()/this._rfactor - 1, this._pfactor)) {
      return memo;
    }
    if (iter.valid === false) {
      return this._precursors(node, this._tree.end, memo);
    }
    if (iter.value.id() === node.id()) {
      iter.prev();
      return this._precursors(node, iter, memo);
    }

    var val = iter.value;
    memo.set(val.id(), iter.value);
    iter.prev();
    return this._precursors(node, iter, memo);
  }

  /**
   *
   * @method _merge
   * @memberof Clusterluck.CHash
   * @private
   * @instance
   *
   */
  _merge(chash) {
    chash.tree().forEach((key, value) => {
      if (this._tree.get(key) === undefined) {
        this._tree = this._tree.insert(key, value);
      }
    });
    return this;
  }

  /**
   *
   * @method _intersect
   * @memberof Clusterluck.CHash
   * @private
   * @instance
   *
   */
  _intersect(chash) {
    var tree = rbt();
    chash.tree().forEach((key, value) => {
      if (this._tree.get(key) !== undefined) {
        tree = tree.insert(key, value);
      }
    });
    this._tree = tree;
    return this;
  }

  /**
   *
   * @method _nodeName
   * @memberof Clusterluck.CHash
   * @private
   * @static
   *
   */
  static _nodeName(node, index) {
    return crypto.createHash("sha256").update(node.id() + "_" + index, "utf8").digest("base64");
  }
}

module.exports = CHash;

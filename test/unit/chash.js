var _ = require("lodash"),
    async = require("async"),
    rbt = require("functional-red-black-tree"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("CHash unit tests", function () {
    var CHash = lib.chash;
    var Node = lib.node;
    var rfactor = 1, pfactor = 1, chash;
    beforeEach(function () {
      chash = new CHash(rfactor, pfactor);
    });

    it("Should construct a new CHash", function () {
      assert.equal(chash.rfactor(), 1);
      assert.equal(chash.pfactor(), 1);
      assert.equal(chash.tree().length, 0);
      assert.equal(chash._weights.size, 0);

      var tree = rbt();
      tree = tree.insert("key", new Node("foo", "bar", 7022));
      chash = new CHash(rfactor, pfactor, tree);
      assert.equal(chash.rfactor(), 1);
      assert.equal(chash.pfactor(), 1);
      assert.equal(chash.tree().length, 1);
      assert.equal(chash._weights.get("foo"), 1);
    });

    it("Should insert nodes", function () {
      // should insert
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.equal(chash.size(), rfactor);
      assert.equal(chash._weights.get(node.id()), 1);

      // should do nothing if 'id' already exists
      chash.insert(node);
      assert.equal(chash.size(), rfactor);

      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node2, rfactor+1);
      assert.equal(chash.size(), 2*rfactor+1);
      assert.equal(chash._weights.get(node2.id()), rfactor+1);
      assert.equal(chash._weights.size, 2);
    });

    it("Should remove nodes", function () {
      // should insert
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node2, rfactor+1);

      // should remove node with custom weight
      chash.remove(node2);
      assert.equal(chash.size(), rfactor);

      // should remove node
      chash.remove(node);
      assert.equal(chash.size(), 0);

      // should do nothing if 'id' doesn't exist
      chash.remove(node);
      assert.equal(chash.size(), 0);
      assert.equal(chash._weights.size, 0);
    });

    it("Should get value at node", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should get existing value
      var out = chash.get(node);
      assert.equal(out.id(), node.id());

      // should return default if node doesn't exist
      var node2 = new Node("id2", "localhost", 8001);
      out = chash.get(node2, "here");
      assert.equal(out, "here");
    });

    it("Should update weight at node", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.equal(chash.size(), rfactor);

      chash.update(node, rfactor+1);
      assert.deepEqual(chash.get(node), node);
      assert.equal(chash.size(), rfactor+1);
      assert.equal(chash._weights.get(node.id()), rfactor+1);
      assert.equal(chash._weights.size, 1);
    });

    it("Should find next greatest value when querying with data", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should get existing value
      var out = chash.find("asdf");
      assert.equal(out.id(), node.id());

      // should return default if node doesn't exist
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node2);
      out = chash.find(node.id() + "_" + 1);
      assert.equal(out.id(), node2.id());
      out = chash.find(node2.id() + "_" + 1);
      assert.equal(out.id(), node.id());
    });

    it("Should find neighbors of a node (pfactor=1,rfactor=1)", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return no neighbors if only one node
      assert.lengthOf(chash.next(node), 0);

      // should return no neighbors if no nodes
      var chash2 = new CHash(rfactor, pfactor);
      assert.lengthOf(chash2.next(node), 0);

      // should return neighbors
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node2);
      var out = chash.next(node);
      assert.lengthOf(out, chash.pfactor());
      assert.equal(out[0].id(), node2.id());
      out = chash.next(node2);
      assert.lengthOf(out, chash.pfactor());
      assert.equal(out[0].id(), node.id());

      _.times(2, (n) => {
        var tmp = new Node("id" + (n+3), "localhost", 8000 + n + 3);
        chash.insert(tmp);
      });
      out = chash.next(node);
      assert.lengthOf(out, chash.pfactor());
      assert(out[0].id() !== node.id());
    });

    it("Should find neighbors of a node (pfactor>1,rfactor=1)", function () {
      chash.pfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      var out = chash.next(node);
      assert.lengthOf(out, chash.pfactor());
      assert.notOk(_.find(out, (el) => {
        return el.id() === node.id();
      }));
    });

    it("Should find neighbors of a node (pfactor>1,rfactor>1)", function () {
      chash.pfactor(3);
      chash.rfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      var out = chash.next(node);
      assert.lengthOf(out, chash.pfactor());
      assert.notOk(_.find(out, (el) => {
        return el.id() === node.id();
      }));
    });

    it("Should find neighbors of a node with specified range", function () {
      chash.pfactor(3);
      chash.rfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      var out = chash.next(node, 1);
      assert.lengthOf(out, 1);
      assert.notOk(out[0].id() === node.id());
    });

    it("Should grab range of nodes based on a key, uniqueness/length check", function () {
      var out = chash.rangeNext("foo");
      assert.lengthOf(out, 0);

      chash.pfactor(2);
      chash.rfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      out = chash.rangeNext("key");
      assert.lengthOf(out, chash.pfactor()+1);
      var outMap = out.map((n) => n.id());
      assert.lengthOf(_.uniq(outMap), out.length);

      var out2 = chash.rangeNext("key", 2);
      assert.lengthOf(out2, 2);
      assert.deepEqual(out.slice(0, 2), out2);

      var end = chash.tree().end;
      var key = end.key;
      var found = _.find([1,2,3].map((n) => {
        return {node: end.value, n: n};
      }), (val) => {
        return chash._nodeName(val.node, val.n) === key;
      });
      out2 = chash.rangeNext(found.node.id() + "_" + found.n, 2);
      assert.lengthOf(out2, 2);
      var out2Map = out2.map((n) => n.id());
      assert.lengthOf(_.uniq(out2Map), out2.length);
    });

    it("Should grab range of nodes based on a key, explicit list check", function () {
      var node1 = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      var node3 = new Node("id3", "localhost", 8002);
      var initKeys = [
        "key1",
        "key2",
        "key3",
        "key4",
        "key5",
        "key6"
      ];
      var keys = initKeys.map((key) => {
        return {key: key, hash: chash._findHash(key)};
      }).sort((a, b) => {
        return a.hash < b.hash ? -1 : (a.hash === b.hash ? 0 : 1);
      });
      initKeys = _.map(keys, "key");
      keys = _.map(keys, "hash");
      var tree = [
        node1,
        node2,
        node1,
        node3,
        node2,
        node3,
      ].map((val, i) => {
        return {key: keys[i], value: val.toJSON(true)};
      });
      chash.fromJSON({rfactor: 2, pfactor: 2, tree: tree});
      var out = chash.rangeNext(initKeys[0], 3);
      assert.deepEqual(out, [node2, node1, node3]);
      out = chash.rangeNext(initKeys[3], 3);
      assert.deepEqual(out, [node2, node3, node1]);
    });

    it("Should find preceding neighbors of a node (pfactor=1,rfactor=1)", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return no neighbors if only one node
      assert.lengthOf(chash.prev(node), 0);

      // should return no neighbors if no nodes
      var chash2 = new CHash(rfactor, pfactor);
      assert.lengthOf(chash2.prev(node), 0);

      // should return neighbors
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node2);
      var out = chash.prev(node);
      assert.lengthOf(out, chash.pfactor());
      assert.equal(out[0].id(), node2.id());
      out = chash.prev(node2);
      assert.lengthOf(out, chash.pfactor());
      assert.equal(out[0].id(), node.id());

      _.times(2, (n) => {
        var tmp = new Node("id" + (n+3), "localhost", 8000 + n + 3);
        chash.insert(tmp);
      });
      out = chash.prev(node);
      assert.lengthOf(out, chash.pfactor());
      assert(out[0].id() !== node.id());
    });

    it("Should find preceding neighbors of a node (pfactor>1,rfactor=1)", function () {
      chash.pfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      var out = chash.prev(node);
      assert.lengthOf(out, chash.pfactor());
      assert.notOk(_.find(out, (el) => {
        return el.id() === node.id();
      }));
    });

    it("Should find preceding neighbors of a node (pfactor>1,rfactor>1)", function () {
      chash.pfactor(3);
      chash.rfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      var out = chash.prev(node);
      assert.lengthOf(out, chash.pfactor());
      assert.notOk(_.find(out, (el) => {
        return el.id() === node.id();
      }));
    });

    it("Should find preceding neighbors of a node with specified range", function () {
      chash.pfactor(3);
      chash.rfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      var out = chash.prev(node, 1);
      assert.lengthOf(out, 1);
      assert.notOk(out[0].id() === node.id());
    });

    it("Should grab reverse range of nodes based on key, unique/length check", function () {
      var out = chash.rangePrev("foo");
      assert.lengthOf(out, 0);

      chash.pfactor(2);
      chash.rfactor(3);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should return neighbors
      _.times(3, (n) => {
        var tmp = new Node("id" + (n+1), "localhost", 8000 + n + 1);
        chash.insert(tmp);
      });
      out = chash.rangePrev("key");
      assert.lengthOf(out, chash.pfactor()+1);

      var out2 = chash.rangePrev("key", 2);
      assert.lengthOf(out2, 2);
      assert.deepEqual(out.slice(0, 2), out2);

      var begin = chash.tree().begin;
      var key = begin.key;
      var found = _.find([1,2,3].map((n) => {
        return {node: begin.value, n: n};
      }), (val) => {
        return chash._nodeName(val.node, val.n) === key;
      });
      out2 = chash.rangePrev(found.node.id() + "_" + found.n, 2);
      assert.lengthOf(out2, 2);
    });

    it("Should grab reverse range of nodes based on a key, explicit list check", function () {
      var node1 = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      var node3 = new Node("id3", "localhost", 8002);
      var initKeys = [
        "key1",
        "key2",
        "key3",
        "key4",
        "key5",
        "key6"
      ];
      var keys = initKeys.map((key) => {
        return {key: key, hash: chash._findHash(key)};
      }).sort((a, b) => {
        return a.hash < b.hash ? -1 : (a.hash === b.hash ? 0 : 1);
      });
      initKeys = _.map(keys, "key");
      keys = _.map(keys, "hash");
      var tree = [
        node1,
        node2,
        node1,
        node3,
        node2,
        node3,
      ].map((val, i) => {
        return {key: keys[i], value: val.toJSON(true)};
      });
      chash.fromJSON({rfactor: 2, pfactor: 2, tree: tree});
      var out = chash.rangePrev(initKeys[0], 3);
      assert.deepEqual(out, [node3, node2, node1]);
      out = chash.rangePrev(initKeys[3], 3);
      assert.deepEqual(out, [node1, node2, node3]);
    });

    it("Should merge ring, not add any existing entries", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      chash2.insert(node);
      chash.merge(chash2);
      assert.equal(chash.size(), chash.rfactor());
      assert.ok(chash.get(node));
      assert.equal(chash._weights.get(node.id()), rfactor);
    });

    it("Should merge ring, add new nodes", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8000);
      chash.insert(node);
      chash2.insert(node, rfactor+1);
      chash2.insert(node2);
      chash.merge(chash2);
      assert.equal(chash.size(), chash.rfactor()*2+1);
      var nodes = _.map(chash.nodes(), (el) => {
        return el.id();
      });
      assert.lengthOf(_.xor(nodes, ["id", "id2"]), 0);
      assert.equal(chash._weights.get(node.id()), rfactor+1);
      assert.equal(chash._weights.get(node2.id()), rfactor);
    });

    it("Should intersect rings, subset", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node, 2);
      chash2.insert(node, 2);
      chash2.insert(node2);
      chash.intersect(chash2);
      assert.equal(chash.size(), chash.rfactor()*2);
      assert.equal(chash.nodes()[0].id(), node.id());
      assert.equal(chash._weights.get(node.id()), 2);
      assert.equal(chash._weights.has(node2.id()), false);
    });

    it("Should intersect rings, superset", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node);
      chash.insert(node2);
      chash2.insert(node);
      chash.intersect(chash2);
      assert.equal(chash.size(), chash.rfactor());
      assert.equal(chash.nodes()[0].id(), node.id());
      assert.equal(chash._weights.get(node.id()), rfactor);
      assert.equal(chash._weights.has(node2.id()), false);
    });

    it("Should intersect rings, diff", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node);
      chash2.insert(node2);
      chash.intersect(chash2);
      assert.equal(chash.size(), 0);
      assert.equal(chash._weights.size, 0);
    });

    it("Should return size correctly", function () {
      assert.equal(chash.size(), 0);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.equal(chash.size(), 1);
    });

    it("Should return number of nodes correctly", function () {
      assert.equal(chash.numberNodes(), 0);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.equal(chash.numberNodes(), 1);
    });

    it("Should return weights of nodes", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node2, 2);

      var weights = chash.weights();
      assert.equal(weights.get(node.id()), 1);
      assert.equal(weights.get(node2.id()), 2);
    });

    it("Should return nodes correctly", function () {
      assert.lengthOf(chash.nodes(), 0);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      var nodes = chash.nodes();
      assert.lengthOf(nodes, 1);
      assert.equal(nodes[0].id(), node.id());
    });

    it("Should return if a node is defined or not", function () {
      var node = new Node("id", "localhost", 8000);
      assert.equal(chash.isDefined(node), false);
      chash.insert(node);
      assert.equal(chash.isDefined(node), true);
    });

    it("Should fail equality check if tree sizes mismatch", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.notOk(chash.equals(chash2));
    });

    it("Should fail equality check if trees aren't equal", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node);
      chash2.insert(node2);
      assert.notOk(chash.equals(chash2));
    });

    it("Should pass equality check if two chashes equal each other", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      chash2.insert(node);
      assert.ok(chash.equals(chash2));
    });

    it("Should serialize to JSON", function () {
      var out = chash.toJSON(true);
      assert.deepEqual(out, {
        rfactor: rfactor,
        pfactor: pfactor,
        tree: []
      });

      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      out = chash.toJSON(true);
      var name = chash._nodeName(node, 1);
      assert.deepEqual(out, {
        rfactor: rfactor,
        pfactor: pfactor,
        tree: [
          {
            key: name,
            value: {id: "id", host: "localhost", port: 8000}
          }
        ]
      });
    });

    it("Should properly parse JSON", function () {
      var node = new Node("id", "localhost", 8000);
      var name = chash._nodeName(node, 1);
      var ent = {
        rfactor: 1,
        pfactor: 1,
        tree: [
          {
            key: name,
            value: {
              id: "id",
              host: "localhost",
              port: 8000
            }
          }
        ]
      };
      chash.fromJSON(ent);
      assert.equal(chash.rfactor(), ent.rfactor);
      assert.equal(chash.pfactor(), ent.pfactor);
      assert.equal(chash.size(), 1);
      assert.equal(chash._weights.get("id"), 1);
      assert.ok(chash.get(node));
    });

    it("Should create a starting iterator", function () {
      assert.deepEqual(chash.begin(), chash.tree().begin);
    });

    it("Should create an ending iterator", function () {
      assert.deepEqual(chash.end(), chash.tree().end);
    });

    it("Should create an iterator", function () {
      var node = new Node("id", "localhost", 8000);
      var iter = chash.iterator(node);
      assert.deepEqual(iter, chash.tree().find(node));
    });
  });
};

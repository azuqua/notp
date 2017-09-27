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

      var tree = rbt();
      tree = tree.insert("key", "value");
      chash = new CHash(rfactor, pfactor, tree);
      assert.equal(chash.rfactor(), 1);
      assert.equal(chash.pfactor(), 1);
      assert.equal(chash.tree().length, 1);
    });

    it("Should insert nodes", function () {
      // should insert
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.equal(chash.size(), rfactor);

      // should do nothing if 'id' already exists
      chash.insert(node);
      assert.equal(chash.size(), rfactor);
    });

    it("Should remove nodes", function () {
      // should insert
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      // should remove node
      chash.remove(node);
      assert.equal(chash.size(), 0);

      // should do nothing if 'id' doesn't exist
      chash.remove(node);
      assert.equal(chash.size(), 0);
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

    it("Should update state at node", function () {
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);

      chash.update(node, "here");
      assert.equal(chash.get(node), "here");
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

    it("Should fail to merge two rings if rfactors/pfactors don't match", function (){
      var chash2 = new CHash(rfactor+1, pfactor);
      var out;
      try {
        out = chash.merge(chash2);
      } catch (e) {
        out = e;
      }
      assert(out instanceof Error);

      chash2 = new CHash(rfactor, pfactor+1);
      try {
        out = chash.merge(chash2);
      } catch (e) {
        out = e;
      }
      assert(out instanceof Error);
    });

    it("Should merge ring, not add any existing entries", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      chash2.insert(node);
      chash.merge(chash2);
      assert.equal(chash.size(), chash.rfactor());
      assert.ok(chash.get(node));
    });

    it("Should merge ring, add new nodes", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8000);
      chash.insert(node);
      chash2.insert(node2);
      chash.merge(chash2);
      assert.equal(chash.size(), chash.rfactor()*2);
      var nodes = _.map(chash.nodes(), (el) => {
        return el.id();
      });
      assert.lengthOf(_.xor(nodes, ["id", "id2"]), 0);
    });

    it("Should fail to intersect two rings if rfactors/pfactors don't match", function () {
      var chash2 = new CHash(rfactor+1, pfactor);
      var out;
      try {
        out = chash.intersect(chash2);
      } catch (e) {
        out = e;
      }
      assert(out instanceof Error);

      chash2 = new CHash(rfactor, pfactor+1);
      try {
        out = chash.intersect(chash2);
      } catch (e) {
        out = e;
      }
      assert(out instanceof Error);
    });

    it("Should intersect rings, subset", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node);
      chash2.insert(node);
      chash2.insert(node2);
      chash.intersect(chash2);
      assert.equal(chash.size(), chash.rfactor());
      assert.equal(chash.nodes()[0].id(), node.id());
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
    });

    it("Should intersect rings, diff", function () {
      var chash2 = new CHash(rfactor, pfactor);
      var node = new Node("id", "localhost", 8000);
      var node2 = new Node("id2", "localhost", 8001);
      chash.insert(node);
      chash2.insert(node2);
      chash.intersect(chash2);
      assert.equal(chash.size(), 0);
    });

    it("Should return size correctly", function () {
      assert.equal(chash.size(), 0);
      var node = new Node("id", "localhost", 8000);
      chash.insert(node);
      assert.equal(chash.size(), 1);
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

    it("Should fail equality check if rfactors mismatch", function () {
      var chash2 = new CHash(rfactor+1, pfactor);
      assert.notOk(chash.equals(chash2));
    });

    it("Should fail equality check if pfactors mismatch", function () {
      var chash2 = new CHash(rfactor, pfactor+1);
      assert.notOk(chash.equals(chash2));
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

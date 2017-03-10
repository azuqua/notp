var _ = require("lodash"),
    async = require("async"),
    rbt = require("functional-red-black-tree"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("VectorClock unit tests", function () {
    var VectorClock = lib.vclock;
    var vclock;
    beforeEach(function () {
      vclock = new VectorClock();
    });

    it("Should construct a vclock", function () {
      assert.equal(vclock._size, 0);

      vclock = new VectorClock("id", 1);
      assert.equal(vclock.size(), 1);
      assert.deepEqual(vclock.get("id").count, 1);
    });

    it("Should insert nodes", function () {
      var node = "key";
      vclock.insert(node);
      assert.equal(vclock.size(), 1);
      assert.equal(vclock.get("key").count, 0);

      vclock.increment(node);
      vclock.insert(node);
      assert.equal(vclock.size(), 1);
      assert.equal(vclock.getCount(node), 1);
    });

    it("Should update node in clock", function () {
      var node = "key";
      vclock.update(node, 1);
      assert.equal(vclock.getCount(node), 1);
      assert.equal(vclock.size(), 1);

      var date = Date.now();
      vclock.update(node, 2, date);
      assert.equal(vclock.getCount(node), 2);
      assert.equal(vclock.getTimestamp(node), date);
      assert.equal(vclock.size(), 1);
    });

    it("Should remove node in clock", function () {
      var node = "key";
      vclock.remove(node);
      assert.equal(vclock.get(node), undefined);
      assert.equal(vclock.size(), 0);

      vclock.insert(node);
      vclock.remove(node);
      assert.equal(vclock.get(node), undefined);
      assert.equal(vclock.size(), 0);
    });

    it("Should increment node in clock", function () {
      var node = "key";
      vclock.increment(node);
      assert.equal(vclock.size(), 1);
      assert.equal(vclock.getCount(node), 1);

      vclock.increment(node);
      assert.equal(vclock.size(), 1);
      assert.equal(vclock.getCount(node), 2);
    });

    it("Should get value stored at a node", function () {
      var node = "key";
      vclock.increment(node);
      assert.equal(vclock.get(node).count, 1);
    });

    it("Should get count stored at a node", function () {
      var node = "key";
      vclock.increment(node);
      assert.equal(vclock.getCount(node), 1);
    });

    it("Should get insert timestamp stored at a node", function () {
      var node = "key";
      var date = Date.now();
      vclock._vector[node] = {count: 1, insert: date};
      assert.equal(vclock.getInsert(node), date);
    });

    it("Should get timestamp stored at a node", function () {
      var node = "key";
      var date = Date.now();
      vclock._vector[node] = {count: 1, time: date};
      assert.equal(vclock.getTimestamp(node), date);
    });

    it("Should return if clock has node or not", function () {
      var node = "key";
      assert.equal(vclock.has(node), false);
      vclock.insert(node);
      assert.equal(vclock.has(node), true);
    });

    it("Should not merge clocks if input is empty", function () {
      var node = "key";
      vclock.insert(node);
      var v2 = new VectorClock();
      vclock.merge(v2);
      assert.equal(vclock.size(), 1);
    });

    it("Should clone input clock if local vector is empty", function () {
      var node = "key";
      var v2 = new VectorClock();
      v2.insert(node);
      vclock.merge(v2);
      assert.equal(vclock.size(), 1);
      assert.deepEqual(vclock._vector, v2._vector);
    });

    it("Should merge two clocks, extending with nonexistant values", function () {
      var node = "key",
          node2 = "key2";
      var v2 = new VectorClock();
      vclock.insert(node);
      v2.insert(node2);
      vclock.merge(v2);
      assert.equal(vclock.size(), 2);
      assert.deepEqual(vclock.get(node2), v2.get(node2));
    });

    it("Should merge two clocks, prioritizing higher counts", function () {
      var node = "key";
      var v2 = new VectorClock();
      vclock.update(node, 1);
      v2.update(node, 2);
      vclock.merge(v2);
      assert.equal(vclock.size(), 1);
      assert.deepEqual(vclock.get(node), v2.get(node));

      vclock.update(node, 3, Date.now());
      v2.update(node, 1, Date.now()-5);
      vclock.merge(v2);
      assert.equal(vclock.size(), 1);
      assert.equal(vclock.getCount(node), 3);
      assert.notEqual(vclock.getTimestamp(node), v2.getTimestamp(node));
    });

    it("Should merge two clocks, prioritizing timestamp after count", function () {
      var node = "key";
      var v2 = new VectorClock();
      vclock.update(node, 1, Date.now()-5);
      v2.update(node, 1, Date.now());
      vclock.merge(v2);
      assert.equal(vclock.size(), 1);
      assert.deepEqual(vclock.get(node), v2.get(node));

      vclock.update(node, 1, Date.now());
      v2.update(node, 1, Date.now()-5);
      vclock.merge(v2);
      assert.equal(vclock.size(), 1);
      assert.equal(vclock.getCount(node), 1);
      assert.notEqual(vclock.getTimestamp(node), v2.getTimestamp(node));
    });

    it("Should descend from trivial clock", function () {
      var node = "key";
      var v2 = new VectorClock();
      vclock.insert(node);
      assert.equal(vclock.descends(v2), true);
    });

    it("Should not descend from input clock unless input is trivial", function () {
      var node = "key";
      var v2 = new VectorClock();
      v2.insert(node);
      assert.equal(vclock.descends(v2), false);
    });

    it("Should not descend from clock if one element is newer in input", function () {
      var node = "key";
      var v2 = new VectorClock();
      vclock.update(node, 1);
      v2.update(node, 2);
      assert.equal(vclock.descends(v2), false);
    });

    it("Should not descend from clock if one element missing in clock", function () {
      var node = "key",
          node2 = "key2";
      var v2 = new VectorClock();
      vclock.update(node, 1);
      v2.update(node2, 1);
      assert.equal(vclock.descends(v2), false);
    });

    it("Should descend from clock if all elements older in input", function () {
      var node = "key";
      var v2 = new VectorClock();
      vclock.update(node, 2);
      v2.update(node, 1);
      assert.equal(vclock.descends(v2), true);
    });

    it("Should return false if two vector clocks don't have same size", function () {
      var v2 = new VectorClock("foo", 1);
      assert.notOk(vclock.equals(v2));
    });

    it("Should return false if two vector clocks have different nodes", function () {
      var v2 = new VectorClock("foo", 1);
      vclock.increment("bar");
      assert.notOk(vclock.equals(v2));
    });

    it("Should return true if two vector clocks equal each other", function () {
      vclock.increment("foo");
      var v2 = (new VectorClock()).fromJSON(vclock.toJSON(true));
      assert.ok(vclock.equals(v2));
    });

    it("Should not trim clock if too small", function () {
      var opts = {
        lowerBound: 2
      };
      var threshold = Date.now();
      var node = "key";
      vclock.update(node, 1);
      vclock.trim(threshold, opts);
      assert.equal(vclock.size(), 1);
    });

    it("Should not trim clock if oldest element too young", function () {
      var opts = {
        lowerBound: 1,
        youngBound: 10
      };
      var threshold = Date.now();
      var node = "key",
          node2 = "key2";
      vclock.update(node, 1, threshold-5);
      vclock.update(node2, 1, threshold-5);
      vclock.trim(threshold, opts);
      assert.equal(vclock.size(), 2);
    });

    it("Should trim clock if too large", function () {
      var opts = {
        lowerBound: 1,
        youngBound: 10,
        upperBound: 1,
        oldBound: 15
      };
      var threshold = Date.now();
      var node = "key",
          node2 = "key2";
      vclock.update(node, 1, threshold-11);
      vclock.update(node2, 1, threshold-12);
      vclock.trim(threshold, opts);
      assert.equal(vclock.size(), 1);
      assert.ok(vclock.get(node));
    });

    it("Should trim clock if too old", function () {
      var opts = {
        lowerBound: 1,
        youngBound: 10,
        upperBound: 2,
        oldBound: 15
      };
      var threshold = Date.now();
      var node = "key",
          node2 = "key2";
      vclock.update(node, 1, threshold-11);
      vclock.update(node2, 1, threshold-16);
      vclock.trim(threshold, opts);
      assert.equal(vclock.size(), 1);
      assert.ok(vclock.get(node));
    });

    it("Should return nodes in clock", function () {
      vclock.insert("node");
      vclock.insert("node2");
      assert.lengthOf(_.xor(vclock.nodes(), ["node", "node2"]), 0);
    });

    it("Should convert clock into json", function () {
      vclock.insert("node");
      assert.deepEqual(vclock.toJSON(), vclock._vector);
      assert.deepEqual(vclock.toJSON(true), vclock._vector);
    });

    it("Should convert json to vclock", function () {
      var vector = {
        foo: {
          count: 1,
          time: 0
        }
      };
      vclock.fromJSON(vector);
      assert.equal(vclock.size(), 1);
      assert.deepEqual(vclock._vector, vector);
    });
  });
};

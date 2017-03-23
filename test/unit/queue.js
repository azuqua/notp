var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Queue unit tests", function () {
    var Queue = lib.queue,
        queue;

    beforeEach(function () {
      queue = new Queue();
    });

    it("Should construct a queue", function () {
      assert.lengthOf(queue._front, 0);
      assert.lengthOf(queue._back, 0);
    });

    it("Should enqueue value", function () {
      queue.enqueue("value");
      assert.deepEqual(queue._back, ["value"]);
      queue.enqueue("value2");
      assert.deepEqual(queue._back, ["value", "value2"]);
    });

    it("Should dequeue value", function () {
      queue.enqueue("value");
      queue.enqueue("value2");
      var val = queue.dequeue();
      assert.equal(val, "value");
      queue.enqueue("value3");
      val = queue.dequeue();
      assert.equal(val, "value2");
      assert.lengthOf(queue._front, 0);
      assert.lengthOf(queue._back, 1);
    });

    it("Should flush queue", function () {
      var vals = ["foo", "bar", "baz"];
      vals.forEach((val) => {
        queue.enqueue(val);
      });
      var out = queue.flush();
      assert.deepEqual(out, vals);
      assert.lengthOf(queue._front, 0);
      assert.lengthOf(queue._back, 0);
    });

    it("Should return queue size", function () {
      assert.equal(queue.size(), 0);
      queue.enqueue("foo");
      assert.equal(queue.size(), 1);
    });

    it("Should return first entry when peeking", function () {
      queue.enqueue("foo");
      assert.equal(queue.peek(), "foo");
      queue.dequeue();
      assert.equal(queue.peek(), undefined);
    });
  });
};

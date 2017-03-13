var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Utils unit tests", function () {
    var utils = lib.utils;

    it("Should return map values", () => {
      var m = new Map([["foo", "bar"]]);
      assert.deepEqual(utils.mapValues(m), ["bar"]);
    });

    it("Should make an object out of an error", () => {
      var err = _.extend(new Error("foo"), {hello: "world"});
      var old = process.env.NODE_ENV;
      process.env.NODE_ENV = "production";
      assert.deepEqual(utils.errorToObject(err), {
        message: "foo",
        hello: "world",
        _error: true
      });

      process.env.NODE_ENV = old;
      assert.deepEqual(_.omit(utils.errorToObject(err), "stack"), {
        message: "foo",
        hello: "world",
        _error: true
      });

      assert.deepEqual(utils.errorToObject({message: "foo"}), {
        message: "foo",
        _error: true
      });
    });

    it("Should scan iterator, consuming all values", () => {
      var m = new Map([["foo", "bar"]]);
      var out = utils.scanIterator(m.entries(), 2);
      assert.deepEqual(out.values, [["foo", "bar"]]);
      assert.equal(out.iterator.next().done, true);
    });

    it("Should scan iterator, consuming exact amount of values", () => {
      var m = new Map([["foo", "bar"]]);
      var out = utils.scanIterator(m.entries(), 1);
      assert.deepEqual(out.values, [["foo", "bar"]]);
      assert.equal(out.iterator.next().done, true);
    });

    it("Should safely parse JSON", () => {
      var val = JSON.stringify("foo");
      var out = utils.safeParse(val);
      assert.equal(out, "foo");

      val = "foo";
      out = utils.safeParse(val);
      assert.ok(out instanceof Error);
    });
  });
};

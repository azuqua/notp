var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("TableTerm unit tests", function () {
    var TableTerm = lib.table_term;
    var term;

    beforeEach(function () {
      term = new TableTerm("foo", "bar", "baz", "bah");
    });

    it("Should get/set value on term", function () {
      assert.equal(term.value(), term._value);
      term.value("a");
      assert.equal(term.value(), "a");
    });

    it("Should get/set state on term", function () {
      assert.equal(term.state(), "bar");
      term.state("b");
      assert.equal(term.state(), "b");
    });

    it("Should get/set vclock on term", function () {
      assert.equal(term.vclock(), "baz");
      term.vclock("c");
      assert.equal(term.vclock(), "c");
    });

    it("Should get/set actor on term", function () {
      assert.equal(term.actor(), "bah");
      term.actor("d");
      assert.equal(term.actor(), "d");
    });
  });
};

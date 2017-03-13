var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("TableTerm unit tests", function () {
    var TableTerm = lib.table_term;
    var VectorClock = lib.vclock;
    var term, vclock;

    beforeEach(function () {
      vclock = new VectorClock();
      term = new TableTerm("foo", "bar", vclock, "bah");
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
      assert.deepEqual(term.vclock(), vclock);
      term.vclock("c");
      assert.equal(term.vclock(), "c");
    });

    it("Should get/set actor on term", function () {
      assert.equal(term.actor(), "bah");
      term.actor("d");
      assert.equal(term.actor(), "d");
    });

    it("Should convert to JSON", function () {
      var out = term.toJSON();
      assert.deepEqual(out, {
        value: term.value(),
        state: term.state(),
        vclock: term.vclock().toJSON(true),
        actor: term.actor()
      });

      out = term.toJSON(true);
      assert.deepEqual(out, {
        value: term.value(),
        state: term.state(),
        vclock: term.vclock().toJSON(true),
        actor: term.actor()
      });
    });

    it("Should initialize from JSON", function () {
      var nterm = (new TableTerm()).fromJSON(term.toJSON());
      assert.deepEqual(nterm, term);
    });

    it("Should check valid JSON", function () {
      [
        "foo",
        {value: undefined},
        {value: "foo", state: 1},
        {value: "foo", state: "create", actor: 1},
        {value: "foo", state: "create", actor: "bar", vclock: undefined}
      ].forEach((val) => {
        assert.notOk(TableTerm.validJSON(val));
      });
    });
  });
};

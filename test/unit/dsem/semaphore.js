var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Semaphore unit tests", function () {
    var Semaphore = lib.dsem.Semaphore;
    var sem,
        id = "id",
        size = 3,
        timeouts = new Map();
    beforeEach(function () {
      sem = new Semaphore(id, size, timeouts);
    });

    it("Should construct a semaphore", function () {
      assert.equal(sem._id, id);
      assert.equal(sem._size, size);
      assert.ok(_.isEqual(sem._timeouts, timeouts));

      sem = new Semaphore("id2", 5, timeouts);
      assert.equal(sem._id, "id2");
      assert.equal(sem._size, 5);
      assert.ok(_.isEqual(sem._timeouts, timeouts));
    });

    it("Should get/set the id of a semaphore", function () {
      assert.equal(sem.id(), id);
      sem.id("id2");
      assert.equal(sem.id(), "id2");
    });

    it("Should get/set the type of a semaphore", function () {
      assert.equal(sem.size(), size);
      sem.size(5);
      assert.equal(sem.size(), 5);
    });

    it("Should get/set the timeouts of a semaphore", function () {
      assert.ok(_.isEqual(sem.timeouts(), timeouts));
      sem.timeouts(new Map([["foo", "bar"]]));
      assert.ok(_.isEqual(sem.timeouts(), new Map([["foo", "bar"]])));
    });
  });
};

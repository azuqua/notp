var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Lock unit tests", function () {
    var Lock = lib.dlm.Lock;
    var lock,
        type = "read",
        id = "id",
        timeout = new Map();
    beforeEach(function () {
      lock = new Lock(type, id, timeout);
    });

    it("Should construct a lock", function () {
      assert.equal(lock._id, id);
      assert.equal(lock._type, type);
      assert.ok(_.isEqual(lock._timeout, timeout));

      lock = new Lock("write", "id2", timeout);
      assert.equal(lock._id, "id2");
      assert.equal(lock._type, "write");
      assert.ok(_.isEqual(lock._timeout, timeout));
    });

    it("Should get/set the id of a lock", function () {
      assert.equal(lock.id(), id);
      lock.id("id2");
      assert.equal(lock.id(), "id2");
    });

    it("Should get/set the type of a lock", function () {
      assert.equal(lock.type(), type);
      lock.type("write");
      assert.equal(lock.type(), "write");
    });

    it("Should get/set the timeout of a lock", function () {
      assert.ok(_.isEqual(lock.timeout(), timeout));
      lock.timeout(8001);
      assert.equal(lock.timeout(), 8001);
    });
  });
};

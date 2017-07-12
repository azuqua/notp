var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  var MTable = lib.mtable;

  describe("MTable unit tests", function () {
    var mtable;
    beforeEach(function (done) {
      mtable = new MTable({path: "./data"});
      async.nextTick(done);
    });

    it("Should construct a MTable", function () {
      assert.ok(_.isEqual(mtable._table, new Map()));
    });

    it("Should start mtable instance", function () {
      mtable.start("foo");
      assert.isString(mtable._id);
    });

    it("Should stop mtable instance", function (done) {
      mtable.start("foo");
      mtable.stop(() => {
        assert.equal(mtable._id, null);
        done();
      });
    });

    it("Should check if mtable instance is idle", function () {
      assert.equal(mtable.idle(), true);
    });

    it("Should get value in table", function () {
      mtable._table.set("key", "val");
      assert.equal(mtable.get("key"), "val");
    });

    it("Should smember value in table", function () {
      mtable.sset("key", "val");
      assert.equal(mtable.smember("key", "val"), true);

      mtable.set("key", "val");
      assert.throws(_.partial(mtable.smember, "key", "val").bind(mtable));
    });

    it("Should hget value in table", function () {
      mtable.hset("key", "hkey", "val");
      assert.equal(mtable.hget("key", "hkey"), "val");

      mtable.set("key", "val");
      assert.throws(_.partial(mtable.hget, "key", "hkey").bind(mtable));
    });

    it("Should set value in table", function () {
      mtable.set("key", "val");
      assert.equal(mtable.get("key"), "val");
    });

    it("Should sset value in table", function () {
      mtable.sset("key", "val");
      assert.equal(mtable.smember("key", "val"), true);

      mtable.set("key", "val");
      assert.throws(_.partial(mtable.sset, "key", "val").bind(mtable));
    });

    it("Should hset value in table", function () {
      mtable.hset("key", "hkey", "val");
      assert.equal(mtable.hget("key", "hkey"), "val");

      mtable.set("key", "val");
      assert.throws(_.partial(mtable.hset, "key", "hkey", "val").bind(mtable));
    });

    it("Should del value in table", function () {
      mtable.set("key", "val");
      mtable.del("key");
      assert.notOk(mtable.get("key"));
    });

    it("Should sdel value in table", function () {
      mtable.sset("key", "val");
      mtable.sset("key", "val2");
      mtable.sdel("key", "val");
      assert.equal(mtable.smember("key", "val"), false);
      assert.equal(mtable.smember("key", "val2"), true);
      mtable.sdel("key", "val2");
      assert.equal(mtable.smember("key", "val2"), false);

      mtable.set("key", "val");
      assert.throws(_.partial(mtable.sdel, "key", "val").bind(mtable));
    });

    it("Should hdel value in table", function () {
      mtable.hset("key", "hkey", "val");
      mtable.hset("key", "hkey2", "val2");
      mtable.hdel("key", "hkey");
      assert.notOk(mtable.hget("key", "hkey"));
      assert.equal(mtable.hget("key", "hkey2"), "val2");
      mtable.hdel("key", "hkey2");
      assert.notOk(mtable.hget("key", "hkey2"));

      mtable.set("key", "val");
      assert.throws(_.partial(mtable.hdel, "key", "hkey").bind(mtable));
    });

    it("Should run an async forEach over table", function (done) {
      mtable.set("key", "val");
      mtable.set("key2", "val2");
      const memo = {};
      mtable.forEach((key, val, next) => {
        memo[key] = val;
        next();
      }, (err) => {
        assert.notOk(err);
        assert.deepEqual(memo, {
          key: "val",
          key2: "val2"
        });
        done();
      });
    });

    it("Should run sync forEach over table", function () {
      mtable.set("key", "val");
      mtable.set("key2", "val2");
      const memo = {};
      mtable.forEachSync((key, val) => {
        memo[key] = val;
      });
      assert.deepEqual(memo, {
        key: "val",
        key2: "val2"
      });
    });
  });

  describe("MTable static unit tests", function () {
    it("Should return invalid type error", function () {
      var error = MTable.invalidTypeError("command", "key", "type");
      assert.ok(error instanceof Error);
      assert.equal(error.type, "INVALID_TYPE");
    });
  });
};

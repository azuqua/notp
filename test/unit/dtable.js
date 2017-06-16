var _ = require("lodash"),
    async = require("async"),
    sinon = require("sinon"),
    stream = require("stream"),
    fs = require("fs"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  var DTable = lib.dtable;
  var consts = lib.consts;

  describe("DTable unit tests", function () {
    var dtable;
    beforeEach(function (done) {
      dtable = new DTable({path: "./data"});
      async.nextTick(done);
    });

    it("Should construct a DTabel", function () {
      assert.equal(dtable._autoSave, consts.dtableOpts.autoSave);
      assert.equal(dtable._writeCount, 0);
      assert.equal(dtable._writeThreshold, consts.dtableOpts.writeThreshold);
      assert.ok(_.isEqual(dtable._table, new Map()));
      assert.equal(dtable._idleTicks, 0);
      assert.equal(dtable._idleTickMax, consts.dtableOpts.autoSave/1000);
      assert.equal(dtable._fsyncInterval, consts.dtableOpts.fsyncInterval);
      assert.equal(dtable._queue.size(), 0);

      var out;
      try {
        dtable = new DTable();
      } catch (e) {
        out = e;
      }
      assert.ok(out instanceof Error);
    });

    it("Should start dtable instance", function (done) {
      dtable.start("foo");
      dtable.once("open", () => {
        assert.isString(dtable._id);
        assert.isNumber(dtable._fd);
        assert.ok(dtable._idleInterval);
        assert.ok(dtable._syncInterval);
        assert.ok(dtable._fstream);
        done();
      });
    });

    it("Should stop dtable instance", function (done) {
      dtable.start("foo");
      dtable.stop(() => {
        assert.equal(dtable._id, null);
        assert.equal(dtable._fstream, null);
        assert.equal(dtable._syncInterval, null);
        assert.equal(dtable._idleInterval, null);
        done();
      });
    });

    it("Should stop dtable instance once fd open", function (done) {
      dtable.start("foo");
      dtable.once("open", () => {
        dtable.stop(() => {
          assert.equal(dtable._id, null);
          assert.equal(dtable._fstream, null);
          assert.equal(dtable._syncInterval, null);
          assert.equal(dtable._idleInterval, null);
          done();
        });
      });
    });

    it("Should stop dtable instance, wait for idle", function (done) {
      dtable.start("foo");
      dtable.once("open", () => {
        dtable._flushing = true;
        dtable.stop(() => {
          assert.equal(dtable._id, null);
          assert.equal(dtable._fstream, null);
          assert.equal(dtable._syncInterval, null);
          assert.equal(dtable._idleInterval, null);
          done();
        });
        dtable._flushing = false;
        dtable.emit("idle");
      });
    });

    it("Should load dtable instance", function (done) {
      dtable.load((err) => {
        assert.notOk(err);
        done();
      });
    });

    it("Should check if dtable instance is idle", function () {
      assert.equal(dtable.idle(), true);
      dtable._flushing = true;
      assert.equal(dtable.idle(), false);
      dtable._flushing = false;
    });

    it("Should get value in table", function () {
      dtable._table.set("key", "val");
      assert.equal(dtable.get("key"), "val");
    });

    it("Should smember value in table", function () {
      dtable.sset("key", "val");
      assert.equal(dtable.smember("key", "val"), true);

      dtable.set("key", "val");
      assert.throws(_.partial(dtable.smember, "key", "val").bind(dtable));
    });

    it("Should hget value in table", function () {
      dtable.hset("key", "hkey", "val");
      assert.equal(dtable.hget("key", "hkey"), "val");

      dtable.set("key", "val");
      assert.throws(_.partial(dtable.hget, "key", "hkey").bind(dtable));
    });

    it("Should set value in table", function () {
      dtable.set("key", "val");
      assert.equal(dtable.get("key"), "val");
    });

    it("Should sset value in table", function () {
      dtable.sset("key", "val");
      assert.equal(dtable.smember("key", "val"), true);

      dtable.set("key", "val");
      assert.throws(_.partial(dtable.sset, "key", "val").bind(dtable));
    });

    it("Should hset value in table", function () {
      dtable.hset("key", "hkey", "val");
      assert.equal(dtable.hget("key", "hkey"), "val");

      dtable.set("key", "val");
      assert.throws(_.partial(dtable.hset, "key", "hkey", "val").bind(dtable));
    });

    it("Should del value in table", function () {
      dtable.set("key", "val");
      dtable.del("key");
      assert.notOk(dtable.get("key"));
    });

    it("Should sdel value in table", function () {
      dtable.sset("key", "val");
      dtable.sset("key", "val2");
      dtable.sdel("key", "val");
      assert.equal(dtable.smember("key", "val"), false);
      assert.equal(dtable.smember("key", "val2"), true);
      dtable.sdel("key", "val2");
      assert.equal(dtable.smember("key", "val2"), false);

      dtable.set("key", "val");
      assert.throws(_.partial(dtable.sdel, "key", "val").bind(dtable));
    });

    it("Should hdel value in table", function () {
      dtable.hset("key", "hkey", "val");
      dtable.hset("key", "hkey2", "val2");
      dtable.hdel("key", "hkey");
      assert.notOk(dtable.hget("key", "hkey"));
      assert.equal(dtable.hget("key", "hkey2"), "val2");
      dtable.hdel("key", "hkey2");
      assert.notOk(dtable.hget("key", "hkey2"));

      dtable.set("key", "val");
      assert.throws(_.partial(dtable.hdel, "key", "hkey").bind(dtable));
    });

    it("Should run an async forEach over table", function (done) {
      dtable.set("key", "val");
      dtable.set("key2", "val2");
      const memo = {};
      dtable.forEach((key, val, next) => {
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
      dtable.set("key", "val");
      dtable.set("key2", "val2");
      const memo = {};
      dtable.forEachSync((key, val) => {
        memo[key] = val;
      });
      assert.deepEqual(memo, {
        key: "val",
        key2: "val2"
      });
    });

    it("Should setup idle flush interval", function (done) {
      dtable._idleTickInterval = 1;
      dtable._setupIdleFlushInterval();
      setTimeout(() => {
        assert.equal(dtable._idleTicks, 1);
        clearInterval(dtable._idleInterval);
        done();
      }, 1);
    });

    it("Should setup idle flush interval, execute flush", function (done) {
      dtable._idleTickInterval = 1;
      dtable._idleTickMax = 1;
      sinon.stub(dtable, "_flush");
      dtable._setupIdleFlushInterval();
      setTimeout(() => {
        assert.equal(dtable._idleTicks, 0);
        assert.ok(dtable._flush.called);
        dtable._flush.restore();
        dtable._fd = null;
        clearInterval(dtable._idleInterval);
        async.nextTick(done);
      }, 1);
    });

    it("Should setup AOF sync interval", function (done) {
      sinon.stub(fs, "createWriteStream", () => {
        var pstream = new stream.PassThrough();
        setTimeout(() => {
          pstream.emit("open", 1);
        }, 2);
        return pstream;
      });
      sinon.stub(fs, "fsync", (fd, cb) => {
        return cb();
      });
      dtable._fsyncInterval = 1;
      dtable._setupAOFSyncInterval();
      assert.ok(dtable._fstream);
      assert.ok(dtable._syncInterval);
      dtable._fstream.once("open", (fd) => {
        assert.equal(dtable._fd, fd);
        setTimeout(() => {
          assert.ok(fs.fsync.called);
          clearInterval(dtable._syncInterval);
          fs.createWriteStream.restore();
          fs.fsync.restore();
          done();
        }, 1);
      });
    });

    it("Should update write count", function () {
      dtable._updateWriteCount();
      assert.equal(dtable._idleTicks, 0);
      assert.equal(dtable._writeCount, 1);
      dtable._writeThreshold = 2;
      sinon.stub(dtable, "_flush");
      dtable._updateWriteCount();
      assert.equal(dtable._writeCount, 0);
      assert.ok(dtable._flush.called);
      dtable._flush.restore();
    });

    it("Should flush state to disk", function (done) {
      dtable._setupAOFSyncInterval();
      sinon.stub(dtable, "_setupAOFSyncInterval");

      dtable._flush();
      assert.notOk(dtable._setupAOFSyncInterval.called);

      dtable._flushing = true;
      dtable._flush();
      assert.notOk(dtable._setupAOFSyncInterval.called);
      dtable._flushing = false;

      dtable.once("open", () => {
        dtable.once("idle", () => {
          assert.ok(dtable._setupAOFSyncInterval);
          dtable._setupAOFSyncInterval.restore();
          fs.unlink(dtable._path, _.partial(async.nextTick, done));
        });
        dtable.once("close", () => {
          assert.equal(dtable._fd, null);
        });
        dtable._flush();
      });
    });

    it("Should flush AOF files to disk", function (done) {
      fs.writeFile(dtable._aofPath, "", (err) => {
        assert.notOk(err);
        dtable._flushAOF((err) => {
          assert.notOk(err);
          fs.unlink(dtable._tmpAOFPath, done);
        });
      });
    });

    it("Should fail flushing AOF files to disk", function (done) {
      fs.writeFile(dtable._aofPath, "", (err) => {
        assert.notOk(err);
        sinon.stub(fs, "createWriteStream", () => {
          var pstream = new stream.PassThrough();
          async.nextTick(() => {
            pstream.emit("error", new Error("foo"));
          });
          return pstream;
        });
        dtable._flushAOF((err) => {
          assert.ok(err);
          fs.createWriteStream.restore();
          fs.unlink(dtable._aofPath, _.partial(async.nextTick, done));
        });
      });
    });

    it("Should flush snapshot of table to disk", function (done) {
      var out = new Map();
      dtable.set("foo", "bar");
      sinon.stub(fs, "createWriteStream", () => {
        var pstream = new stream.PassThrough();
        pstream.on("data", (data) => {
          data = JSON.parse(data);
          out.set(data.key, DTable.decodeValue(data.value));
        });
        pstream.once("end", () => {
          pstream.emit("close");
        });
        return pstream;
      });
      dtable._flushTable({
        foo: "bar"
      }, (err) => {
        assert.notOk(err);
        assert.ok(_.isEqual(out, dtable._table));
        fs.createWriteStream.restore();
        async.nextTick(done);
      });
    });

    it("Should fail flushing snapshot of table to disk", function (done) {
      sinon.stub(fs, "createWriteStream", () => {
        var pstream = new stream.PassThrough();
        async.nextTick(() => {
          pstream.emit("error", new Error("foo"));
        });
        return pstream;
      });
      dtable._flushTable({
        foo: "bar"
      }, (err) => {
        assert.ok(err);
        fs.createWriteStream.restore();
        async.nextTick(done);
      });
    });

    it("Should write action to log", function (done) {
      dtable._writeToLog("command", "foo", "bar", "baz");
      assert.deepEqual(dtable._queue.dequeue(), {op: "command", args: ["foo", "bar", "baz"]});

      dtable._fd = 1;
      dtable._fstream = new stream.PassThrough();
      dtable._fstream.once("data", (data) => {
        data = JSON.parse(data);
        assert.deepEqual(data, {
          op: "command",
          args: [
            {type: "string", data: "foo"},
            {type: "string", data: "bar"},
            {type: "string", data: "baz"}
          ]
        });
        async.nextTick(done);
      });
      dtable._writeToLog("command", "foo", "bar", "baz");
    });

    it("Should fail to load snapshot if disk read fails", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(() => {
          cb(new Error("foo"));
        });
      });
      dtable._loadState((err) => {
        assert.ok(err);
        fs.stat.restore();
        done();
      });
    });

    it("Should skip loading snapshot if file doesn't exist", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(_.partial(cb, _.extend(new Error("foo"), {code: "ENOENT"})));
      });
      dtable._loadState((err) => {
        assert.notOk(err);
        assert.equal(dtable._table.size, 0);
        fs.stat.restore();
        done();
      });
    });

    it("Should load snapshot from disk", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(cb);
      });
      sinon.stub(fs, "createReadStream", () => {
        var pstream = new stream.PassThrough();
        async.nextTick(() => {
          pstream.write(JSON.stringify({
            key: "key",
            value: {type: "string", data: "val"}
          }) + "\n");
          pstream.end();
        });
        return pstream;
      });
      dtable._loadState((err) => {
        assert.notOk(err);
        assert.ok(_.isEqual(dtable._table, new Map([["key", "val"]])));
        fs.stat.restore();
        fs.createReadStream.restore();
        done();
      });
    });

    it("Should fail loading snapshot from disk if rstream emits error", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(cb);
      });
      sinon.stub(fs, "createReadStream", () => {
        var pstream = new stream.PassThrough();
        async.nextTick(() => {
          pstream.emit("error", new Error("foo"));
        });
        return pstream;
      });
      dtable._loadState((err) => {
        assert.ok(err);
        fs.stat.restore();
        fs.createReadStream.restore();
        done();
      });
    });

    it("Should fail to load AOF if disk read fails", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(() => {
          cb(new Error("foo"));
        });
      });
      dtable._loadAOF(dtable._aofPath, (err) => {
        assert.ok(err);
        fs.stat.restore();
        done();
      });
    });

    it("Should skip loading AOF if file doesn't exist", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(_.partial(cb, _.extend(new Error("foo"), {code: "ENOENT"})));
      });
      dtable._loadAOF(dtable._aofPath, (err) => {
        assert.notOk(err);
        assert.equal(dtable._table.size, 0);
        fs.stat.restore();
        done();
      });
    });

    it("Should load AOF from disk", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(cb);
      });
      sinon.stub(fs, "createReadStream", () => {
        var pstream = new stream.PassThrough();
        async.nextTick(() => {
          pstream.write(JSON.stringify({
            op: "set",
            args: [{type: "string", data: "key"}, {type: "string", data: "val"}]
          }) + "\n");
          pstream.end();
        });
        return pstream;
      });
      dtable._loadAOF(dtable._aofPath, (err) => {
        assert.notOk(err);
        assert.ok(_.isEqual(dtable._table, new Map([["key", "val"]])));
        fs.stat.restore();
        fs.createReadStream.restore();
        done();
      });
    });

    it("Should fail loading AOF from disk if rstream emits error", function (done) {
      sinon.stub(fs, "stat", (path, cb) => {
        async.nextTick(cb);
      });
      sinon.stub(fs, "createReadStream", () => {
        var pstream = new stream.PassThrough();
        async.nextTick(() => {
          pstream.emit("error", new Error("foo"));
        });
        return pstream;
      });
      dtable._loadAOF(dtable._aofPath, (err) => {
        assert.ok(err);
        fs.stat.restore();
        fs.createReadStream.restore();
        done();
      });
    });
  });

  describe("DTable static unit tests", function () {
    it("Should return invalid type error", function () {
      var error = DTable.invalidTypeError("command", "key", "type");
      assert.ok(error instanceof Error);
      assert.equal(error.type, "INVALID_TYPE");
    });

    it("Should encode value", function () {
      var out = DTable.encodeValue(new Set(["val", "val2"]));
      assert.deepEqual(out, {
        type: "Set",
        data: ["val", "val2"]
      });

      out = DTable.encodeValue(new Map([["key", "val"]]));
      assert.deepEqual(out, {
        type: "Map",
        data: [["key", "val"]]
      });

      out = DTable.encodeValue("foobar");
      assert.deepEqual(out, {
        type: "string",
        data: "foobar"
      });
    });

    it("Should decode value", function () {
      var out = DTable.decodeValue({
        type: "Set",
        data: ["val", "val2"]
      });
      assert.deepEqual(out, new Set(["val", "val2"]));

      out = DTable.decodeValue({
        type: "Map",
        data: [["key", "val"]]
      });
      assert.deepEqual(out, new Map([["key", "val"]]));

      out = DTable.decodeValue({
        type: "object",
        data: {}
      });
      assert.deepEqual(out, {});
    });
  });
};

var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    sinon = require("sinon"),
    fs = require("fs"),
    microtime = require("microtime"),
    stream = require("stream"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("DiskTable unit tests", function () {
    var VectorClock = lib.vclock,
        CHash = lib.chash,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        DiskTable = lib.dtable,
        TableTerm = lib.table_term,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("DiskTable state tests", function () {
      var kernel,
          gossip,
          vclock,
          chash,
          table,
          opts,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function () {
        
        kernel = new NetKernel(new MockIPC(), id, host, port);
        chash = new CHash(3, 3);
        chash.insert(new Node(id, host, port));
        vclock = new VectorClock();
        gossip = new GossipRing(kernel, chash, vclock, {
          interval: 100,
          flushInterval: 100,
          flushPath: "/foo/bar",
          vclockOpts: {}
        });
      });

      beforeEach(function () {
        table = new DiskTable(kernel, gossip, {
          vclockOpts: {lowerBound: 10},
          pollOpts: {interval: 100, block: 5},
          flushOpts: {interval: 5000, path: "/foo/baz"},
          purgeMax: 5
        });
      });

      it("Should construct a table", function () {
        assert.isObject(table._flushOpts);
        assert.notOk(table._flushInterval);
        assert.notOk(table._flushCursor);
      });

      it("Should return if table is idle or not", function () {
        assert.ok(table.idle());

        var streams = new Map([["key", "value"]]);
        table.streams(streams);
        assert.notOk(table.idle());
        table.streams(new Map());

        table._pollCursor = 1;
        assert.notOk(table.idle());
        table._pollCursor = null;

        table._migrateCursor = 1;
        assert.notOk(table.idle());
        table._migrateCursor = null;

        table._flushCursor = 1;
        assert.notOk(table.idle());
        table._flushCursor = null;
      });

      it("Should not start table if already started", function () {
        table._stopped = false;
        table.start("foo");
        assert.notOk(table._flushInterval);
        table._stopped = true;
      });

      it("Should start table", function () {
        var name = "foo";
        var events = ["state", "migrate"];
        var gEvents = ["process", "leave"];
        var oldCounts = gEvents.reduce((memo, val) => {
          memo[val] = gossip.listeners(val).length;
          return memo;
        }, {});
        table.start(name);
        assert.equal(table._id, name);
        assert.lengthOf(table.kernel().listeners(name), 1);
        events.forEach((val) => {
          assert.lengthOf(table.listeners(val), 1);
        });
        gEvents.forEach((val) => {
          assert.lengthOf(gossip.listeners(val), oldCounts[val]+1);
        });
        assert.lengthOf(table.listeners("stop"), events.length + gEvents.length);
        assert.lengthOf(table.listeners("pause"), 1);
        assert.deepEqual(gossip._tables.get(name), table);
        assert.ok(table._pollInterval);
        assert.ok(table._flushInterval);
        table.stop(true);
      });

      it("Should fail to stop table if already stopped", function () {
        table._stopped = true;
        table.stop();
        assert.equal(table._stopped, true);
      });

      it("Should pause table", function () {
        var name = "foo";
        table.start(name);
        table.pause();
        assert.lengthOf(table.listeners("pause"), 0);
        assert.lengthOf(table.kernel().listeners(name), 0);
        assert.notOk(table._pollInterval);
        assert.notOk(table._flushInterval);
        [
          {cursor: "_pollCursor", next: "_nextPoll"},
          {cursor: "_migrateCursor", next: "nextMigrate"},
          {cursor: "_flushCursor"},
          {cursor: "_purgeCursor"}
        ].forEach((ent) => {
          assert.notOk(table[ent.cursor]);
          assert.notOk(table[ent.next]);
        });
        assert.equal(table._migrations.size, 0);
      });

      it("Should resume table", function () {
        var name = "foo";
        table.start(name);
        table.pause();
        table.resume();
        assert.lengthOf(table.listeners("pause"), 1);
        assert.lengthOf(table.kernel().listeners(name), 1);
        assert.ok(table._pollInterval);
        assert.ok(table._flushInterval);
      });
    });

    describe("DiskTable manipulation tests", function () {
      var kernel,
          nKernel,
          gossip,
          vclock,
          chash,
          table,
          opts,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function (done) {
        kernel = new NetKernel(new MockIPC(), id, host, port);
        nKernel = new NetKernel(new MockIPC(), "id2", host, port+1);
        async.each([kernel, nKernel], (k, next) => {
          k.start({retry: 500, maxRetries: false});
          k.once("_ready", next);
        }, done);
      });

      beforeEach(function () {
        var node = new Node(id, host, port);
        // var node2 = new Node("id2", host, port+1);
        chash = new CHash(3, 3);
        chash.insert(node);
        vclock = new VectorClock(uuid.v4(), 1);
        gossip = new GossipRing(kernel, chash, vclock, {
          interval: 100,
          wquorum: 1,
          flushPath: "/foo/bar",
          vclockOpts: {lowerBound: 10}
        });
        gossip.start("foo");
        table = new DiskTable(kernel, gossip, {
          vclockOpts: {lowerBound: 10},
          pollOpts: {interval: 100},
          flushOpts: {interval: 5000, path: "/foo/baz"},
          purgeMax: 5
        });
        table.start("bar");
        clearInterval(gossip._interval);
        clearInterval(gossip._flush);
      });

      afterEach(function (done) {
        table.stop(true);
        gossip.stop(true);
        var iter = gossip.kernel().sinks().entries();
        var val = iter.next();
        gossip.kernel().sinks().forEach((val, key) => {
          gossip.kernel().sinks().delete(key);
          val.stop();
        });
        done();
      });

      after(function () {
        kernel.stop();
        nKernel.stop();
      });

      it("Should load state from disk", function (done) {
        // load should only ever be done when stopped
        table.stop(true);
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        sinon.stub(fs, "createReadStream", () => {
          var pstream = new stream.PassThrough();
          async.nextTick(() => {
            pstream.write(JSON.stringify([
              {key: "key", value: term.toJSON(true)}
            ]));
            pstream.end();
          });
          return pstream;
        });
        table.load((err) => {
          assert.notOk(err);
          assert.ok(table.has("key"));
          assert.deepEqual(table.get("key"), term);
          fs.createReadStream.restore();
          done();
        });
      });

      it("Should error on loading state from disk when invalid JSON", function (done) {
        // load should only ever be done when stopped
        table.stop(true);
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        sinon.stub(fs, "createReadStream", () => {
          var pstream = new stream.PassThrough();
          async.nextTick(() => {
            pstream.emit("error", new Error("error"));
          });
          return pstream;
        });
        table.load((err) => {
          assert.ok(err);
          fs.createReadStream.restore();
          done();
        });
      });

      it("Should error on badly formatted table entry on disk", function (done) {
        // load should only ever be done when stopped
        table.stop(true);
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        sinon.stub(fs, "createReadStream", () => {
          var pstream = new stream.PassThrough();
          async.nextTick(() => {
            pstream.write(JSON.stringify([
              "foo"
            ]));
            pstream.end();
          });
          return pstream;
        });
        table.load((err) => {
          assert.ok(err);
          fs.createReadStream.restore();
          done();
        });
      });

      it("Should load state from disk synchronously", function () {
        // load should only ever be done when stopped
        table.stop(true);
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        sinon.stub(fs, "readFileSync", () => {
          return JSON.stringify([
            {key: "key", value: term.toJSON(true)}
          ]);
        });
        table.loadSync();
        assert.ok(table.has("key"));
        assert.deepEqual(table.get("key"), term);
        fs.readFileSync.restore();
      });

      it("Should error on loading state from disk if invalid JSON", function () {
        // load should only ever be done when stopped
        table.stop(true);
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        sinon.stub(fs, "readFileSync", () => {
          return "foo";
        });
        var out;
        try {
          out = table.loadSync();
        } catch (e) {
          out = e;
        }
        assert.ok(out instanceof Error);
        assert.notOk(table.has("key"));
        assert.equal(table._values.size, 0);
        fs.readFileSync.restore();
      });

      it("Should flush state to disk", function (done) {
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        table.set("key", term);
        sinon.stub(fs, "createWriteStream", () => {
          var pstream = new stream.PassThrough();
          var acc = Buffer.from("");
          pstream.on("data", (data) => {
            acc = Buffer.concat([acc, data]);
          });
          pstream.on("end", () => {
            assert.deepEqual(JSON.parse(acc), [
              {key: "key", value: term.toJSON(true)}
            ]);
            async.nextTick(() => {
              assert.equal(table._flushCursor, null);
              fs.createWriteStream.restore();
              done();
            });
          });
          return pstream;
        });
        table.flush();
      });

      it("Should fail to flush state to disk on error in either stream", function (done) {
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        table.set("key", term);
        sinon.stub(fs, "createWriteStream", () => {
          var pstream = new stream.PassThrough();
          async.nextTick(() => {
            pstream.emit("error", new Error("error"));
          });
          pstream.on("finish", () => {
            async.nextTick(() => {
              assert.notOk(table._flushCursor);
              fs.createWriteStream.restore();
              done();
            });
          });
          // since we're not actually using a file stream, we need to manually end the stream
          pstream.on("error", () => {
            pstream.end();
          });
          return pstream;
        });
        table.flush();
      });

      it("Should skip flushing state to disk", function () {
        table._flushCursor = 1;
        sinon.spy(table, "emit").withArgs("flush");
        table.flush();
        assert.notOk(table.emit.called);

        table._flushCursor = null;
        table._stopped = true;
        table.flush();
        assert.notOk(table.emit.called);
        table.emit.restore();
        table._stopped = false;
      });

      it("Should flush state to disk sync", function () {
        var term = new TableTerm("value", "state", new VectorClock("actor", 1), "actor");
        table.set("key", term);
        sinon.stub(fs, "writeFileSync", (path, data) => {
          assert.equal(data, JSON.stringify([
            {key: "key", value: term.toJSON(true)}
          ]));
        });
        table.flushSync();
        assert.ok(fs.writeFileSync.called);
        fs.writeFileSync.restore();
      });

      it("Should skip flushing state to disk sync", function () {
        table._flushCursor = 1;
        sinon.spy(table, "emit").withArgs("flush");
        table.flushSync();
        assert.notOk(table.emit.called);

        table._flushCursor = null;
        table._stopped = true;
        table.flushSync();
        assert.notOk(table.emit.called);
        table.emit.restore();
        table._stopped = false;
      });
    });

    describe("DiskTable static tests", function () {
      it("Should check valid disk entry JSON", function () {
        [
          "foo",
          {key: 1},
          {key: "key", value: "value"},
        ].forEach((ent) => {
          assert.notOk(DiskTable.validDiskEntry(ent));
        });


        var val = {key: "key", value: {value: "val", state: "state", vclock: {}, actor: "actor"}};
        assert.ok(DiskTable.validDiskEntry(val));
      });
    });
  });
};

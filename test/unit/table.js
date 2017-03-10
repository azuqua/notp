var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    stream = require("stream"),
    sinon = require("sinon"),
    fs = require("fs"),
    microtime = require("microtime"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("StateTable unit tests", function () {
    var VectorClock = lib.vclock,
        CHash = lib.chash,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        StateTable = lib.table,
        TableTerm = lib.table_term,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("StateTable state tests", function () {
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
        table = new StateTable(kernel, gossip, {
          vclockOpts: {lowerBound: 10},
          pollOpts: {interval: 100, block: 5},
          flushOpts: {interval: 100},
          purgeMax: 5
        });
      });

      it("Should construct a table", function () {
        assert.deepEqual(table._kernel, kernel);
        assert.deepEqual(table._gossip, gossip);
        assert.isObject(table._vclockOpts);
        assert.isObject(table._pollOpts);
        assert.isObject(table._flushOpts);
        assert.isNumber(table._purgeMax);
        assert(gossip._streams instanceof Map);
        assert.equal(gossip._streams.size, 0);
      });

      it("Should grab kernel of table", function () {
        assert.deepEqual(table.kernel(), kernel);
      });

      it("Should set kernel of table", function () {
        var kernel2 = new NetKernel(new MockIPC(), "id2", host, port+1);
        table.kernel(kernel2);
        assert.deepEqual(table._kernel, kernel2);
      });

      it("Should grab active streams of table", function () {
        assert(table.streams() instanceof Map);
        assert.equal(table.streams().size, 0);
      });

      it("Should set active streams of table", function () {
        var streams = new Map([["key", "value"]]);
        table.streams(streams);
        assert(table.streams() instanceof Map);
        assert.equal(table.streams().size, 1);
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
        // +1 for the kernel
        assert.lengthOf(table.listeners("stop"), events.length + gEvents.length + 1);
        assert.deepEqual(gossip._tables.get(name), table);
        assert.ok(table._pollInterval);
        assert.ok(table._flushInterval);
        table.stop(true);
      });

      it("Should stop table", function (done) {
        var name = "foo";
        table.start(name);
        table.once("stop", () => {
          assert.equal(table._id, "foo");
          assert.equal(table._streams.size, 0);
          var events = ["state", "migrate"];
          events.forEach((val) => {
            assert.lengthOf(table.listeners(val), 0);
          });
          assert.lengthOf(table.listeners("stop"), 0);
          assert.lengthOf(table.kernel().listeners(name), 0);
          done();
        });
        table.stop();
      });

      it("Should stop table, waiting for idle state", function (done) {
        var name = "foo";
        table.start(name);
        table.once("stop", () => {
          assert.equal(table._id, "foo");
          assert.equal(gossip._streams.size, 0);
          var events = ["state", "migrate"];
          events.forEach((val) => {
            assert.lengthOf(table.listeners(val), 0);
          });
          assert.lengthOf(table.listeners("stop"), 0);
          assert.lengthOf(table.kernel().listeners(name), 0);
          done();
        });
        table._streams.set("foo", "bar");
        table.stop();
        table._streams.clear();
        table.emit("idle");
      });

      it("Should forcefully stop table", function (done) {
        var name = "foo";
        table.start(name);
        table.once("stop", () => {
          assert.equal(table._id, "foo");
          assert.equal(gossip._streams.size, 0);
          var events = ["state", "migrate"];
          events.forEach((val) => {
            assert.lengthOf(table.listeners(val), 0);
          });
          assert.lengthOf(table.listeners("stop"), 0);
          assert.lengthOf(table.kernel().listeners(name), 0);
          done();
        });
        table.stop(true);
      });

      it("Should stop table, clearing any timeouts associated with cursors", function (done) {
        var name = "foo";
        table.start(name);
        table.once("stop", () => {
          assert.equal(table._id, "foo");
          assert.equal(gossip._streams.size, 0);
          var events = ["state", "migrate"];
          events.forEach((val) => {
            assert.lengthOf(table.listeners(val), 0);
          });
          assert.lengthOf(table.listeners("stop"), 0);
          assert.lengthOf(table.kernel().listeners(name), 0);
          assert.equal(table._pollCursor, null);
          assert.equal(table._nextPoll, null);
          done();
        });
        table._pollCursor = "foo";
        table._nextPoll = setTimeout(() => {return "bar";}, 0);
        table.stop(true);
      });

      it("Should get gossip ring", function () {
        assert.deepEqual(table.gossip(), gossip);
      });

      it("Should set gossip ring", function () {
        table.gossip("foo");
        assert.equal(table.gossip(), "foo");
      });

      it("Should get value from table", function () {
        table._values.set("foo", "bar");
        assert.equal(table.get("foo"), "bar");
      });

      it("Should set value in table", function () {
        table.set("foo", "bar");
        assert.equal(table.get("foo"), "bar");
      });

      it("Should delete value in table", function () {
        table.set("foo", "bar");
        table.delete("foo");
        assert.notOk(table._values.has("foo"));
      });

      it("Should check if key exists in table", function () {
        assert.notOk(table.has("foo"));
        table.set("foo", "bar");
        assert.ok(table.has("foo"));
      });

      it("Should return iterator of keys", function () {
        table.set("foo", "bar");
        var keyIt = table.keys();
        var out = keyIt.next();
        assert.equal(out.value, "foo");
        assert.equal(out.done, false);
        out = keyIt.next();
        assert.equal(out.done, true);
      });

      it("Should return iterator of values", function () {
        table.set("foo", "bar");
        var valIt = table.values();
        var out = valIt.next();
        assert.equal(out.value, "bar");
        assert.equal(out.done, false);
        out = valIt.next();
        assert.equal(out.done, true);
      });

      it("Should return iterator over key/value pairs", function () {
        table.set("foo", "bar");
        var it = table.iterator();
        var out = it.next();
        assert.deepEqual(out.value, ["foo", "bar"]);
        assert.equal(out.done, false);
        out = it.next();
        assert.equal(out.done, true);
      });

      it("Should decode new job", function () {
        var buf = Buffer.from(JSON.stringify({
          event: "foo",
          data: {
            data: {
              id: "id",
              state: "update",
              value: "bar"
            },
            vclock: (new VectorClock()).toJSON(true),
            round: 0,
            actor: "baz"
          }
        }));
        var out = table.decodeJob(buf);
        assert.equal(out.event, "foo");
        out = out.data;
        assert.equal(out.id, "id");
        assert.equal(out.round, 0);
        out = out.term;
        assert.equal(out.state(), "update");
        assert.equal(out.value(), "bar");
        assert.equal(out.actor(), "baz");
        assert.ok(out.vclock().equals(new VectorClock()));
      });
    });

    describe("StateTable manipulation tests", function () {
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
        table = new StateTable(kernel, gossip, {
          vclockOpts: {lowerBound: 10},
          pollOpts: {interval: 100},
          flushOpts: {interval: 100},
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

      it("Should skip polling if stopped or cursor set", function () {
        sinon.spy(table, "emit").withArgs("poll");
        table._pollCursor = 1;
        table.poll();
        table._pollCursor = null;
        assert.notOk(table.emit.called);

        table._migrateCursor = 1;
        table.poll();
        assert.notOk(table.emit.called);
        table._migrateCursor = null;

        table._stopped = true;
        table.poll();
        assert.notOk(table.emit.called);
        table._stopped = false;
        table.emit.restore();
      });

      it("Should poll table and attempt to broadcast key/value pair based on existing vclock entries", function () {
        var term = new TableTerm({}, "create", new VectorClock("foo", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "route", (bucket, event, msg, actor, clock, n) => {
          assert.isArray(bucket);
          assert.equal(event, "state");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table._values.get("foo").vclock());
          assert.equal(n, 1);
        });
        table.poll();
        assert.ok(table.route.called);
        table.route.restore();
      });

      it("Should poll table and filter out empty vclock entries", function () {
        var term = new TableTerm({}, "destroy", new VectorClock(), "bar");
        table.set("foo", term);
        sinon.stub(table, "route", (bucket, event, msg, actor, clock, n) => {
          assert.isArray(bucket);
          assert.equal(event, "state");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table.get("foo").vclock());
          assert.equal(n, 1);
        });
        table.poll();
        assert.equal(table._values.size, 0);
        assert.notOk(table.route.called);
        table.route.restore();
      });

      it("Should poll table and filter out misplaced vclock entries", function () {
        var term = new TableTerm({}, "create", new VectorClock(), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table.get("foo").vclock());
          assert.equal(n, 1);
          cb();
        });
        sinon.spy(table, "_filterClock");
        sinon.stub(gossip, "find").returns([]);
        table.poll();
        assert.equal(table._values.size, 0);
        assert.ok(table.routeSync.called);
        assert.ok(table._filterClock.called);
        gossip.find.restore();
        table._filterClock.restore();
        table.routeSync.restore();
      });

      it("Should poll table but error out on migrate call", function () {
        var term = new TableTerm({}, "create", new VectorClock(), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table.get("foo").vclock());
          assert.equal(n, 1);
          cb(new Error("errored"));
        });
        sinon.spy(table, "_filterClock");
        sinon.stub(gossip, "find").returns([]);
        table.poll();
        assert.equal(table._values.size, 1);
        assert.ok(table.routeSync.called);
        assert.ok(table._filterClock.called);
        gossip.find.restore();
        table._filterClock.restore();
        table.routeSync.restore();
      });
    });
  });
};

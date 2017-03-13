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

        var nTable = new StateTable(kernel, gossip, {
          vclockOpts: {lowerBound: 10},
          pollOpts: {interval: 100, block: 5},
          flushOpts: {interval: 100}
        });
        assert.equal(table._purgeMax, 5);
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

      it("Should not start table if stopped", function () {
        table._stopped = false;
        table.start("foo");
        assert.equal(table._stopped, false);
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
          assert.lengthOf(table.listeners("pause"), 0);
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
          assert.lengthOf(table.listeners("pause"), 0);
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
          assert.lengthOf(table.listeners("pause"), 0);
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
          assert.lengthOf(table.listeners("pause"), 0);
          assert.lengthOf(table.kernel().listeners(name), 0);
          assert.equal(table._pollCursor, null);
          assert.equal(table._nextPoll, null);
          done();
        });
        table._pollCursor = "foo";
        table._nextPoll = setTimeout(() => {return "bar";}, 0);
        table.stop(true);
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
          {cursor: "_flushCursor", next: "_nextFlush"},
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

        buf = Buffer.from("foo");
        out = table.decodeJob(buf);
        assert.ok(out instanceof Error);
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
        assert.notOk(table._pollCursor);
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
        assert.notOk(table._pollCursor);
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
        assert.notOk(table._pollCursor);
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
        assert.notOk(table._pollCursor);
        assert.ok(table._filterClock.called);
        gossip.find.restore();
        table._filterClock.restore();
        table.routeSync.restore();
      });

      it("Should poll table but error out due to changed cursor", function () {
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
          table._pollCursor = null;
          cb();
        });
        sinon.spy(table, "_filterClock");
        sinon.stub(gossip, "find").returns([]);
        table.poll();
        assert.equal(table._values.size, 1);
        assert.ok(table.routeSync.called);
        assert.notOk(table._pollCursor);
        assert.ok(table._filterClock.called);
        gossip.find.restore();
        table._filterClock.restore();
        table.routeSync.restore();
      });

      it("Should poll table but not emit 'idle' after completion", function () {
        var term = new TableTerm({}, "create", new VectorClock(), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          table._migrateCursor = 1;
          cb();
        });
        sinon.stub(gossip, "find").returns([]);
        sinon.spy(table, "emit").withArgs("idle");
        table.poll();
        assert.ok(table.routeSync.called);
        assert.notOk(table._pollCursor);
        assert.notOk(table.emit.calledWith("idle"));
        gossip.find.restore();
        table.emit.restore();
        table.routeSync.restore();
      });

      it("Should update state of table", function () {
        var term = new TableTerm({}, "create", new VectorClock(), "bar");
        var data = {
          id: "foo",
          term: term,
          round: 0
        };
        table.once("create", (id, value) => {
          assert.equal(data.id, id);
          assert.deepEqual(data.term.value(), value);
        });
        table.update(data);
        assert.ok(table.has("foo"));
        assert.equal(table.get("foo").vclock().getCount("bar"), 1);
      });

      it("Should update state of table synchronously", function () {
        var term = new TableTerm({}, "create", new VectorClock(), "bar");
        var data = {
          id: "foo",
          term: term,
          round: 0
        };
        table.once("create", (id, value) => {
          assert.equal(data.id, id);
          assert.deepEqual(data.term.value(), value);
        });
        sinon.stub(table, "reply", (from, val) => {
          assert.deepEqual(JSON.parse(val), {ok: true});
        });
        table.updateSync(data, {});
        assert.ok(table.has("foo"));
        assert.equal(table.get("foo").vclock().getCount("bar"), 1);
        assert.notOk(table._migrations.has("foo"));
        assert.ok(table.reply.called);
        table.reply.restore();
      });

      it("Should flush state to disk", function () {
        sinon.spy(table, "emit").withArgs("flush");
        table.flush();
        assert.ok(table.emit.called);
        table.emit.restore();
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
        sinon.spy(table, "emit").withArgs("flush");
        table.flushSync();
        assert.ok(table.emit.called);
        table.emit.restore();
      });

      it("Should skip flushing state to disk sync", function () {
        table._flushCursor = 1;
        sinon.spy(table, "emit").withArgs("flush");
        table.flushSync();
        assert.notOk(table.emit.called);

        table._flushCursor = null;
        table._stopped = true;
        table.flush();
        assert.notOk(table.emit.called);
        table.emit.restore();
        table._stopped = false;
      });

      it("Should purge table", function (done) {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        table.start("name");
        var term = new TableTerm({}, "create", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table._values.get("foo").vclock());
          assert.equal(n, 1);
          async.nextTick(cb);
        });
        table.purge(chash2);
        table.once("resume", () => {
          assert.ok(table.routeSync.called);
          table.routeSync.restore();
          assert.equal(table._values.size, 0);
          done();
        });
      });

      it("Should purge table, error on migrate", function (done) {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        table.start("name");
        var term = new TableTerm({}, "create", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table._values.get("foo").vclock());
          assert.equal(n, 1);
          async.nextTick(() => {cb(new Error("error"));});
        });
        table._purgeMax = 2;
        table.purge(chash2);
        table.once("error", (err) => {
          assert.ok(err);
          table.once("resume", () => {
            assert.ok(table.routeSync.called);
            table.routeSync.restore();
            assert.equal(table._values.size, 1);
            done();
          });
        });
      });

      it("Should skip purging table", function () {
        sinon.spy(table, "pause");
        table._purgeCursor = 1;
        table.purge(chash);
        assert.notOk(table.pause.called);
        table._purgeCursor = null;
        table._stopped = true;
        table.purge(chash);
        assert.notOk(table.pause.called);
        table._stopped = false;
      });

      it("Should migrate table w/o poll cursor", function (done) {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        table.start("name");
        var term = new TableTerm({}, "create", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table._values.get("foo").vclock());
          assert.equal(n, 1);
          async.nextTick(cb);
        });
        table.migrate(chash, chash2);
        table.once("idle", () => {
          assert.notOk(table._migrateCursor);
          assert.equal(table._migrations.size, 0);
          done();
        });
      });

      it("Should migrate table, handle failed migration", function (done) {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        table.start("name");
        var term = new TableTerm({}, "create", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table._values.get("foo").vclock());
          assert.equal(n, 1);
          async.nextTick(_.partial(cb, new Error("error")));
        });
        table.migrate(chash, chash2);
        table.once("idle", () => {
          assert.notOk(table._migrateCursor);
          assert.equal(table._migrations.size, 0);
          done();
        });
      });

      it("Should migrate table, handle corrupted migration", function () {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        table.start("name");
        var term = new TableTerm({}, "create", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "routeSync", (bucket, event, msg, actor, clock, cb, n) => {
          assert.isArray(bucket);
          assert.equal(event, "migrate");
          assert.deepEqual(msg, {
            id: "foo",
            state: "create",
            value: {}
          });
          assert.deepEqual(clock, table._values.get("foo").vclock());
          assert.equal(n, 1);
          table._migrateCursor = null;
          table._migrations.delete("foo");
          cb();
        });
        table.migrate(chash, chash2);
        assert.notOk(table._migrateCursor);
        assert.equal(table._migrations.size, 0);
      });

      it("Should migrate table, clear poll cursor", function (done) {
        sinon.stub(table, "_migrateState", () => {
          table._migrateCursor = null;
        });
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        table._pollCursor = 1;
        table.migrate(chash, chash2);
        assert.ok(table._migrateState.called);
        assert.notOk(table._pollCursor);

        table._pollCursor = 1;
        table._nextPoll = setTimeout(() => {}, 0);
        table.migrate(chash, chash2);
        assert.equal(table._migrateState.callCount, 2);
        assert.notOk(table._pollCursor);
        assert.notOk(table._nextPoll);

        table._migrateState.restore();
        done();
      });
      
      it("Should migrate key/val pair out of table", function (done) {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
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
          sinon.stub(gossip, "find").returns([]);
          cb();
        });
        table.migrate(chash, chash2);
        assert.equal(table._values.size, 0);
        assert.ok(table.routeSync.called);
        assert.notOk(table._migrateCursor);
        assert.equal(table._values.has("foo"), false);
        gossip.find.restore();
        table.routeSync.restore();
        done();
      });

      it("Should skip migrating table", function () {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        sinon.spy(table, "emit").withArgs("migrate");
        table._migrateCursor = 1;
        table.migrate(chash, chash2);
        assert.notOk(table.emit.called);
        table._migrateCursor = null;

        table._stopped = true;
        table.migrate(chash, chash2);
        assert.notOk(table.emit.called);
        table._stopped = false;

        table.migrate(chash, chash);
        assert.notOk(table.emit.called);
      });

      it("Should merge state", function () {
        var term = new TableTerm({}, "update", new VectorClock(), "bar");
        var oterm = new TableTerm({}, "update", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "emit", (state, id, val) => {
          assert.equal(state, "update");
          assert.equal(id, "foo");
          assert.deepEqual(val, {});
        });
        table._mergeState(term, "foo", oterm);
        assert.deepEqual(table.get("foo"), oterm);
        table.emit.restore();
      });

      it("Should fail to merge state due to vector clock conflict", function () {
        var term = new TableTerm({}, "update", new VectorClock("baz", 1), "baz");
        var oterm = new TableTerm({}, "update", new VectorClock("bar", 1), "bar");
        table.set("foo", term);
        sinon.stub(table, "emit", (event, id, msg) => {
          assert.equal(event, "conflict");
          assert.equal(id, "foo");
          assert.deepEqual(msg, oterm);
          table.emit.restore();
        });
        table._mergeState(term, "foo", oterm);
        assert.deepEqual(table.get("foo"), term);
      });

      it("Should route message to nodes", function () {
        var bucket = [kernel.self()];
        var event = "foo";
        var msg = "bar";
        var actor = "baz";
        var vclock = new VectorClock(actor, 1);
        sinon.stub(table, "abcast", (nodes, id, event, value) => {
          assert.deepEqual(nodes, bucket);
          assert.equal(id, table._id);
          assert.equal(event, "foo");
          assert.deepEqual(value, {
            actor: actor,
            data: msg,
            vclock: vclock.toJSON(true),
            round: 0
          });
        });
        table.once("send", (sClock, sEvent, sMsg) => {
          assert.deepEqual(sClock, vclock);
          assert.equal(sEvent, event);
          assert.equal(sMsg, msg);
        });
        table.route(bucket, event, msg, actor, vclock, 1);
      });

      it("Should ignore routing when round number is 0", function () {
        var bucket = [kernel.self()];
        var event = "foo";
        var msg = "bar";
        var actor = "baz";
        var vclock = new VectorClock(actor, 1);
        sinon.spy(table, "abcast");
        table.route(bucket, event, msg, actor, vclock, 0);
        assert.notOk(table.abcast.called);
        table.abcast.restore();
      });

      it("Should route message to nodes synchronously", function (done) {
        var bucket = [kernel.self()];
        var event = "foo";
        var msg = "bar";
        var actor = "baz";
        var vclock = new VectorClock(actor, 1);
        sinon.stub(table, "multicall", (nodes, id, event, value, cb, timeout) => {
          assert.deepEqual(nodes, bucket);
          assert.equal(id, table._id);
          assert.equal(event, "foo");
          assert.deepEqual(value, {
            actor: actor,
            data: msg,
            vclock: vclock.toJSON(true),
            round: 0
          });
          assert.equal(timeout, 5000);
          async.nextTick(cb);
        });
        table.once("send", (sClock, sEvent, sMsg) => {
          assert.deepEqual(sClock, vclock);
          assert.equal(sEvent, event);
          assert.equal(sMsg, msg);
        });
        table.routeSync(bucket, event, msg, actor, vclock, (err) => {
          assert.notOk(err);
          done();
        }, 1);
      });

      it("Should route message to nodes synchronously, return error", function (done) {
        var bucket = [kernel.self()];
        var event = "foo";
        var msg = "bar";
        var actor = "baz";
        var vclock = new VectorClock(actor, 1);
        sinon.stub(table, "multicall", (nodes, id, event, value, cb, timeout) => {
          assert.deepEqual(nodes, bucket);
          assert.equal(id, table._id);
          assert.equal(event, "foo");
          assert.deepEqual(value, {
            actor: actor,
            data: msg,
            vclock: vclock.toJSON(true),
            round: 0
          });
          assert.equal(timeout, 5000);
          async.nextTick(_.partial(cb, new Error("error")));
        });
        table.once("send", (sClock, sEvent, sMsg) => {
          assert.deepEqual(sClock, vclock);
          assert.equal(sEvent, event);
          assert.equal(sMsg, msg);
        });
        table.routeSync(bucket, event, msg, actor, vclock, (err) => {
          assert.ok(err);
          done();
        }, 1);
      });

      it("Should ignore routing message sync when round number is 0", function (done) {
        var bucket = [kernel.self()];
        var event = "foo";
        var msg = "bar";
        var actor = "baz";
        var vclock = new VectorClock(actor, 1);
        sinon.spy(table, "multicall");
        table.routeSync(bucket, event, msg, actor, vclock, () => {
          assert.notOk(table.multicall.called);
          table.multicall.restore();
          done();
        }, 0);
      });
    });
  });
};

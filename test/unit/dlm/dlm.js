var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    stream = require("stream"),
    sinon = require("sinon"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("DLMServer unit tests", function () {
    var DLMServer = lib.dlm.DLMServer,
        Lock = lib.dlm.Lock,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        CHash = lib.chash,
        VectorClock = lib.vclock,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("DLMServer state tests", function () {
      var kernel,
          gossip,
          server,
          chash,
          vclock,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function () {
        kernel = new NetKernel(new MockIPC(), id, host, port);
        chash = new CHash(3, 3);
        vclock = new VectorClock();
        gossip = new GossipRing(kernel, chash, vclock, {
          interval: 100,
          flushInterval: 100,
          flushPath: "/foo/bar",
          vclockOpts: {}
        });
      });

      beforeEach(function () {
        server = new DLMServer(gossip, kernel);
      });

      it("Should start generic server with name", function (done) {
        var name = "foo";
        server.start(name);
        assert.equal(server._id, name);
        assert.lengthOf(server.kernel().listeners(name), 1);
        assert.lengthOf(server.listeners("pause"), 1);
        assert.lengthOf(server.listeners("stop"), 5);
        server.stop();
        server.once("stop", done);
      });

      it("Should start generic server w/o name", function (done) {
        var id = server.id();
        server.start();
        assert.equal(server._id, id);
        assert.lengthOf(server.kernel().listeners(id), 1);
        assert.lengthOf(server.listeners("pause"), 1);
        assert.lengthOf(server.listeners("stop"), 5);
        server.stop();
        server.once("stop", done);
      });

      it("Should stop generic server", function (done) {
        var name = "foo";
        server.start(name);
        server.once("stop", () => {
          assert.equal(server._streams.size, 0);
          assert.lengthOf(server.listeners("pause"), 0);
          assert.lengthOf(server.kernel().listeners(name), 0);
          assert.equal(server._table._table.size, 0);
          assert.equal(server._locks.size, 0);
          done();
        });
        server.stop();
      });

      it("Should wait for idle event before stopping", function (done) {
        var name = "foo";
        server.start(name);
        server.once("stop", () => {
          assert.equal(server._streams.size, 0);
          assert.lengthOf(server.listeners("pause"), 0);
          assert.lengthOf(server.kernel().listeners(name), 0);
          assert.equal(server._table._table.size, 0);
          assert.equal(server._locks.size, 0);
          done();
        });
        server._streams.set("foo", "bar");
        server.stop();
        server._streams.clear();
        server.emit("idle");
      });

      it("Should return whether handler is idle or not", function (done) {
        server.start("foo");
        assert.equal(server.idle(), true);
        server._streams.set("foo", "bar");
        assert.equal(server.idle(), false);
        server._streams.delete("foo");
        sinon.stub(server._table, "idle", () => {return false;});
        assert.equal(server.idle(), false);
        server._table.idle.restore();

        server._streams.set("foo", "bar");
        server._table.emit("idle");
        assert.equal(server.idle(), false);
        server._streams.delete("foo");
        server._table.emit("idle");
        assert.equal(server.idle(), true);

        server.stop();
        server.once("stop", done);
      });

      it("Should fail to load state", function (done) {
        sinon.stub(server._table, "load", (cb) => {
          return cb(new Error("foo"));
        });
        server.load((err) => {
          assert.ok(err);
          server._table.load.restore();
          done();
        });
      });

      it("Should load state, no disk persistence", function (done) {
        server.load(() => {
          assert.equal(server._table._table.size, 0);
          done();
        });
      });

      it("Should load state, disk persistence", function (done) {
        server._disk = true;
        sinon.stub(server._table, "load", (cb) => {
          server._table.set("foo", "bar");
          server._table.set("bar", new Map([["baz", "quab"]]));
          return cb();
        });
        server.load(() => {
          assert.ok(server._locks.has("foo"));
          assert.ok(server._locks.has("bar"));
          assert.equal(server._locks.get("foo").type(), "write");
          assert.equal(server._locks.get("bar").type(), "read");
          assert.ok(server._locks.get("bar").timeout().has("baz"));
          server._table.load.restore();
          server.stop();
          server.once("stop", done);
        });
      });
    });

    describe("DLMServer operation tests", function () {
      var kernel,
          gossip,
          server,
          chash,
          vclock, 
          id = "id",
          host = "localhost",
          port = 8000;

      before(function (done) {
        kernel = new NetKernel(new MockIPC(), id, host, port);
        chash = new CHash(3, 3);
        vclock = new VectorClock();
        gossip = new GossipRing(kernel, chash, vclock, {
          interval: 100,
          flushInterval: 100,
          flushPath: "/foo/bar",
          vclockOpts: {}
        });
        gossip.start("ring");
        kernel.start({retry: 500, maxRetries: Infinity});
        kernel.once("_ready", done);
      });

      beforeEach(function () {
        server = new DLMServer(gossip, kernel);
        server.start("foo");
      });

      afterEach(function (done) {
        server.once("stop", function () {
          async.nextTick(done);
        });
        server.stop();
      });

      after(function () {
        kernel.stop();
      });

      it("Should decode job", function () {
        var job = Buffer.from(JSON.stringify({
          event: "rlock",
          data: {
            id: "id",
            holder: "holder",
            timeout: 1000
          }
        }));
        var out = server.decodeJob(job);
        assert.deepEqual(out, {
          event: "rlock",
          data: {
            id: "id",
            holder: "holder",
            timeout: 1000
          }
        });

        out = server.decodeJob(Buffer.from("foo"));
        assert.ok(out instanceof Error);

        out = server.decodeJob(Buffer.from(JSON.stringify({event: "rlock"})));
        assert.ok(out instanceof Error);

        out = server.decodeJob(Buffer.from(JSON.stringify({event: "not defined"})));
        assert.ok(out instanceof Error);
      });

      it("Should decode singleton", function () {
        var job = {
          event: "rlock",
          data: {
            id: "id",
            holder: "holder",
            timeout: 1000
          }
        };
        var out = server.decodeSingleton(job);
        assert.deepEqual(out, job);

        out = server.decodeSingleton({event: "rlock"});
        assert.ok(out instanceof Error);

        out = server.decodeSingleton({event: "not defined"});
        assert.ok(out instanceof Error);
      });
    });

    describe("DLMServer command tests", function () {
      var kernel,
          gossip,
          server,
          chash,
          vclock, 
          from,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function (done) {
        from = {node: new Node("id2")};
        kernel = new NetKernel(new MockIPC(), id, host, port);
        chash = (new CHash(3, 3)).insert(new Node(id, host, port));
        vclock = new VectorClock();
        gossip = new GossipRing(kernel, chash, vclock, {
          interval: 100,
          flushInterval: 100,
          flushPath: "/foo/bar",
          vclockOpts: {}
        });
        gossip.start("ring");
        kernel.start({retry: 500, maxRetries: Infinity});
        kernel.once("_ready", done);
      });

      beforeEach(function () {
        server = new DLMServer(gossip, kernel);
        server.start("foo");
      });

      afterEach(function (done) {
        server.once("stop", done);
        server.stop();
      });

      after(function () {
        kernel.stop();
      });

      it("Should fail to perform rlock command, timeout", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "rlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          async.nextTick(() => {
            return cb(new Error("Timeout"));
          });
        });
        server.rlock("id", "holder", 1000, (err, res) => {
          assert.ok(err);
          server.multicall.restore();
          done();
        });
      });


      it("Should fail to perform rlock command, quorum not reached, retry limit breached", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "rlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          async.nextTick(() => {
            return cb(null, [JSON.stringify({ok: false})]);
          });
        });
        sinon.stub(server, "abcast");
        server.rlock("id", "holder", 1000, (err, res) => {
          assert.ok(err);
          server.multicall.restore();
          server.abcast.restore();
          done();
        }, 1000, 0);
      });

      it("Should fail to perform rlock command, timeout breached, retry limit breached", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "rlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 0
          });
          async.nextTick(() => {
            return cb(null, [JSON.stringify({ok: true})]);
          });
        });
        sinon.stub(server, "abcast");
        server.rlock("id", "holder", 0, (err, res) => {
          assert.ok(err);
          server.multicall.restore();
          server.abcast.restore();
          done();
        }, 1000, 0);
      });

      it("Should perform rlock command, >0 retries", function (done) {
        const oldMin = server._minWaitTimeout;
        const oldMax = server._maxWaitTimeout;
        server._minWaitTimeout = 0;
        server._maxWaitTimeout = 0;
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "rlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          server.multicall.restore();
          sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
            assert.deepEqual(nodes, gossip.ring().nodes());
            assert.equal(id, server._id);
            assert.equal(event, "rlock");
            assert.deepEqual(msg, {
              id: "id",
              holder: "holder",
              timeout: 1000
            });
            return cb(null, [{ok: true}]);
          });
          return cb(null, [{ok: false}]);
        });
        sinon.stub(server, "abcast");
        server.rlock("id", "holder", 1000, (err, res) => {
          assert.notOk(err);
          assert.deepEqual(res, gossip.ring().nodes());
          server.multicall.restore();
          server.abcast.restore();
          server._minWaitTimeout = oldMin;
          server._maxWaitTimeout = oldMax;
          done();
        }, 1000, 1);
      });

      it("Should perform rlock command", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "rlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          return cb(null, [{ok: true}]);
        });
        server.rlock("id", "holder", 1000, (err, res) => {
          assert.notOk(err);
          assert.deepEqual(res, gossip.ring().nodes());
          server.multicall.restore();
          done();
        }, 1000, 0);
      });

      it("Should fail to perform wlock command, timeout", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          async.nextTick(() => {
            return cb(new Error("Timeout"));
          });
        });
        server.wlock("id", "holder", 1000, (err, res) => {
          assert.ok(err);
          server.multicall.restore();
          done();
        });
      });


      it("Should fail to perform wlock command, quorum not reached, retry limit breached", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          async.nextTick(() => {
            return cb(null, [JSON.stringify({ok: false})]);
          });
        });
        sinon.stub(server, "abcast");
        server.wlock("id", "holder", 1000, (err, res) => {
          assert.ok(err);
          server.multicall.restore();
          server.abcast.restore();
          done();
        }, 1000, 0);
      });

      it("Should fail to perform wlock command, timeout breached, retry limit breached", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 0
          });
          async.nextTick(() => {
            return cb(null, [JSON.stringify({ok: true})]);
          });
        });
        sinon.stub(server, "abcast");
        server.wlock("id", "holder", 0, (err, res) => {
          assert.ok(err);
          server.multicall.restore();
          server.abcast.restore();
          done();
        }, 1000, 0);
      });

      it("Should perform wlock command, >0 retries", function (done) {
        const oldMin = server._minWaitTimeout;
        const oldMax = server._maxWaitTimeout;
        server._minWaitTimeout = 0;
        server._maxWaitTimeout = 0;
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          server.multicall.restore();
          sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
            assert.deepEqual(nodes, gossip.ring().nodes());
            assert.equal(id, server._id);
            assert.equal(event, "wlock");
            assert.deepEqual(msg, {
              id: "id",
              holder: "holder",
              timeout: 1000
            });
            return cb(null, [{ok: true}]);
          });
          return cb(null, [{ok: false}]);
        });
        sinon.stub(server, "abcast");
        server.wlock("id", "holder", 1000, (err, res) => {
          assert.notOk(err);
          assert.deepEqual(res, gossip.ring().nodes());
          server.multicall.restore();
          server.abcast.restore();
          server._minWaitTimeout = oldMin;
          server._maxWaitTimeout = oldMax;
          done();
        }, 1000, 1);
      });

      it("Should perform wlock command", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          return cb(null, [{ok: true}]);
        });
        server.wlock("id", "holder", 1000, (err, res) => {
          assert.notOk(err);
          assert.deepEqual(res, gossip.ring().nodes());
          server.multicall.restore();
          done();
        }, 1000, 0);
      });

      it("Should fail to perform runlock command, timeout", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "runlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
          async.nextTick(() => {
            return cb(new Error("Tiemout"));
          });
        });
        server.runlock("id", "holder", (err) => {
          assert.ok(err);
          server.multicall.restore();
          done();
        }, 1000);
      });

      it("Should perform runlock command", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "runlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
          async.nextTick(() => {
            return cb(null, [JSON.stringify({ok: true})]);
          });
        });
        server.runlock("id", "holder", (err) => {
          assert.notOk(err);
          server.multicall.restore();
          done();
        }, 1000);
      });

      it("Should perform async runlock command", function () {
        sinon.stub(server, "abcast", (nodes, id, event, msg) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "runlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
        });
        server.runlockAsync("id", "holder");
        server.abcast.restore();
      });

      it("Should fail to perform wunlock command, timeout", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wunlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
          async.nextTick(() => {
            return cb(new Error("Tiemout"));
          });
        });
        server.wunlock("id", "holder", (err) => {
          assert.ok(err);
          server.multicall.restore();
          done();
        }, 1000);
      });

      it("Should perform wunlock command", function (done) {
        sinon.stub(server, "multicall", (nodes, id, event, msg, cb, timeout) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wunlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
          async.nextTick(() => {
            return cb(null, [JSON.stringify({ok: true})]);
          });
        });
        server.wunlock("id", "holder", (err) => {
          assert.notOk(err);
          server.multicall.restore();
          done();
        }, 1000);
      });

      it("Should perform async wunlock command", function () {
        sinon.stub(server, "abcast", (nodes, id, event, msg) => {
          assert.deepEqual(nodes, gossip.ring().nodes());
          assert.equal(id, server._id);
          assert.equal(event, "wunlock");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
        });
        server.wunlockAsync("id", "holder");
        server.abcast.restore();
      });

      it("Should fail to complete 'rlock' event, write lock exists", function () {
        server._locks.set("foo", new Lock("write", "foo"));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doRLock({
          id: "foo",
          holder: "holder",
          timeout: 1000
        }, from);
        server.reply.restore();
      });

      it("Should handle 'rlock' event, holder already exists at lock", function () {
        server._locks.set("foo", new Lock("read", "foo", new Map([["holder", undefined]])));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doRLock({
          id: "foo",
          holder: "holder",
          timeout: 1000
        }, from);
        assert.equal(server._locks.size, 1);
        assert.equal(server._locks.get("foo").timeout().size, 1);
        server.reply.restore();
      });

      it("Should handle 'rlock' event", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doRLock({
          id: "foo",
          holder: "holder",
          timeout: 1000
        }, from);
        assert.equal(server._locks.size, 1);
        assert.equal(server._locks.get("foo").timeout().size, 1);
        assert.ok(server._locks.get("foo").timeout().has("holder"));
        assert.deepEqual(server._table.hget("foo", "holder").timeout, 1000);

        server._doRLock({
          id: "foo",
          holder: "holder2",
          timeout: 1000
        }, from);
        assert.equal(server._locks.size, 1);
        assert.equal(server._locks.get("foo").timeout().size, 2);
        assert.ok(server._locks.get("foo").timeout().has("holder2"));
        assert.deepEqual(server._table.hget("foo", "holder").timeout, 1000);
        server.reply.restore();
      });

      it("Should fail to complete 'wlock' event, lock already exists", function () {
        server._locks.set("foo", new Lock("write", "foo"));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doWLock({
          id: "foo",
          holder: "holder",
          timeout: 1000
        }, from);
        server.reply.restore();
      });

      it("Should handle 'wlock' event", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doWLock({
          id: "foo",
          holder: "holder",
          timeout: 1000
        }, from);
        assert.equal(server._locks.size, 1);
        assert.equal(server._table.get("foo").holder, "holder");
        assert.equal(server._table.get("foo").timeout, 1000);
        server.reply.restore();
      });

      it("Should fail to complete 'runlock' event, no lock exists", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doRUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should fail to complete 'runlock' event, write lock exists", function () {
        server._locks.set("foo", new Lock("write", "foo"));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doRUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should fail to complete 'runlock' event, holder doesn't exist at lock", function () {
        server._locks.set("foo", new Lock("read", "foo", new Map([["holder2", null]])));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doRUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should handle 'runlock' event", function () {
        server._locks.set("foo", new Lock("read", "foo", new Map([["holder", null], ["holder2", null]])));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doRUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        assert.notOk(server._locks.get("foo").timeout().has("holder"));

        server._doRUnlock({
          id: "foo",
          holder: "holder2"
        }, from);
        assert.notOk(server._locks.has("foo"));
        server._safeReply.restore();
      });

      it("Should fail to complete 'wunlock' event, no lock exists", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doWUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should fail to complete 'wunlock' event, read lock exists", function () {
        server._locks.set("foo", new Lock("read", "foo", new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doWUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should fail to complete 'wunlock' event, holder doesn't match lock", function () {
        server._locks.set("foo", new Lock("write", "foo"));
        server._table.set("foo", {holder: "holder2"});
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doWUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should handle 'wunlock' event", function () {
        server._locks.set("foo", new Lock("write", "foo"));
        server._table.set("foo", {holder: "holder"});
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doWUnlock({
          id: "foo",
          holder: "holder"
        }, from);
        assert.notOk(server._locks.has("foo"));
        server._safeReply.restore();
      });

      it("Should fail to clear rlock after timeout, lock doesn't exist", function () {
        var oldLocks = server._locks;
        server._clearRLock("id", "holder");
        assert.ok(_.isEqual(oldLocks, server._locks));
      });

      it("Should fail to clear rlock after timeout, write lock exists", function () {
        server._locks.set("id", new Lock("write", "id"));
        var oldLocks = server._locks;
        server._clearRLock("id", "holder");
        assert.ok(_.isEqual(oldLocks, server._locks));
      });

      it("Should fail to clear rlock after timeout, holder doesn't exist at lock", function () {
        server._locks.set("id", new Lock("read", "id", new Map([["holder2", null]])));
        var oldLocks = server._locks;
        server._clearRLock("id", "holder");
        assert.ok(_.isEqual(oldLocks, server._locks));
      });

      it("Should clear rlock after timeout", function () {
        server._locks.set("id", new Lock("read", "id", new Map([["holder", null], ["holder2", null]])));
        server._clearRLock("id", "holder");
        assert.notOk(server._locks.get("id").timeout().has("holder"));
        server._clearRLock("id", "holder2");
        assert.notOk(server._locks.has("id"));
      });

      it("Should fail to clear wlock after timeout, lock doesn't eixst", function () {
        var oldLocks = server._locks;
        server._clearWLock("id", "holder");
        assert.ok(_.isEqual(oldLocks, server._locks));
      });

      it("Should fail to clear wlock after timeout, read lock exists", function () {
        server._locks.set("id", new Lock("read", "id", new Map([["holder", null]])));
        server._table.set("id", new Map([["holder", {}]]));
        var oldLocks = server._locks;
        server._clearWLock("id", "holder");
        assert.ok(_.isEqual(oldLocks, server._locks));
      });

      it("Should fail to clear wlock after timeout, holder doesn't match", function () {
        server._locks.set("id", new Lock("write", "id"));
        server._table.set("id", {holder: "holder"});
        var oldLocks = server._locks;
        server._clearWLock("id", "holder2");
        assert.ok(_.isEqual(oldLocks, server._locks));
      });

      it("Should clear wlock after timeout", function () {
        server._locks.set("id", new Lock("write", "id"));
        server._table.set("id", {holder: "holder"});
        server._clearWLock("id", "holder");
        assert.notOk(server._locks.has("id"));
      });
    });

    describe("DLMServer static tests", function () {
      it("Should fail to parse an incoming job, not an object", function () {
        var out = DLMServer.parseJob("foo", "rlock");
        assert.ok(out instanceof Error);
      });

      it("Should fail to parse an incoming job, unsupported command", function () {
        var out = DLMServer.parseJob({}, "foo");
        assert.ok(out instanceof Error);
      });

      it("Should fail to parse an incoming job, bad type match", function () {
        var out = DLMServer.parseJob({
          id: 5,
          holder: "asdf"
        }, "runlock");
        assert.ok(out instanceof Error);

        out = DLMServer.parseJob({
          id: "asdf",
          holder: 5
        }, "runlock");
        assert.ok(out instanceof Error);
      });

      it("Should parse an incoming job", function () {
        var obj = {
          id: "foo",
          holder: "bar"
        };
        assert.deepEqual(DLMServer.parseJob(obj, "runlock"), obj);
      });

      it("Should find, from a multicall response, which responses indicate success", function () {
        var out = DLMServer.findLockPasses(["foo", "bar"], [{ok: false}, {ok: true}]);
        assert.deepEqual(out, ["bar"]);
      });

      it("Should calculate wait time for a retry", function () {
        var out = DLMServer.calculateWaitTime(0, 100);
        assert.ok(out >= 0 && out <= 100);
      });
    });
  });
};

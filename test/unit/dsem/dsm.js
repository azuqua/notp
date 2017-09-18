var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    sinon = require("sinon"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("DSMServer unit tests", function () {
    var DSMServer = lib.dsem.DSMServer,
        Semaphore = lib.dsem.Semaphore,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        CHash = lib.chash,
        VectorClock = lib.vclock,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("DSMServer state tests", function () {
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
        server = new DSMServer(gossip, kernel);
      });

      it("Should start generic server with name", function (done) {
        var name = "foo";
        server.start(name);
        assert.equal(server._id, name);
        assert.lengthOf(server.kernel().listeners(name), 1);
        assert.lengthOf(server.listeners("pause"), 1);
        assert.lengthOf(server.listeners("stop"), 6);
        server.stop();
        server.once("stop", done);
      });

      it("Should start generic server w/o name", function (done) {
        var id = server.id();
        server.start();
        assert.equal(server._id, id);
        assert.lengthOf(server.kernel().listeners(id), 1);
        assert.lengthOf(server.listeners("pause"), 1);
        assert.lengthOf(server.listeners("stop"), 6);
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
          assert.equal(server._semaphores.size, 0);
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
          assert.equal(server._semaphores.size, 0);
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
          server._table.set("SEMAPHORES::foo", 3);
          server._table.set("SEMAPHORE_HOLDERS::foo", new Map([["bar", {}]]));
          return cb();
        });
        server.load(() => {
          assert.ok(server._semaphores.has("foo"));
          assert.equal(server._semaphores.get("foo").size(), 3);
          assert.ok(server._semaphores.get("foo").timeouts().has("bar"));
          server._table.load.restore();
          server.stop();
          server.once("stop", done);
        });
      });

      it("Should load state, disk persistence, other order", function (done) {
        server._disk = true;
        sinon.stub(server._table, "load", (cb) => {
          server._table.set("SEMAPHORE_HOLDERS::foo", new Map([["bar", {}]]));
          server._table.set("SEMAPHORES::foo", 3);
          return cb();
        });
        server.load(() => {
          assert.ok(server._semaphores.has("foo"));
          assert.equal(server._semaphores.get("foo").size(), 3);
          assert.ok(server._semaphores.get("foo").timeouts().has("bar"));
          server._table.load.restore();
          server.stop();
          server.once("stop", done);
        });
      });
    });

    describe("DSMServer operation tests", function () {
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
        server = new DSMServer(gossip, kernel);
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
          event: "post",
          data: {
            id: "id",
            holder: "holder",
            timeout: 1000
          }
        }));
        var out = server.decodeJob(job);
        assert.deepEqual(out, {
          event: "post",
          data: {
            id: "id",
            holder: "holder",
            timeout: 1000
          }
        });

        out = server.decodeJob(Buffer.from("foo"));
        assert.ok(out instanceof Error);

        out = server.decodeJob(Buffer.from(JSON.stringify({event: "post"})));
        assert.ok(out instanceof Error);

        out = server.decodeJob(Buffer.from(JSON.stringify({event: "not defined"})));
        assert.ok(out instanceof Error);
      });
    });

    describe("DSMServer command tests", function () {
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
        server = new DSMServer(gossip, kernel);
        server.start("foo");
      });

      afterEach(function (done) {
        server.once("stop", done);
        server.stop();
      });

      after(function () {
        kernel.stop();
      });

      it("Should fail to perform create command, timeout", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "create");
          assert.deepEqual(msg, {
            id: "id",
            n: 3
          });
          async.nextTick(() => {
            return cb(new Error("Timeout"));
          });
        });
        server.create("id", 3, (err, res) => {
          assert.ok(err);
          server.call.restore();
          done();
        });
      });

      it("Should perform create command", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "create");
          assert.deepEqual(msg, {
            id: "id",
            n: 3
          });
          return cb(null, JSON.stringify({ok: true}));
        });
        server.create("id", 3, (err) => {
          assert.notOk(err);
          server.call.restore();
          done();
        });
      });

      it("Should fail to perform read command, timeout", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "read");
          assert.deepEqual(msg, {
            id: "id"
          });
          async.nextTick(() => {
            return cb(new Error("Timeout"));
          });
        });
        server.read("id", (err, res) => {
          assert.ok(err);
          server.call.restore();
          done();
        });
      });

      it("Should fail to perform read command, response invalid JSON", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "read");
          assert.deepEqual(msg, {
            id: "id"
          });
          async.nextTick(() => {
            return cb(null, "{");
          });
        });
        server.read("id", (err, res) => {
          assert.ok(err);
          server.call.restore();
          done();
        });
      });

      it("Should perform read command", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "read");
          assert.deepEqual(msg, {
            id: "id"
          });
          return cb(null, JSON.stringify({ok: true, data: {n: 3, active: 0}}));
        });
        server.read("id", (err, res) => {
          assert.notOk(err);
          assert.deepEqual(res, {n: 3, active: 0});
          server.call.restore();
          done();
        });
      });

      it("Should fail to perform destroy command, timeout", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "destroy");
          assert.deepEqual(msg, {
            id: "id"
          });
          async.nextTick(() => {
            return cb(new Error("Timeout"));
          });
        });
        server.destroy("id", (err, res) => {
          assert.ok(err);
          server.call.restore();
          done();
        });
      });

      it("Should perform destroy command", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "destroy");
          assert.deepEqual(msg, {
            id: "id"
          });
          return cb(null, JSON.stringify({ok: true}));
        });
        server.destroy("id", (err) => {
          assert.notOk(err);
          server.call.restore();
          done();
        });
      });

      it("Should fail to perform post command, timeout", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "post");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          async.nextTick(() => {
            return cb(new Error("Timeout"));
          });
        });
        server.post("id", "holder", 1000, (err, res) => {
          assert.ok(err);
          server.call.restore();
          done();
        });
      });

      it("Should fail to perform post command, timeout breached, retry limit breached", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "post");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 0
          });
          async.nextTick(() => {
            return cb(null, JSON.stringify({ok: true}));
          });
        });
        sinon.stub(server, "cast");
        server.post("id", "holder", 0, (err, res) => {
          assert.ok(err);
          server.call.restore();
          server.cast.restore();
          done();
        }, 1000, 0);
      });

      it("Should perform post command, >0 retries", function (done) {
        const oldMin = server._minWaitTimeout;
        const oldMax = server._maxWaitTimeout;
        server._minWaitTimeout = 0;
        server._maxWaitTimeout = 0;
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "post");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          server.call.restore();
          sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
            assert.deepEqual(node, {
              node: gossip.ring().find("id"),
              id: server._id
            });
            assert.equal(event, "post");
            assert.deepEqual(msg, {
              id: "id",
              holder: "holder",
              timeout: 1000
            });
            return cb(null, JSON.stringify({ok: true}));
          });
          return cb(null, JSON.stringify({ok: false}));
        });
        sinon.stub(server, "cast");
        server.post("id", "holder", 1000, (err, res) => {
          assert.notOk(err);
          server.call.restore();
          server.cast.restore();
          server._minWaitTimeout = oldMin;
          server._maxWaitTimeout = oldMax;
          done();
        }, 1000, 1);
      });

      it("Should perform post command", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "post");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder",
            timeout: 1000
          });
          return cb(null, JSON.stringify({ok: true}));
        });
        server.post("id", "holder", 1000, (err) => {
          assert.notOk(err);
          server.call.restore();
          done();
        });
      });

      it("Should fail to perform close command, timeout", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "close");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
          async.nextTick(() => {
            return cb(new Error("Tiemout"));
          });
        });
        server.close("id", "holder", (err) => {
          assert.ok(err);
          server.call.restore();
          done();
        }, 1000);
      });

      it("Should perform close command", function (done) {
        sinon.stub(server, "call", (node, event, msg, cb, timeout) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "close");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
          async.nextTick(() => {
            return cb(null, JSON.stringify({ok: true}));
          });
        });
        server.close("id", "holder", (err) => {
          assert.notOk(err);
          server.call.restore();
          done();
        }, 1000);
      });

      it("Should perform async close command", function () {
        sinon.stub(server, "cast", (node, event, msg) => {
          assert.deepEqual(node, {
            node: gossip.ring().find("id"),
            id: server._id
          });
          assert.equal(event, "close");
          assert.deepEqual(msg, {
            id: "id",
            holder: "holder"
          });
        });
        server.closeAsync("id", "holder");
        server.cast.restore();
      });

      it("Should fail to complete 'create' command, size different", function () {
        server._semaphores.set("foo", new Semaphore("id", 3, new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_errorReply", (from, out) => {
          assert.ok(out instanceof Error);
        });
        server._doCreate({
          id: "foo",
          n: 5
        }, from);
        server._errorReply.restore();
      });

      it("Should complete 'create' command, semaphore already exists", function () {
        server._semaphores.set("foo", new Semaphore("id", 3, new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doCreate({
          id: "foo",
          n: 3
        }, from);
        server.reply.restore();
      });

      it("Should complete 'create' command", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doCreate({
          id: "foo",
          n: 3
        }, from);
        assert.ok(_.isEqual(server._semaphores.get("foo"), new Semaphore("foo", 3, new Map())));
        server.reply.restore();
      });

      it("Should fail to complete 'read' command, semaphore doesn't exist", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_errorReply", (from, out) => {
          assert.ok(out instanceof Error);
        });
        server._doRead({
          id: "foo"
        }, from);
        server._errorReply.restore();
      });

      it("Should complete 'read' command", function () {
        server._semaphores.set("foo", new Semaphore("foo", 3, new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true, data: {n: 3, active: 0}});
        });
        server._doRead({
          id: "foo"
        }, from);
        server.reply.restore();
      });

      it("Should complete 'destroy' command, semaphore doesn't exist", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doDestroy({
          id: "foo"
        }, from);
        server.reply.restore();
      });

      it("Should complete 'destroy' command", function () {
        server._semaphores.set("foo", new Semaphore("foo", 3, new Map([["bar", null]])));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doDestroy({
          id: "foo"
        }, from);
        assert.notOk(server._semaphores.has("foo"));
        server.reply.restore();
      });

      it("Should fail to complete 'post' command, semaphore doesn't exist", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_errorReply", (from, out) => {
          assert.ok(out instanceof Error);
        });
        server._doPost({
          id: "foo",
          holder: "holder"
        }, from);
        server._errorReply.restore();
      });

      it("Should fail to complete 'post' command, semaphore full", function () {
        server._semaphores.set("foo", new Semaphore("foo", 0, new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doPost({
          id: "foo",
          holder: "holder"
        }, from);
        server.reply.restore();
      });

      it("Should complete 'post' command, holder already exists at semaphore", function () {
        server._semaphores.set("foo", new Semaphore("foo", 3, new Map([["holder", null]])));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doPost({
          id: "foo",
          holder: "holder"
        }, from);
        server.reply.restore();
      });

      it("Should complete 'post' command", function () {
        server._semaphores.set("foo", new Semaphore("foo", 3, new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "reply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doPost({
          id: "foo",
          holder: "holder",
          timeout: 1000
        }, from);
        assert.ok(server._semaphores.get("foo").timeouts().has("holder"));
        server.reply.restore();
      });

      it("Should fail to complete 'close' command, semaphore doesn't exist", function () {
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_errorReply", (from, out) => {
          assert.ok(out instanceof Error);
        });
        server._doClose({
          id: "foo",
          holder: "holder"
        }, from);
        server._errorReply.restore();
      });

      it("Should fail to complete 'close' command, holder doesn't exist at semaphore", function () {
        server._semaphores.set("foo", new Semaphore("foo", 3, new Map()));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: false});
        });
        server._doClose({
          id: "foo",
          holder: "holder"
        }, from);
        server._safeReply.restore();
      });

      it("Should complete 'close' command", function () {
        server._semaphores.set("foo", new Semaphore("foo", 3, new Map([["holder", null]])));
        var from = {
          tag: uuid.v4(),
          node: kernel.self()
        };
        sinon.stub(server, "_safeReply", (from, out) => {
          assert.deepEqual(out, {ok: true});
        });
        server._doClose({
          id: "foo",
          holder: "holder"
        }, from);
        assert.notOk(server._semaphores.get("foo").timeouts().has("holder"));
        server._safeReply.restore();
      });

      it("Should fail to clear semaphore after timeout, semaphore doesn't exist", function () {
        var oldSemaphores = server._semaphores;
        server._clearSem("id", "holder");
        assert.ok(_.isEqual(oldSemaphores, server._semaphores));
      });

      it("Should fail to clear semaphore after timeout, holder doesn't exist at semaphore", function () {
        server._semaphores.set("id", new Semaphore("id", 3, new Map([["holder2", null]])));
        var oldSemaphores = server._semaphores;
        server._clearSem("id", "holder");
        assert.ok(_.isEqual(oldSemaphores, server._semaphores));
      });

      it("Should clear semaphore after timeout", function () {
        server._semaphores.set("id", new Semaphore("id", 3, new Map([["holder", null], ["holder2", null]])));
        server._clearSem("id", "holder");
        assert.notOk(server._semaphores.get("id").timeouts().has("holder"));
        server._clearSem("id", "holder2");
        assert.notOk(server._semaphores.get("id").timeouts().has("holder2"));
      });
      
      it("Should reply with an error", function (done) {
        sinon.stub(server, "_safeReply", (from, rstream) => {
          rstream.once("error", (err) => {
            assert.ok(err);
            async.nextTick(done);
          });
        });
        server._errorReply({}, new Error("foo"));
      });
    });

    describe("DSMServer static tests", function () {
      it("Should fail to parse an incoming job, not an object", function () {
        var out = DSMServer.parseJob("foo", "rlock");
        assert.ok(out instanceof Error);
      });

      it("Should fail to parse an incoming job, unsupported command", function () {
        var out = DSMServer.parseJob({}, "foo");
        assert.ok(out instanceof Error);
      });

      it("Should fail to parse an incoming job, bad type match", function () {
        var out = DSMServer.parseJob({
          id: 5,
          holder: "asdf"
        }, "close");
        assert.ok(out instanceof Error);

        out = DSMServer.parseJob({
          id: "asdf",
          holder: 5
        }, "close");
        assert.ok(out instanceof Error);
      });

      it("Should parse an incoming job", function () {
        var obj = {
          id: "foo",
          holder: "bar"
        };
        assert.deepEqual(DSMServer.parseJob(obj, "close"), obj);
      });

      it("Should calculate wait time for a retry", function () {
        var out = DSMServer.calculateWaitTime(0, 100);
        assert.ok(out >= 0 && out <= 100);
      });
    });
  });
};

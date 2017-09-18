var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    stream = require("stream"),
    sinon = require("sinon"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("CommandServer unit tests", function () {
    var CommandServer = lib.command_server,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        CHash = lib.chash,
        VectorClock = lib.vclock,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("CommandServer state tests", function () {
      var kernel,
          gossip,
          server,
          chash,
          vclock,
          opts,
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
        server = new CommandServer(gossip, kernel);
      });

      it("Should start generic server with name", function () {
        var name = "foo";
        server.start(name);
        assert.equal(server._id, name);
        assert.lengthOf(server.kernel().listeners(name), 1);
        assert.lengthOf(server.listeners("pause"), 1);
        server.stop();
      });

      it("Should start generic server w/o name", function () {
        var id = server.id();
        server.start();
        assert.equal(server._id, id);
        assert.lengthOf(server.kernel().listeners(id), 1);
        assert.lengthOf(server.listeners("pause"), 1);
        server.stop();
      });

      it("Should stop generic server", function (done) {
        var name = "foo";
        server.start(name);
        server.once("stop", () => {
          assert.equal(server._streams.size, 0);
          assert.lengthOf(server.listeners("pause"), 0);
          assert.lengthOf(server.kernel().listeners(name), 0);
          done();
        });
        server.stop();
      });

      it("Should forcefully stop generic server", function (done) {
        var name = "foo";
        server.start(name);
        server.once("stop", () => {
          assert.equal(server._streams.size, 0);
          assert.lengthOf(server.listeners("pause"), 0);
          assert.lengthOf(server.kernel().listeners(name), 0);
          done();
        });
        server.stop(true);
      });

      it("Should wait for idle event before stopping", function (done) {
        var name = "foo";
        server.start(name);
        server.once("stop", () => {
          assert.equal(server._streams.size, 0);
          assert.lengthOf(server.listeners("pause"), 0);
          assert.lengthOf(server.kernel().listeners(name), 0);
          done();
        });
        server._streams.set("foo", "bar");
        server.stop();
        server._streams.clear();
        server.emit("idle");
      });
    });

    describe("CommandServer operation tests", function () {
      var kernel,
          gossip,
          server,
          chash,
          vclock, 
          opts,
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
        server = new CommandServer(gossip, kernel);
        server.start("foo");
      });

      afterEach(function (done) {
        server.stop(true);
        done();
      });

      after(function () {
        kernel.stop();
      });

      it("Should decode job", function () {
        var job = Buffer.from(JSON.stringify({event: "inspect"}));
        var out = server.decodeJob(job);
        assert.deepEqual(out, {event: "inspect", data: undefined});

        out = server.decodeJob(Buffer.from("foo"));
        assert.ok(out instanceof Error);

        out = server.decodeJob(Buffer.from(JSON.stringify({event: "get"})));
        assert.ok(out instanceof Error);

        out = server.decodeJob(Buffer.from(JSON.stringify({event: "not defined"})));
        assert.ok(out instanceof Error);
      });

      it("Should decode singleton", function () {
        var job = {event: "inspect"};
        var out = server.decodeSingleton(job);
        assert.deepEqual(out, {event: "inspect", data: undefined});

        out = server.decodeSingleton({event: "get"});
        assert.ok(out instanceof Error);

        out = server.decodeSingleton({event: "not defined"});
        assert.ok(out instanceof Error);
      });

      it("Should skip parsing stream if command comes from cluster node", function () {
        var data = Buffer.from(JSON.stringify({ok: true}));
        var stream = {stream: uuid.v4(), done: false};
        assert.notOk(server.streams().has(stream.stream));
        server._parse(data, stream, {node: kernel.self()});
        assert.notOk(server.streams().has(stream.stream));

        kernel.sinks().set("id2", "test string");
        server._parse(data, stream, {node: new Node("id2")});
        assert.notOk(server.streams().has(stream.stream));
        kernel.sinks().delete("id2");
      });

      it("Should parse stream successfully when message doesn't come from cluster", function () {
        var data = Buffer.from(JSON.stringify({ok: true}));
        var stream = {stream: uuid.v4(), done: false};
        assert.notOk(server.streams().has(stream.stream));
        server._parse(data, stream, {node: new Node("id2")});
        assert.ok(server.streams().has(stream.stream));
      });

      it("Should encode a reply to a client socket", function () {
        var from = {
          node: new Node("id2"),
          tag: null,
          socket: "socket"
        };
        var msg = {id: "id"};
        kernel.ipc().server.once("socket", (event, msg) => {
          assert.equal(event, "message");
          assert.deepEqual(msg, NetKernel._encodeMsg(kernel.cookie(), msg));
        });
        server._encodedReply(from, msg);
      });
    });

    describe("CommandServer command tests", function () {
      var kernel,
          gossip,
          server,
          chash,
          vclock, 
          opts,
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
        server = new CommandServer(gossip, kernel);
        server.start("foo");
        sinon.stub(server, "_encodedReply").returnsArg(1);
      });

      afterEach(function (done) {
        server._encodedReply.restore();
        server.stop(true);
        done();
      });

      after(function () {
        kernel.stop();
      });

      it("Should fail to join a ring", function () {
        var data = {id: "ring"};
        var out = server.join(data, from);
        assert.equal(out.ok, false);
        assert.isObject(out.error);
      });

      it("Should successfully join a ring", function (done) {
        sinon.stub(gossip, "join").returns(0);
        var data = {id: "ring"};
        var out = server.join(data, from);
        assert.equal(out.ok, true);
        gossip.join.restore();
        done();
      });

      it("Should meet a node in a ring", function (done) {
        var data = {node: new Node("foo", "localhost", 8000)};
        sinon.stub(gossip, "meet");
        var out = server.meet(data, from);
        assert.equal(out.ok, true);
        gossip.meet.restore();
        done();
      });

      it("Should leave a ring, default force to false", function (done) {
        sinon.stub(gossip, "leave", (force) => {
          assert.equal(force, false);
        });
        var data = {};
        var out = server.leave(data, from);
        assert.equal(out.ok, true);
        gossip.leave.restore();
        done();
      });

      it("Should leave a ring, force is true", function (done) {
        sinon.stub(gossip, "leave", (force) => {
          assert.equal(force, true);
        });
        var data = {force: true};
        var out = server.leave(data, from);
        assert.equal(out.ok, true);
        gossip.leave.restore();
        done();
      });

      it("Should insert a node into a ring, default force", function (done) {
        var data = {node: new Node("foo", "localhost", 8000)};
        sinon.stub(gossip, "insert", (node, force) => {
          assert.ok(node.equals(data.node));
          assert.equal(force, false);
        });
        var out = server.insert(data, from);
        assert.equal(out.ok, true);
        gossip.insert.restore();
        done();
      });

      it("Should insert a node into a ring, default force", function (done) {
        var data = {node: new Node("foo", "localhost", 8000), force: true};
        sinon.stub(gossip, "insert", (node, force) => {
          assert.ok(node.equals(data.node));
          assert.equal(force, true);
        });
        var out = server.insert(data, from);
        assert.equal(out.ok, true);
        gossip.insert.restore();
        done();
      });

      it("Should minsert nodes into a ring, default force", function (done) {
        var data = {nodes: [new Node("foo", "localhost", 8000)]};
        sinon.stub(gossip, "minsert", (nodes, force) => {
          assert.lengthOf(nodes, 1);
          assert.equal(force, false);
        });
        var out = server.minsert(data, from);
        assert.equal(out.ok, true);
        gossip.minsert.restore();
        done();
      });

      it("Should minsert nodes into a ring, force is true", function (done) {
        var data = {nodes: [new Node("foo", "localhost", 8000)], force: true};
        sinon.stub(gossip, "minsert", (nodes, force) => {
          assert.lengthOf(nodes, 1);
          assert.equal(force, true);
        });
        var out = server.minsert(data, from);
        assert.equal(out.ok, true);
        gossip.minsert.restore();
        done();
      });

      it("Should minsert nodes into a ring, filter out this node", function (done) {
        var data = {nodes: [kernel.self()]};
        sinon.stub(gossip, "minsert", (nodes, force) => {
          assert.lengthOf(nodes, 0);
          assert.equal(force, false);
        });
        var out = server.minsert(data, from);
        assert.equal(out.ok, true);
        gossip.minsert.restore();
        done();
      });

      it("Should remove a node from a ring, default force", function (done) {
        var data = {node: new Node("foo", "localhost", 8000)};
        sinon.stub(gossip, "remove", (node, force) => {
          assert.ok(node.equals(data.node));
          assert.equal(force, false);
        });
        var out = server.remove(data, from);
        assert.equal(out.ok, true);
        gossip.remove.restore();
        done();
      });

      it("Should remove a node from a ring, force is true", function (done) {
        var data = {node: new Node("foo", "localhost", 8000), force: true};
        sinon.stub(gossip, "remove", (node, force) => {
          assert.ok(node.equals(data.node));
          assert.equal(force, true);
        });
        var out = server.remove(data, from);
        assert.equal(out.ok, true);
        gossip.remove.restore();
        done();
      });

      it("Should mremove nodes from a ring, default force", function (done) {
        var data = {nodes: [new Node("foo", "localhost", 8000)]};
        sinon.stub(gossip, "mremove", (nodes, force) => {
          assert.lengthOf(nodes, 1);
          assert.equal(force, false);
        });
        var out = server.mremove(data, from);
        assert.equal(out.ok, true);
        gossip.mremove.restore();
        done();
      });

      it("Should mremove nodes from a ring, force is true", function (done) {
        var data = {nodes: [new Node("foo", "localhost", 8000)], force: true};
        sinon.stub(gossip, "mremove", (nodes, force) => {
          assert.lengthOf(nodes, 1);
          assert.equal(force, true);
        });
        var out = server.mremove(data, from);
        assert.equal(out.ok, true);
        gossip.mremove.restore();
        done();
      });

      it("Should mremove nodes from a ring, filter out this node", function (done) {
        var data = {nodes: [kernel.self()]};
        sinon.stub(gossip, "mremove", (nodes, force) => {
          assert.lengthOf(nodes, 0);
          assert.equal(force, false);
        });
        var out = server.mremove(data, from);
        assert.equal(out.ok, true);
        gossip.mremove.restore();
        done();
      });

      it("Should inspect a ring", function (done) {
        var data = {};
        var out = server.inspect(data, from);
        assert.equal(out.ok, true);
        assert.deepEqual(out.data, gossip.ring().toJSON(true));
        done();
      });

      it("Should return if ring has a node", function (done) {
        var data = {id: id};
        var out = server.has(data, from);
        assert.equal(out.ok, true);
        assert.equal(out.data, true);

        data.id = id + "1";
        out = server.has(data, from);
        assert.equal(out.ok, true);
        assert.equal(out.data, false);
        done();
      });

      it("Should return error if checking state of node that doesn't exist", function (done) {
        var data = {id: id + "1"};
        var out = server.get(data, from);
        assert.equal(out.ok, false);
        assert.isObject(out.error);
        done();
      });

      it("Should return state of node in ring", function (done) {
        var data = {id: id};
        var out = server.get(data, from);
        assert.equal(out.ok, true);
        assert.deepEqual(out.data, kernel.self().toJSON(true));
        done();
      });
    });
  });
};

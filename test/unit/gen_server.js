var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    stream = require("stream"),
    sinon = require("sinon"),
    fs = require("fs"),
    microtime = require("microtime"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("GenServer unit tests", function () {
    var GenServer = lib.gen_server,
        NetKernel = lib.kernel,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("GenServer state tests", function () {
      var kernel,
          server,
          opts,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function () {
        kernel = new NetKernel(new MockIPC(), id, host, port);
      });

      beforeEach(function () {
        server = new GenServer(kernel);
      });

      it("Should construct a generic server", function () {
        assert.deepEqual(server._kernel, kernel);
        assert(server._streams instanceof Map);
        assert.equal(server._streams.size, 0);
      });

      it("Should grab id of generic server", function () {
        var id = server._id;
        assert.deepEqual(server.id(), id);
      });

      it("Should set id of generic server", function () {
        server.id("foo");
        assert.deepEqual(server.id(), "foo");
      });

      it("Should grab kernel of gossip ring", function () {
        assert.deepEqual(server.kernel(), kernel);
      });

      it("Should set kernel of gossip ring", function () {
        var kernel2 = new NetKernel(new MockIPC(), "id2", host, port+1);
        server.kernel(kernel2);
        assert.deepEqual(server._kernel, kernel2);
      });

      it("Should grab active streams of gossip ring", function () {
        assert(server.streams() instanceof Map);
        assert.equal(server.streams().size, 0);
      });

      it("Should set active streams of gossip ring", function () {
        var streams = new Map([["key", "value"]]);
        server.streams(streams);
        assert(server.streams() instanceof Map);
        assert.equal(server.streams().size, 1);
      });

      it("Should return if gossip process is idle or not", function () {
        assert.ok(server.idle());
        var streams = new Map([["key", "value"]]);
        server.streams(streams);
        assert.notOk(server.idle());
      });

      it("Should fail to start generic server with existent name", function () {
        var name = "foo";
        var handler = _.identity;
        server.kernel().once(name, handler);
        var out;
        try {
          out = server.start(name);
        } catch (e) {
          out = e;
        }
        assert.ok(out instanceof Error);
        server.kernel().removeListener(name, handler);
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

      it("Should decode job", function () {
        var job = Buffer.from(JSON.stringify({event: "foo", data: "bar", hello: "world"}));
        var out = server.decodeJob(job);
        assert.deepEqual(out, {event: "foo", data: "bar"});

        out = server.decodeJob(Buffer.from("foo"));
        assert.ok(out instanceof Error);
      });
    });

    describe("GenServer operation tests", function () {
      var kernel,
          nKernel,
          server,
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
        }, () => {
          kernel.connect(nKernel.self());
          nKernel.connect(kernel.self());
          done();
        });
      });

      beforeEach(function () {
        var node = new Node(id, host, port);
        // var node2 = new Node("id2", host, port+1);
        server = new GenServer(kernel);
        server.start("foo");
      });

      afterEach(function (done) {
        server.stop(true);
        done();
      });

      after(function () {
        kernel.stop();
        nKernel.stop();
      });

      it("Should parse incoming job streams", function () {
        var data = Buffer.from(JSON.stringify({ok: true}));
        var stream = {stream: uuid.v4(), done: false};
        assert.notOk(server.streams().has(stream.stream));
        server._parse(data, stream, {});
        assert.ok(server.streams().has(stream.stream));
        assert.equal(Buffer.compare(data, server.streams().get(stream.stream)), 0);
      });

      it("Should parse incoming job streams, with existing stream data", function () {
        var data = Buffer.from(JSON.stringify({ok: true}));
        var stream = {stream: uuid.v4(), done: false};
        var init = Buffer.from("foo");
        server.streams().set(stream.stream, init);
        server._parse(data, stream, {});
        assert.ok(server.streams().has(stream.stream));
        var exp = Buffer.concat([init, data], init.length + data.length);
        assert.equal(Buffer.compare(exp, server.streams().get(stream.stream)), 0);
      });

      it("Should skip parsing full job if stream errors", function () {
        sinon.spy(server, "decodeJob");
        var data = Buffer.from(JSON.stringify({ok: true}));
        var stream = {stream: uuid.v4(), error: {foo: "bar"}, done: true};
        var init = Buffer.from("foo");
        server.streams().set(stream.stream, init);
        server._parse(data, stream, {});
        assert.notOk(server.streams().has(stream.stream));
        assert.notOk(server.decodeJob.called);
        server.decodeJob.restore();
      });

      it("Should parse a full job", function () {
        sinon.stub(server, "emit", (event, val) => {
          if (event === "idle") return;
          assert.equal(event, "msg");
          assert.equal(val, "foo");
        });
        var data = Buffer.from(JSON.stringify({
          event: "msg",
          data: "foo"
        }));
        var stream = {stream: uuid.v4(), done: false};
        server._parse(data, stream, {});
        server._parse(data, {stream: stream.stream, done: true}, {});
        assert.notOk(server._streams.has(stream.stream));
        assert.ok(server.emit.called);
        server.emit.restore();
      });

      it("Should parse full job, not emitting idle event", function () {
        server._streams.set("foo", "bar");
        sinon.stub(server, "emit");
        var data = Buffer.from(JSON.stringify({
          event: "msg",
          data: "foo"
        }));
        var stream = {stream: uuid.v4(), done: false};
        server._parse(data, stream, {});
        server._parse(data, {stream: stream.stream, done: true}, {});
        assert.notOk(server._streams.has(stream.stream));
        assert.notOk(server.emit.calledWith(["idle"]));
        server.emit.restore();
      });

      it("Should decode incoming job", function () {
        var buf = Buffer.from(JSON.stringify({
          event: "msg",
          data: "foo"
        }));
        var out = server.decodeJob(buf);
        assert.equal(out.event, "msg");
        assert.equal(out.data, "foo");
      });

      it("Should parse recipient as local node", function () {
        var id = "id";
        var out = server._parseRecipient(id);
        assert.equal(out.id, id);
        assert.ok(out.node.equals(kernel.self()));
      });

      it("Should parse recipient as another node", function () {
        var input = {id: "id", node: kernel.self()};
        var out = server._parseRecipient(input);
        assert.equal(out.id, input.id);
        assert.ok(out.node.equals(input.node));
      });

      it("Should call another generic server", function (done) {
        var server2 = new GenServer(kernel);
        server2.start("bar");
        server2.on("msg", (data, from) => {
          server2.reply(from, data);
        });
        server.call("bar", "msg", Buffer.from("hello"), (err, res) => {
          assert.notOk(err);
          assert.equal(Buffer.compare(res, Buffer.from("hello")), 0);
          server2.stop(true);
          done();
        }, 5000);
      });

      it("Should multicall generic servers", function (done) {
        var server2 = new GenServer(kernel);
        server2.start("bar");
        server2.on("msg", (data, from) => {
          server2.reply(from, data);
        });
        var server3 = new GenServer(nKernel);
        server3.start("bar");
        server3.on("msg", (data, from) => {
          server3.reply(from, data);
        });
        var data = Buffer.from("hello");
        server.multicall([kernel.self(), nKernel.self()], "bar", "msg", data, (err, res) => {
          assert.notOk(err);
          assert.isArray(res);
          assert.lengthOf(res, 2);
          assert.equal(Buffer.compare(res[0], Buffer.from("hello")), 0);
          assert.equal(Buffer.compare(res[1], Buffer.from("hello")), 0);
          server2.stop(true);
          server3.stop(true);
          done();
        }, 5000);
      });

      it("Should cast to another generic server", function (done) {
        var server2 = new GenServer(kernel);
        server2.start("bar");
        server2.on("msg", (data, from) => {
          assert.ok(from.node.equals(kernel.self()));
          assert.equal(Buffer.compare(data, Buffer.from("hello")), 0);
          server2.stop(true);
          done();
        });
        server.cast("bar", "msg", Buffer.from("hello"));
      });

      it("Should multicast to generic servers", function (done) {
        var server2 = new GenServer(kernel);
        server2.start("bar");
        var count = 0;
        server2.on("msg", (data, from) => {
          count++;
          assert.ok(from.node.equals(kernel.self()));
          assert.equal(Buffer.compare(data, Buffer.from("hello")), 0);
          server2.stop(true);
          if (count === 2) done();
        });
        var server3 = new GenServer(nKernel);
        server3.start("bar");
        server3.on("msg", (data, from) => {
          count++;
          assert.ok(from.node.equals(kernel.self()));
          assert.equal(Buffer.compare(data, Buffer.from("hello")), 0);
          server3.stop(true);
          if (count === 2) done();
        });
        server.abcast([kernel.self(), nKernel.self()], "bar", "msg", Buffer.from("hello"));
      });
    });
  });
};

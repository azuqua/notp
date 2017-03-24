var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    stream = require("stream"),
    crypto = require("crypto"),
    sinon = require("sinon"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("NetKernel unit tests", function () {
    var NetKernel = lib.kernel;
    var Node = lib.node;
    var Connection = lib.conn;
    var MockIPC = mocks.ipc;

    describe("NetKernel state tests", function () {
      var kernel,
          id = "id",
          host = "localhost",
          port = 8000;

      beforeEach(function () {
        kernel = new NetKernel(new MockIPC(), id, host, port);
      });

      it("Should construct a netkernel", function () {
        assert.equal(kernel._id, id);
        assert.equal(kernel._host, host);
        assert.equal(kernel._port, port);
        var node = kernel._self;
        assert.equal(node.id(), id);
        assert.equal(node.host(), host);
        assert.equal(node.port(), port);
        assert.equal(kernel._srcs.size, 0);
        assert.equal(kernel._sinks.size, 0);
      });

      it("Should grab id of netkernel", function () {
        assert.equal(kernel.id(), id);
      });

      it("Should set id of netkernel", function () {
        kernel.id("id2");
        assert.equal(kernel.id(), "id2");
        assert.equal(kernel._self.id(), "id2");
      });

      it("Should grab host of netkernel", function () {
        assert.equal(kernel.host(), host);
      });

      it("Should set host of netkernel", function () {
        kernel.host("localhost2");
        assert.equal(kernel.host(), "localhost2");
        assert.equal(kernel._self.host(), "localhost2");
      });

      it("Should grab port of netkernel", function () {
        assert.equal(kernel.port(), port);
      });

      it("Should set port of netkernel", function () {
        kernel.port(8001);
        assert.equal(kernel.port(), 8001);
        assert.equal(kernel._self.port(), 8001);
      });

      it("Should grab self-referential node of netkernel", function () {
        var node = kernel.self();
        assert.equal(node.id(), id);
        assert.equal(node.host(), host);
        assert.equal(node.port(), port);
      });

      it("Should set self-referential node of netkernel", function () {
        var node = new Node("id2", "localhost2", 8001);
        kernel.self(node);
        var self = kernel.self();
        assert.equal(self.id(), node.id());
        assert.equal(self.host(), node.host());
        assert.equal(self.port(), node.port());
        assert.equal(kernel.id(), node.id());
        assert.equal(kernel.host(), node.host());
        assert.equal(kernel.port(), node.port());
      });

      it("Should grab node sources of netkernel", function () {
        var sources = kernel.sources();
        assert(sources instanceof Map);

        kernel._srcs = new Map([["id", new Node("id", "localhost", 8000)]]);
        sources = kernel.sources();
        assert(sources instanceof Map);
        assert.equal(sources.size, 1);
      });

      it("Should set node sources of netkernel", function () {
        var map = new Map([["id", new Node("id", "localhost", 8000)]]);
        kernel.sources(map);
        var sources = kernel.sources();
        assert(sources instanceof Map);
        assert.equal(sources.size, 1);
        sources.forEach((val, key) => {
          assert(map.has(key));
          assert.deepEqual(map.get(key), val);
        });
      });

      it("Should grab node sinks of netkernel", function () {
        var sinks = kernel.sinks();
        assert(sinks instanceof Map);

        kernel._sinks = new Map([["id", new Node("id", "localhost", 8000)]]);
        sinks = kernel.sinks();
        assert(sinks instanceof Map);
        assert.equal(sinks.size, 1);
      });

      it("Should set node sinks of netkernel", function () {
        var map = new Map([["id", new Node("id", "localhost", 8000)]]);
        kernel.sinks(map);
        var sinks = kernel.sinks();
        assert(sinks instanceof Map);
        assert.equal(sinks.size, 1);
        sinks.forEach((val, key) => {
          assert(map.has(key));
          assert.deepEqual(map.get(key), val);
        });
      });

      it("Should grab node cookie", function () {
        var cookie = kernel.cookie();
        assert.equal(cookie, null);

        kernel._cookie = "foo";
        cookie = kernel.cookie();
        assert(typeof cookie === "string");
        assert.equal(cookie, "foo");
      });

      it("Should set node cookie", function () {
        kernel.cookie("foo");
        var cookie = kernel.cookie();
        assert(typeof cookie === "string");
        assert.equal(cookie, "foo");

        kernel.cookie(5);
        cookie = kernel.cookie();
        assert.equal(cookie, "foo");
      });

      it("Should initialize ipc server", function (done) {
        var opts = {retry: 500, maxRetries: false};
        kernel.start(opts);
        kernel.on("_ready", () => {
          assert.equal(kernel._cookie, null);
          kernel._ipc.network().delete(kernel.self().id());
          done();
        });
      });

      it("Should stop ipc server", function (done) {
        var opts = {retry: 500, maxRetries: false};
        kernel.start(opts);
        kernel.on("_ready", () => {
          kernel.stop();
        });
        kernel.on("_stop", done);
      });

      it("Should start ipc message registry", function (done) {
        kernel._ipc.config.retry = 500;
        kernel._ipc.config.maxRetries = false;
        kernel._ipc.serveNet(kernel.host(), kernel.port(), () => {
          kernel.on("_ready", () => {
            assert.equal(kernel._ipc.server.listeners("message").length, 1);
            assert.equal(kernel._ipc.server.listeners("socket.disconnected").length, 1);
            kernel._ipc.network().delete(kernel.self().id());
            done();
          });
          kernel._startData();
        });
        kernel._ipc.server.start();
      });
    });

    describe("NetKernel routing tests", function () {
      var kernel,
          ipc,
          id = "id2",
          host = "localhost",
          port = 8001,
          opts = {retry: 500, maxRetries: false},
          other = new Node("self", "localhost", 8002);

      before(function (done) {
        ipc = new MockIPC();
        kernel = new NetKernel(ipc, id, host, port);
        kernel.start(opts);
        kernel.sinks().set("self", new Connection(ipc, other));
        kernel.on("_ready", done);
      });

      after(function () {
        kernel.stop();
      });

      it("Should route messages through ipc server, setting source", function (done) {
        var msgID = uuid.v4();
        var buf = Buffer.from("baz");
        var stream = {id: "bar", done: false};
        ipc.connectToNet(id, host, port, () => {
          _.times(2, () => {
            ipc.of[id].emit("message", {
              from: other.toJSON(true),
              id: msgID,
              stream: stream,
              data: buf.toJSON()
            });
          });
        });

        var count = 0, socket;
        assert.notOk(kernel.sources().has(other.id()));

        kernel.on(msgID, (data, str, from) => {
          assert(Buffer.isBuffer(data));
          assert.equal(Buffer.compare(data, buf), 0);
          assert.deepEqual(str, stream);
          assert.deepEqual(_.pick(from, ["tag", "node"]), {
            tag: null,
            node: other
          });
          if (count === 0) {
            assert.equal(kernel.sources().size, 1);
            socket = kernel.sources().values().next().value;
            count++;
          }
          else {
            assert.equal(kernel.sources().size, 1);
            assert.deepEqual(kernel.sources().get(socket.id), socket);
            ipc.disconnect(id);
            kernel.removeAllListeners(msgID);
            done();
          }
        });
      });
    });

    describe("NetKernel external connection tests", function () {
      var kernel,
          nKernel,
          ipc,
          opts = {retry: 500, maxRetries: false};

      before(function (done) {
        async.map([
          {id: "foo", host: "localhost", port: 8000},
          {id: "bar", host: "localhost", port: 8001}
        ], (val, next) => {
          var ipc = new MockIPC();
          var inner = new NetKernel(ipc, val.id, val.host, val.port);
          inner.start(opts);
          inner.on("_ready", _.partial(next, null, inner));
        }, function (err, out) {
          assert.notOk(err);
          kernel = out[0];
          nKernel = out[1];
          done();
        });
      });

      afterEach(function () {
        kernel.disconnect(nKernel.self());
        nKernel.disconnect(kernel.self());
      });

      after(function () {
        kernel.stop();
        nKernel.stop();
      });

      it("Should connect to external node asynchronously", function (done) {
        var node = nKernel.self();
        kernel.connect(node);
        assert.equal(kernel.sinks().size, 1);
        var conn = kernel.connection(node);
        conn.once("connect", done);
      });

      it("Should connect to an external node", function (done) {
        var node = nKernel.self();
        kernel.connect(node, () => {
          assert.deepEqual(kernel.sinks().get(node.id()).node(), node);
          assert.equal(kernel.sinks().size, 1);
          done();
        });
      });

      it("Should skip connecting to node if node is self", function (done) {
        kernel.connect(kernel.self(), () => {
          assert.equal(kernel.sinks().size, 0);
          done();
        });
      });

      it("Should skip connecting to node if already connected", function (done) {
        var node = nKernel.self();
        async.timesSeries(2, (val, next) => {
          kernel.connect(node, () => {
            assert.deepEqual(kernel.sinks().get(node.id()).node(), node);
            assert.equal(kernel.sinks().size, 1);
            next();
          });
        }, done);
      });

      it("Should disconnect from an external node", function (done) {
        var node = nKernel.self();
        var out = kernel.connect(node, () => {
          kernel.disconnect(node);
          assert.equal(kernel.sinks().size, 0);
          done();
        });
      });

      it("Should be connected to self", function () {
        assert(kernel.isConnected(kernel.self()));
      });

      it("Should return if kernel is connected to node or not", function (done) {
        var node = nKernel.self();
        assert.notOk(kernel.isConnected(node));
        kernel.connect(node, () => {
          assert(kernel.isConnected(node));
          done();
        });
      });

      it("Should return connection to external node if one exists", function (done) {
        var node = nKernel.self();
        kernel.connect(node, () => {
          var conn = kernel.connection(node);
          assert.ok(conn);
          done();
        });
      });

      it("Should return null on kernel.connection to self connection", function () {
        assert.equal(kernel.connection(kernel.self()), null);
      });

      it("Should return null on kernel.connection to non-existant connection", function () {
        assert.equal(kernel.connection(nKernel.self()), null);
      });

      it("Should add socket if socket id doesn't exist", function () {
        kernel._addSocket({id: "foo"});
        assert.equal(kernel.sources().size, 1);
        kernel.sources().delete("foo");
      });

      it("Should not add socket if socket id already exists", function () {
        kernel._addSocket({id: "foo"});
        assert.equal(kernel.sources().size, 1);
        kernel._addSocket({id: "foo"});
        assert.equal(kernel.sources().size, 1);
        kernel.sources().delete("foo");
      });

      it("Should disconnect from socket", function () {
        kernel._addSocket({id: "foo"});
        assert.equal(kernel.sources().size, 1);
        kernel._disconnectSocket("foo");
        assert.equal(kernel.sources().size, 1);
      });
    });

    describe("NetKernel communication protocol tests", function () {
      var kernel,
          nKernel,
          opts = {retry: 500, maxRetries: false};

      before(function (done) {
        async.map([
          {id: "foo", host: "localhost", port: 8000},
          {id: "bar", host: "localhost", port: 8001}
        ], (val, next) => {
          var ipc = new MockIPC();
          var inner = new NetKernel(ipc, val.id, val.host, val.port);
          inner.start(opts);
          inner.on("_ready", _.partial(next, null, inner));
        }, (err, out) => {
          assert.notOk(err);
          kernel = out[0];
          nKernel = out[1];
          async.times(2, (val, next) => {
            var first = out[val];
            var second = out[(val+1)%2];
            first.connect(second.self(), next);
          }, done);
        });
      });

      after(function () {
        kernel.stop();
        nKernel.stop();
      });

      it("Should make async call to internal node with buffer", function (done) {
        var data = Buffer.from("hello");
        var acc = Buffer.from("");
        var job = uuid.v4();
        kernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            done();
          }
        });
        kernel.cast(kernel.self(), job, data);
      });

      it("Should make async call to internal node with stream", function (done) {
        var data = new stream.PassThrough();
        var acc = Buffer.from("");
        var job = uuid.v4();
        kernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            done();
          }
        });
        kernel.cast(kernel.self(), job, data);
        async.nextTick(() => {
          data.write(Buffer.from("hello"));
          data.end();
        });
      });

      it("Should make async call to external node with buffer", function (done) {
        var data = Buffer.from("hello");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            done();
          }
        });
        kernel.cast(nKernel.self(), job, data);
      });

      it("Should make async call to external node with stream", function (done) {
        var data = new stream.PassThrough();
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            done();
          }
        });
        kernel.cast(nKernel.self(), job, data);
        async.nextTick(() => {
          data.write(Buffer.from("hello"));
          data.end();
        });
      });

      it("Should make async call to multiple external nodes with stream", function (done) {
        var data = new stream.PassThrough();
        var job = uuid.v4();
        var streams = new Map();
        var called = 0;
        nKernel.on(job, (data, stream, from) => {
          if (!streams.has(stream.stream)) {
            streams.set(stream.stream, Buffer.from(""));
          }
          var inner = streams.get(stream.stream);
          if (!stream.done) {
            inner = Buffer.concat([inner, data], inner.length + data.length);
            streams.set(stream.stream, inner);
          }
          else {
            assert.equal(Buffer.compare(inner, Buffer.from("hello")), 0);
            called++;
            if (called === 2) return done();
          }
        });
        kernel.abcast([nKernel.self(), nKernel.self()], job, data);
        async.nextTick(() => {
          data.write(Buffer.from("hello"));
          data.end();
        });
      });

      it("Should make sync call to external node with input buffer, return callback", function (done) {
        var data = Buffer.from("hello");
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            nKernel.cast(from.node, from.tag, exp);
          }
        });
        kernel.call(nKernel.self(), job, data, (err, out) => {
          assert.notOk(err);
          assert.equal(Buffer.compare(out, exp), 0);
          done();
        });
      });

      it("Should make sync call to external node with input buffer, return stream", function (done) {
        var data = Buffer.from("hello");
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            nKernel.cast(from.node, from.tag, exp);
          }
        });
        var rstream = kernel.call(nKernel.self(), job, data);
        var retAcc = Buffer.from("");
        rstream.on("data", (data) => {
          retAcc = Buffer.concat([retAcc, data], retAcc.length + data.length);
        });
        rstream.on("end", () => {
          assert.equal(Buffer.compare(retAcc, exp), 0);
          done();
        });
      });

      it("Should make sync call to external node with input stream, return callback", function (done) {
        var data = new stream.PassThrough();
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            nKernel.cast(from.node, from.tag, exp);
          }
        });
        kernel.call(nKernel.self(), job, data, (err, out) => {
          assert.notOk(err);
          assert.equal(Buffer.compare(out, exp), 0);
          done();
        });
        async.nextTick(() => {
          data.write(Buffer.from("hello"));
          data.end();
        });
      });

      it("Should make sync call to external node with input stream, return stream", function (done) {
        var data = new stream.PassThrough();
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            nKernel.cast(from.node, from.tag, exp);
          }
        });
        async.nextTick(() => {
          data.write(Buffer.from("hello"));
          data.end();
        });

        var rstream = kernel.call(nKernel.self(), job, data);
        var retAcc = Buffer.from("");
        rstream.on("data", (data) => {
          retAcc = Buffer.concat([retAcc, data], retAcc.length + data.length);
        });
        rstream.on("end", () => {
          assert.equal(Buffer.compare(retAcc, exp), 0);
          done();
        });
      });

      it("Should return an error if return stream errors", function (done) {
        var data = Buffer.from("hello");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, rcvStream, from) => {
          if (!rcvStream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            var rstream = new stream.PassThrough();
            async.nextTick(() => {
              rstream.emit("error", new Error("foo"));
              rstream.end();
            });
            nKernel.cast(from.node, from.tag, rstream);
          }
        });
        kernel.call(nKernel.self(), job, data, (err, out) => {
          assert.ok(err);
          done();
        });
      });

      it("Should fail to reply to sync call on external node with no tag", function (done) {
        var data = Buffer.from("hello");
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            var out;
            try {
              out = nKernel.reply({node: from.node, tag: null}, exp);
            } catch (e) {
              out = e;
            }
            assert(out instanceof Error);
            nKernel.reply(from, exp);
          }
        });
        kernel.call(nKernel.self(), job, data, (err, out) => {
          assert.notOk(err);
          assert.equal(Buffer.compare(out, exp), 0);
          done();
        });
      });

      it("Should reply to sync call on external node", function (done) {
        var data = Buffer.from("hello");
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("hello")), 0);
            nKernel.reply(from, exp);
          }
        });
        kernel.call(nKernel.self(), job, data, (err, out) => {
          assert.notOk(err);
          assert.equal(Buffer.compare(out, exp), 0);
          done();
        });
      });

      it("Should make sync call to multiple external nodes with buffer", function (done) {
        var data = Buffer.from("hello");
        var exp = Buffer.from("world");
        var acc = Buffer.from("");
        var job = uuid.v4();
        var streams = new Map();
        nKernel.on(job, (data, stream, from) => {
          if (!streams.has(stream.stream)) {
            streams.set(stream.stream, Buffer.from(""));
          }
          var inner = streams.get(stream.stream);
          if (!stream.done) {
            inner = Buffer.concat([inner, data], inner.length + data.length);
            streams.set(stream.stream, inner);
          }
          else {
            assert.equal(Buffer.compare(inner, Buffer.from("hello")), 0);
            nKernel.reply(from, exp);
          }
        });
        kernel.multicall([nKernel.self(), nKernel.self()], job, data, (err, out) => {
          assert.notOk(err);
          assert.lengthOf(out, 2);
          out.forEach((val) => {
            assert.equal(Buffer.compare(val, exp), 0);
          });
          done();
        });
      });

      it("Should stream data locally", function (done) {
        var pstream = new stream.PassThrough();
        var job = uuid.v4();
        async.nextTick(() => {
          pstream.write(Buffer.from("foo"));
          pstream.end();
        });
        var tag = null;
        
        var acc = Buffer.from("");
        kernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("foo")), 0);
            nKernel.removeAllListeners(job);
            done();
          }
        });

        var endBefore = pstream.listeners("end").length,
            dataBefore = pstream.listeners("data").length,
            errorBefore = pstream.listeners("error").length;
        kernel._streamLocal(job, tag, pstream);
        assert.equal(pstream.listeners("data").length, dataBefore+1);
        assert.equal(pstream.listeners("error").length, errorBefore+1);
        assert.equal(pstream.listeners("end").length, endBefore+1);
      });

      it("Should fail to stream data locally when input stream errors", function (done) {
        var pstream = new stream.PassThrough();
        var job = uuid.v4();
        async.nextTick(() => {
          pstream.emit("error", new Error("foo"));
          pstream.end();
        });
        var tag = null;
        
        var acc = Buffer.from("");
        kernel.once(job, (data, stream, from) => {
          assert.equal(data, null);
          assert.ok(stream.error);
          assert.equal(stream.done, true);
          assert.lengthOf(pstream.listeners("data"), 0);
          done();
        });

        var endBefore = pstream.listeners("end").length,
            dataBefore = pstream.listeners("data").length,
            errorBefore = pstream.listeners("error").length;
        kernel._streamLocal(job, tag, pstream);
        assert.equal(pstream.listeners("data").length, dataBefore+1);
        assert.equal(pstream.listeners("error").length, errorBefore+1);
        assert.equal(pstream.listeners("end").length, endBefore+1);
      });

      it("Should send data locally", function (done) {
        var job = uuid.v4();
        var buf = Buffer.from("foo");
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: false};
        kernel.once(job, (data, rstream, from) => {
          assert.deepEqual(stream, rstream);
          assert.deepEqual(from, {
            tag: tag,
            node: kernel.self()
          });
          assert.equal(Buffer.compare(data, buf), 0);
          done();
        });
        kernel._sendLocal(job, tag, stream, buf);
      });

      it("Should send end locally", function (done) {
        var job = uuid.v4();
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: false};
        kernel.once(job, (data, rstream, from) => {
          assert.equal(stream.stream, rstream.stream);
          assert.equal(stream.done, true);
          assert.equal(rstream.done, true);
          assert.deepEqual(from, {
            tag: tag,
            node: kernel.self()
          });
          assert.equal(data, null);
          done();
        });
        kernel._finishLocal(job, tag, stream);
      });

      it("Should skip sending end if 'error' already emitted", function () {
        var job = uuid.v4();
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: true};
        kernel._finishLocal(job, tag, stream);
      });

      it("Should send error locally", function (done) {
        var job = uuid.v4();
        var err = new Error("foo");
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: false};
        var dstream = _.defaults({
          error: lib.utils.errorToObject(err),
          done: true
        }, stream);
        kernel.once(job, (data, rstream, from) => {
          assert.deepEqual(rstream, dstream);
          assert.deepEqual(from, {
            tag: tag,
            node: kernel.self()
          });
          assert.equal(data, null);
          done();
        });
        kernel._sendLocalError(job, tag, stream, err);
      });

      it("Should skip sending error if 'done' already emitted", function () {
        var job = uuid.v4();
        var err = new Error("foo");
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: true};
        kernel._sendLocalError(job, tag, stream, err);
      });

      it("Should stream data externally", function (done) {
        var pstream = new stream.PassThrough();
        var job = uuid.v4();
        async.nextTick(() => {
          pstream.write(Buffer.from("foo"));
          pstream.end();
        });
        var node = nKernel.self();
        var tag = null;
        var endBefore = pstream.listeners("end").length,
            dataBefore = pstream.listeners("data").length,
            errorBefore = pstream.listeners("error").length;
        kernel._streamData(node, job, tag, pstream);
        assert.equal(pstream.listeners("data").length, dataBefore+1);
        assert.equal(pstream.listeners("error").length, errorBefore+1);
        assert.equal(pstream.listeners("end").length, endBefore+1);

        var acc = Buffer.from("");
        nKernel.on(job, (data, stream, from) => {
          if (!stream.done) {
            acc = Buffer.concat([acc, data], acc.length + data.length);
          }
          else {
            assert.equal(Buffer.compare(acc, Buffer.from("foo")), 0);
            nKernel.removeAllListeners(job);
            done();
          }
        });
      });

      it("Should fail to stream data externally if stream errors", function (done) {
        var pstream = new stream.PassThrough();
        var job = uuid.v4();
        async.nextTick(() => {
          pstream.emit("error", new Error("foo"));
          pstream.end();
        });
        var node = nKernel.self();
        var tag = null;
        var endBefore = pstream.listeners("end").length,
            dataBefore = pstream.listeners("data").length,
            errorBefore = pstream.listeners("error").length;
        kernel._streamData(node, job, tag, pstream);
        assert.equal(pstream.listeners("data").length, dataBefore+1);
        assert.equal(pstream.listeners("error").length, errorBefore+1);
        assert.equal(pstream.listeners("end").length, endBefore+1);

        nKernel.once(job, (data, stream, from) => {
          assert.equal(data, null);
          assert.ok(stream.error);
          assert.equal(stream.done, true);
          assert.lengthOf(pstream.listeners("data"), 0);
          done();
        });
      });

      it("Should send data externally", function (done) {
        var node = nKernel.self();
        var job = uuid.v4();
        var buf = Buffer.from("foo");
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: false};
        var called = 0;
        var conn = kernel.connection(node);
        conn.once("send", (event, data) => {
          assert.equal(event, "message");
          assert.deepEqual(data, {
            id: job,
            tag: tag,
            from: kernel.self().toJSON(true),
            stream: stream,
            data: buf.toJSON()
          });
          conn._streams = new Map();
          called++;
          if (called === 2) return done(); 
        });
        nKernel.once(job, (data, rstream, from) => {
          assert.deepEqual(stream, rstream);
          assert.deepEqual(_.pick(from, ["node", "tag"]), {
            tag: tag,
            node: kernel.self()
          });
          assert.equal(Buffer.compare(data, buf), 0);
          called++;
          if (called === 2) return done();
        });
        kernel._sendData(conn, job, tag, stream, buf);
      });

      it("Should send end externally", function (done) {
        var node = nKernel.self();
        var job = uuid.v4();
        var tag = "tag";
        var stream = {stream: uuid.v4(), done: false};
        var called = 0;
        var conn = kernel.connection(node);
        conn.once("send", (event, data) => {
          assert.equal(event, "message");
          assert.deepEqual(data, {
            id: job,
            tag: tag,
            from: kernel.self().toJSON(true),
            stream: stream,
            data: null
          });
          conn._streams = new Map();
          called++;
          if (called === 2) return done(); 
        });
        nKernel.once(job, (data, rstream, from) => {
          assert.deepEqual(stream, rstream);
          assert.deepEqual(_.pick(from, ["node", "tag"]), {
            tag: tag,
            node: kernel.self()
          });
          assert.equal(data, null);
          called++;
          if (called === 2) return done();
        });
        kernel._finishData(conn, job, tag, stream);
      });

      it("Should skip end sending if 'error' already emitted", function () {
        var node = nKernel.self();
        var job = uuid.v4();
        var tag = "tag";
        var pstream = {stream: uuid.v4(), done: true};
        kernel._finishData(kernel.connection(node), job, tag, pstream);
      });

      it("Should send error externally", function (done) {
        var node = nKernel.self();
        var job = uuid.v4();
        var err = new Error("foo");
        var tag = "tag";
        var pstream = {stream: uuid.v4(), done: false};
        var dstream = _.defaults({
          error: lib.utils.errorToObject(err),
          done: true
        }, pstream);
        var called = 0;
        kernel.connection(node).once("send", (event, data) => {
          assert.equal(event, "message");
          assert.deepEqual(data, {
            id: job,
            tag: tag,
            from: kernel.self().toJSON(true),
            stream: {
              stream: pstream.stream,
              done: true,
              error: lib.utils.errorToObject(err)
            },
            data: null
          });
          called++;
          if (called === 2) return done(); 
        });
        nKernel.once(job, (data, rstream, from) => {
          assert.deepEqual(rstream, dstream);
          assert.deepEqual(_.pick(from, ["node", "tag"]), {
            tag: tag,
            node: kernel.self()
          });
          assert.equal(data, null);
          called++;
          if (called === 2) return done();
        });
        kernel._sendError(kernel.connection(node), job, tag, pstream, err);
      });

      it("Should skip error sending if 'done' already emitted", function () {
        var node = nKernel.self();
        var job = uuid.v4();
        var err = new Error("foo");
        var tag = "tag";
        var pstream = {stream: uuid.v4(), done: true};
        kernel._sendError(kernel.connection(node), job, tag, pstream, err);
      });

      it("Should skip message", function (done) {
        var buf = Buffer.from("foo");
        kernel.once("_skip", (data) => {
          assert.equal(Buffer.compare(buf, data), 0);
          done();
        });
        kernel._skipMsg(buf);
      });

      it("Should create return stream for sync requests, infinite timeout", function () {
        var out = kernel._rstream(kernel.self(), "id");
        assert.equal(kernel.listeners("id").length, 1);
        kernel.removeAllListeners("id");
      });

      it("Should create return stream for sync requests, non-numeric timeout", function () {
        var out = kernel._rstream(kernel.self(), "id", "foo");
        assert.equal(kernel.listeners("id").length, 1);
        kernel.removeAllListeners("id");
      });

      it("Should create return stream for sync requests, set timeout and trigger", function (done) {
        var out = kernel._rstream(kernel.self(), "id", 0);
        out.once("error", (err) => {
          assert.ok(err);
          assert.lengthOf(kernel.listeners("id"), 0);
          done();
        });
        assert.equal(kernel.listeners("id").length, 1);
        kernel.removeAllListeners("id");
      });

      it("Should create return stream for sync requests, set timeout and decommission it", function (done) {
        var out = kernel._rstream(kernel.self(), "id", 0);
        assert.equal(kernel.listeners("id").length, 1);
        kernel.removeAllListeners("id");

        out.end();
        sinon.spy(out, "emit").withArgs("error");
        async.nextTick(() => {
          assert.notOk(out.emit.called);
          out.emit.restore();
          done();
        });
      });

      it("Should skip receipt of sync data message", function (done) {
        var node = kernel.self();
        var tag = uuid.v4();
        var from = {
          tag: tag,
          node: kernel.self()
        };
        var rstream = new stream.PassThrough();
        var data = Buffer.from("foo");
        var pstream = {stream: uuid.v4(), done: false};
        kernel.on("_skip", () => {
          done();
        });
        kernel._rcvData(nKernel.self(), tag, rstream, data, pstream, from);
      });

      it("Should emit error on return stream with stream error", function (done) {
        var node = kernel.self();
        var tag = uuid.v4();
        var from = {
          tag: tag,
          node: kernel.self()
        };
        var rstream = new stream.PassThrough();
        var data = Buffer.from("foo");
        var err = new Error("foo");
        var pstream = {stream: uuid.v4(), done: false, error: err};
        rstream.once("error", (e) => {
          assert.deepEqual(err, e);
          done();
        });
        kernel._rcvData(node, tag, rstream, data, pstream, from);
      });

      it("Should remove all listeners when stream finished", function (done) {
        var node = kernel.self();
        var tag = uuid.v4();
        var from = {
          tag: tag,
          node: kernel.self()
        };
        var rstream = new stream.PassThrough();
        var data = Buffer.from("foo");
        var pstream = {stream: uuid.v4(), done: true};
        rstream.once("finish", () => {
          assert.equal(kernel.listeners(tag).length, 0);
          done();
        });
        kernel._rcvData(node, tag, rstream, data, pstream, from);
      });

      it("Should handle receipt of data from a node and pipe into return stream", function (done) {
        var node = kernel.self();
        var tag = uuid.v4();
        var from = {
          tag: tag,
          node: kernel.self()
        };
        var rstream = new stream.PassThrough();
        var data = Buffer.from("foo");
        var pstream = {stream: uuid.v4(), done: false};
        rstream.once("data", (buf) => {
          assert.equal(Buffer.compare(data, buf), 0);
          done();
        });
        kernel._rcvData(node, tag, rstream, data, pstream, from);
      });
    });

    describe("NetKernel static tests", function () {
      it("Should encode a buffer", function () {
        assert.equal(NetKernel._encodeBuffer(null), null);
        var buf = Buffer.from("asdf");
        assert.deepEqual(NetKernel._encodeBuffer(buf), buf.toJSON());
      });

      it("Should decode a buffer", function () {
        assert.equal(NetKernel._decodeBuffer(null), null);
        var data = Buffer.from("asdf").toJSON();
        assert.deepEqual(NetKernel._decodeBuffer(data), Buffer.from(data.data));
      });

      it("Should fail to coerce non-buffer/string/stream into a stream", function () {
        var val = 5;
        var out;
        try {
          out = NetKernel._coerceStream(val);
        } catch (e) {
          out = e;
        }
        assert(out instanceof Error);
      });

      it("Should coerce a stream into a stream", function () {
        var rstream = new stream.PassThrough();
        assert.deepEqual(NetKernel._coerceStream(rstream), rstream);
      });

      it("Should coerce a buffer into a stream", function (done) {
        var str = "asdf";
        var data = Buffer.from(str);
        var rstream = NetKernel._coerceStream(data);
        assert(rstream instanceof stream.Stream);
        var acc = Buffer.from("");
        rstream.on("data", (data) => {
          acc = Buffer.concat([acc, data], acc.length + data.length);
        }).on("end", () => {
          assert.equal(Buffer.compare(acc, data), 0);
          done();
        });
      });

      it("Should coerce a string into a stream", function (done) {
        var data = "asdf";
        var rstream = NetKernel._coerceStream(data);
        assert(rstream instanceof stream.Stream);
        var acc = Buffer.from("");
        rstream.on("data", (data) => {
          acc = Buffer.concat([acc, data], acc.length + data.length);
        }).on("end", () => {
          assert.equal(Buffer.compare(acc, Buffer.from(data)), 0);
          done();
        });
      });

      it("Should fail to aggregate on error", function (done) {
        var rstream = new stream.PassThrough();
        var buf = Buffer.from("hello world");
        var fooErr = new Error("foo");
        async.nextTick(() => {
          rstream.write(buf.slice(0, 6));
          rstream.emit("error", fooErr);
        });
        NetKernel._collectStream(rstream, (err, data) => {
          assert.ok(err);
          assert.deepEqual(err, fooErr);
          assert.lengthOf(rstream.listeners("data"), 0);
          done();
        });
      });

      it("Should fail to aggregate on buffer limit breach", function (done) {
        var rstream = new stream.PassThrough();
        var buf = Buffer.from("hello world");
        async.nextTick(() => {
          rstream.write(buf.slice(0, 6));
        });
        NetKernel._collectStream(rstream, (err, data) => {
          assert.ok(err);
          assert.lengthOf(rstream.listeners("data"), 0);
          done();
        }, 1);
      });

      it("Should aggregate data from a stream into a buffer", function (done) {
        var rstream = new stream.PassThrough();
        var buf = Buffer.from("hello world");
        async.nextTick(() => {
          rstream.write(buf.slice(0, 6));
          rstream.write(buf.slice(6, buf.length));
          rstream.end();
        });
        NetKernel._collectStream(rstream, (err, data) => {
          assert.notOk(err);
          assert.equal(Buffer.compare(data, buf), 0);
          done();
        });
      });

      it("Should turn error into plain object", function () {
        assert.equal(NetKernel._encodeError(null), null);
        assert(_.isPlainObject(NetKernel._encodeError(new Error("foo"))));
      });

      it("Should hmac data", function () {
        var key = "foo";
        var data = "asdf";
        assert.equal(NetKernel._hmacData(key, data), crypto.createHmac("sha256", key).update(data).digest("hex"));
      });

      it("Should encode message with encryption key", function () {
        var key = null;
        var data = {foo: "bar"};
        assert.deepEqual(NetKernel._encodeMsg(key, data), data);

        key = "asdf";
        var enc = NetKernel._encodeMsg(key, data);
        assert.equal(NetKernel._hmacData(key, JSON.stringify(data)), enc.checkSum);
        assert.deepEqual(_.omit(enc, "checkSum"), data);
      });

      it("Should decode message with decryption key", function () {
        var key = null;
        var data = {foo: "bar"};
        assert.equal(NetKernel._decodeMsg(null, data), data);

        key = "asdf";
        var hmac = NetKernel._hmacData(key, JSON.stringify(data));
        data.checkSum = hmac;
        assert.deepEqual(NetKernel._decodeMsg(key, data), _.omit(data, ["checkSum"]));
      });

      it("Should fail to decode if message tampered with", function () {
        var data = {foo: "bar"};
        var key = "asdf";
        var hmac = NetKernel._hmacData(key, JSON.stringify(data));
        hmac += "a";
        data.checkSum = hmac;
        var ret = NetKernel._decodeMsg(key, data);
        assert(ret instanceof Error);
        assert(typeof ret.sent === "string");
        assert(typeof ret.calculated === "string");
      });
    });
  });
};

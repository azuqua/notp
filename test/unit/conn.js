var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Connection unit tests", function () {
    var Connection = lib.conn,
        Node = lib.node,
        server,
        ipc,
        node,
        conn;

    before(function (done) {
      ipc = new mocks.ipc();
      ipc.config.id = "id";
      ipc.serveNet("localhost", 8000, done);
      ipc.server.start();
    });

    beforeEach(function () {
      node = new Node("id", "localhost", 8000);
      conn = new Connection(ipc, node);
    });

    after(function () {
      ipc.server.stop();
    });

    it("Should construct a connection", function () {
      assert.deepEqual(conn._ipc, ipc);
      assert.ok(conn._node.equals(node));
      assert.equal(conn._queue.size(), 0);
      assert.equal(conn._connecting, false);
      assert.equal(conn._active, false);
      assert.equal(conn._streams.size, 0);
    });

    it("Should start connection", function (done) {
      conn.start();
      assert.ok(conn._connecting);
      assert.ok(conn._active);
      conn.on("connect", () => {
        assert.notOk(conn._connecting);
        assert.lengthOf(conn._ipc.of[node.id()].listeners("connect"), 1);
        assert.lengthOf(conn._ipc.of[node.id()].listeners("disconnect"), 1);
        conn._ipc.disconnect(conn._node.id());
        done();
      });
    });

    it("Should stop connection", function (done) {
      conn.start();
      conn.once("connect", () => {
        conn.stop();
        conn.once("disconnect", () => {
          assert.notOk(conn._connecting);
          assert.notOk(conn._active);
          assert.equal(conn._queue.size(), 0);
          done();
        });
      });
    });

    it("Should wait for idle state via stream check before stopping connection", function (done) {
      conn.start();
      conn.once("connect", () => {
        conn._streams.set("foo", true);
        conn.once("disconnect", () => {
          assert.notOk(conn._connecting);
          assert.notOk(conn._active);
          assert.equal(conn._queue.size(), 0);
          assert.equal(conn._streams.size, 0);
          done();
        });
        conn.stop();
        conn._streams.delete("foo");
        conn.emit("idle");
      });
    });

    it("Should wait for idle state via queue check before stopping connection", function (done) {
      conn.start();
      conn.once("connect", () => {
        conn._queue.enqueue({
          event: "foo",
          data: {
            stream: {stream: "id", done: true},
            data: "bar"
          }
        });
        conn.once("disconnect", () => {
          assert.notOk(conn._connecting);
          assert.notOk(conn._active);
          assert.equal(conn._queue.size(), 0);
          assert.equal(conn._streams.size, 0);
          done();
        });
        conn.stop();
        conn._queue.dequeue();
        conn.emit("idle");
      });
    });

    it("Should forcibly stop connection", function (done) {
      conn.start();
      conn.once("connect", () => {
        conn.stop(true);
        assert.notOk(conn._connecting);
        assert.notOk(conn._active);
        assert.equal(conn._queue.size(), 0);
        assert.equal(conn._streams.size, 0);
        done();
      });
    });

    it("Should return node at connection", function () {
      assert(conn.node().equals(node));
    });

    it("Should return queue at connection", function () {
      assert.equal(conn.queue().size(), 0);
      conn.queue().enqueue("val");
      assert.equal(conn.queue().size(), 1);
    });

    it("Should return active status of connection", function () {
      assert.equal(conn.active(), false);
    });

    it("Should return connecting status of connection", function () {
      assert.equal(conn.connecting(), false);
    });

    it("Should return idle state of connection", function () {
      assert.equal(conn.idle(), true);
      conn.queue().enqueue("val");
      assert.equal(conn.idle(), false);
    });

    it("Should return max queue length", function () {
      assert.equal(conn.maxLen(), 1024);
    });

    it("Should set new max queue length", function () {
      conn.maxLen(1023);
      assert.equal(conn.maxLen(), 1023);
      conn.maxLen(-1);
      assert.equal(conn.maxLen(), 1023);

      conn._queue.enqueue("foo");
      conn._queue.enqueue("bar");
      conn.maxLen(1);
      assert.equal(conn.maxLen(), 1);
      assert.equal(conn._queue.size(), 1);
    });

    it("Should throw if connection inactive and data is sent", function () {
      var data = {
        data: "bar",
        stream: {stream: uuid.v4(), done: false}
      };
      var out = conn.send("foo", data);
      assert(out instanceof Error);
    });

    it("Should queue data if connection reconnecting", function () {
      conn._active = true;
      conn._connecting = true;
      var data = {
        data: "bar",
        stream: {stream: uuid.v4(), done: false}
      };
      conn.send("foo", data);
      assert.equal(conn.queue().size(), 1);
      assert.deepEqual(conn.queue().dequeue(), {
        event: "foo",
        data: data
      });
    });

    it("Should queue data and drop data if queue has reached max size", function () {
      conn._active = true;
      conn._connecting = true;
      conn._maxLen = 1;
      conn._queue.enqueue("data");
      var data = {
        data: "bar",
        stream: {stream: uuid.v4(), done: false}
      };
      conn.send("foo", data);
      assert.equal(conn.queue().size(), 1);
      assert.notEqual(conn._queue.peek(), "data");
      assert.deepEqual(conn.queue().dequeue(), {
        event: "foo",
        data: data
      });
    });

    it("Should write data if connection active and not reconnecting", function (done) {
      conn.start();
      conn.once("connect", () => {
        var val = {
          data: "bar",
          stream: {stream: uuid.v4(), done: false}
        };
        conn.once("send", (event, data) => {
          assert.equal(event, "foo");
          assert.equal(data, val);
          conn.stop();
          done();
        });
        ipc.of[node.id()].on("foo", (data) => {
          assert.deepEqual(data, val);
        });
        conn.send("foo", val);
      });
    });

    it("Should handle connection logic", function (done) {
      var vals = [
        {
          event: "foo",
          data: {
            data: 0,
            stream: {stream: uuid.v4(), done: false}
          }
        },
        {
          event: "bar",
          data: {
            data: 1,
            stream: {stream: uuid.v4(), done: false}
          }
        },
        {
          event: "baz",
          data: {
            data: 2,
            stream: {stream: uuid.v4(), done: false}
          }
        }
      ];
      conn.start();
      conn.once("connect", () => {
        var called = 0;
        conn.on("send", (event, data) => {
          assert.deepEqual(vals[called], {
            event: event,
            data: data
          });
          called++;
          if (called === 3) {
            assert.equal(conn.connecting(), false);
            conn.removeAllListeners("send");
            conn.stop();
            return done();
          }
        });
        vals.forEach((val) => {
          conn._queue.enqueue(val);
        });
        conn._handleConnect();
      });
    });

    it("Should handle disconnection logic when active", function (done) {
      conn.start();
      conn.once("connect", () => {
        conn.once("disconnect", () => {
          assert.equal(conn.connecting(), true);
          conn.stop();
          done();
        });
        conn._handleDisconnect();
      });
    });

    it("Should handle disconnection logic after stop", function (done) {
      conn.start();
      conn.once("connect", () => {
        conn.once("disconnect", () => {
          assert.equal(conn.connecting(), false);
          conn.stop();
          done();
        });
        conn._active = false;
        conn._handleDisconnect();
      });
    });

    it("Should update stream map", function (done) {
      var id = uuid.v4();
      conn._updateStream({stream: id, done: false});
      assert.equal(conn._streams.size, 1);
      conn.on("idle", () => {
        assert.equal(conn._streams.size, 0);
        done();
      });
      conn._updateStream({stream: id, done: true});
    });
  });
};

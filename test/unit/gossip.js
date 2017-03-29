var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    stream = require("stream"),
    sinon = require("sinon"),
    fs = require("fs"),
    microtime = require("microtime"),
    EventEmitter = require("events").EventEmitter,
    assert = require("chai").assert;

class MockTable extends EventEmitter {
  constructor(id) {
    super();
    this._id = id;
  }

  id() {
    return "foo";
  }

  stop() {
    return this;
  }
}

module.exports = function (mocks, lib) {
  describe("Gossip unit tests", function () {
    var VectorClock = lib.vclock,
        CHash = lib.chash,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("Gossip state tests", function () {
      var kernel,
          gossip,
          vclock,
          chash,
          opts,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function () {
        kernel = new NetKernel(new MockIPC(), id, host, port);
        chash = new CHash(3, 3);
        chash.insert(new Node(id, host, port));
        vclock = new VectorClock();
      });

      beforeEach(function () {
        gossip = new GossipRing(kernel, chash, vclock, {
          interval: 100,
          flushInterval: 100,
          flushPath: "/foo/bar",
          wquorum: 1,
          vclockOpts: {}
        });
      });

      it("Should construct a gossip ring", function () {
        assert.deepEqual(gossip._kernel, kernel);
        assert.deepEqual(gossip._ring, chash);
        assert.deepEqual(gossip._vclock, vclock);
        assert.isNumber(gossip._poll);
        assert.isObject(gossip._vclockOpts);
        assert(gossip._streams instanceof Map);
        assert.equal(gossip._streams.size, 0);
        assert.equal(gossip._actor, null);
        assert.equal(gossip._ringID, null);
      });

      it("Should grab ring of gossip ring", function () {
        assert.deepEqual(gossip.ring(), chash);
      });

      it("Should set ring of gossip ring", function () {
        var chash2 = new CHash();
        chash2.insert(new Node("id2", "localhost", 8001));
        gossip.ring(chash2);
        assert.deepEqual(gossip._ring, chash2);
      });

      it("Should grab vclock of gossip ring", function () {
        assert.deepEqual(gossip.vclock(), vclock);
      });

      it("Should set vclock of gossip ring", function () {
        var clock = new VectorClock(new Node("id2", "localhost", 8001), 1);
        gossip.vclock(clock);
        assert.deepEqual(gossip._vclock, clock);
      });

      it("Should grab kernel of gossip ring", function () {
        assert.deepEqual(gossip.kernel(), kernel);
      });

      it("Should set kernel of gossip ring", function () {
        var kernel2 = new NetKernel(new MockIPC(), "id2", host, port+1);
        gossip.kernel(kernel2);
        assert.deepEqual(gossip._kernel, kernel2);
      });

      it("Should grab active streams of gossip ring", function () {
        assert(gossip.streams() instanceof Map);
        assert.equal(gossip.streams().size, 0);
      });

      it("Should set active streams of gossip ring", function () {
        var streams = new Map([["key", "value"]]);
        gossip.streams(streams);
        assert(gossip.streams() instanceof Map);
        assert.equal(gossip.streams().size, 1);
      });

      it("Should return if gossip process is idle or not", function () {
        assert.ok(gossip.idle());
        var streams = new Map([["key", "value"]]);
        gossip.streams(streams);
        assert.notOk(gossip.idle());
      });

      it("Should grab gossip actor", function () {
        gossip._actor = "foo";
        assert.equal(gossip.actor(), gossip._actor);
      });

      it("Should set gossip actor", function () {
        gossip.actor("foo");
        assert.equal(gossip._actor, "foo");
      });

      it("Should start gossip process", function () {
        var ringID = "foo";
        gossip.vclock().increment(uuid.v4());
        gossip.start(ringID);
        assert.equal(gossip._ringID, ringID);
        assert.ok(gossip._interval);
        assert.ok(gossip._flush);
        assert.lengthOf(gossip.kernel().listeners(ringID), 1);
        assert.lengthOf(gossip.listeners("pause"), 1);
        var events = ["ring"];
        events.forEach((val) => {
          assert.lengthOf(gossip.listeners(val), 1);
        });
        assert.lengthOf(gossip.listeners("pause"), events.length);
        gossip.stop(true);
      });

      it("Should stop gossip process", function (done) {
        var ringID = "foo";
        gossip.vclock().increment(uuid.v4());
        gossip.start(ringID);
        gossip.once("stop", () => {
          assert.equal(gossip._interval, null);
          assert.equal(gossip._flush, null);
          assert.equal(gossip._ringID, null);
          assert.equal(gossip._streams.size, 0);
          var events = ["ring", "state", "process"];
          events.forEach((val) => {
            assert.lengthOf(gossip.listeners(val), 0);
          });
          assert.lengthOf(gossip.listeners("pause"), 0);
          done();
        });
        gossip.stop();
      });

      it("Should forcefully stop gossip process", function (done) {
        var ringID = "foo";
        gossip.vclock().increment(uuid.v4());
        gossip.start(ringID);
        gossip.once("stop", () => {
          assert.equal(gossip._interval, null);
          assert.equal(gossip._flush, null);
          assert.equal(gossip._ringID, null);
          assert.equal(gossip._streams.size, 0);
          done();
        });
        gossip.stop(true);
      });

      it("Should pause gossip process", function () {
        var ringID = "foo";
        gossip.start(ringID);
        gossip.pause();
        assert.lengthOf(gossip.kernel().listeners(ringID), 0);
        assert.lengthOf(gossip.listeners("pause"), 0);
        assert.equal(gossip._interval, null);
        assert.equal(gossip._flush, null);
      });

      it("Should resume gossip process", function () {
        var ringID = "foo";
        gossip.start(ringID);
        gossip.pause();
        gossip.resume();
        assert.lengthOf(gossip.kernel().listeners(ringID), 1);
        assert.lengthOf(gossip.listeners("pause"), 1);
        assert.ok(gossip._interval);
        assert.ok(gossip._flush);
      });

      it("Should immediately emit 'close'", function () {
        gossip.once("close", () => {
          assert.equal(gossip._ringID, null);
        });
        gossip.leave();
      });

      it("Should skip loading state from disk if flush path isn't a string", function (done) {
        gossip._flushPath = null;
        sinon.spy(fs, "readFile");
        gossip.load((err) => {
          assert.notOk(err);
          assert.notOk(fs.readFile.called);
          fs.readFile.restore();
          done();
        });
      });

      it("Should skip loading state from disk if call results in ENOENT error", function (done) {
        gossip._flushPath = "/foo/bar";
        sinon.stub(fs, "readFile", (path, cb) => {
          async.nextTick(() => {
            return cb(_.extend(new Error("error"), {
              code: "ENOENT"
            }));
          });
        });
        gossip.load((err) => {
          assert.notOk(err);
          assert.deepEqual(gossip.ring(), chash);
          assert.deepEqual(gossip.vclock(), vclock);
          assert.notOk(gossip._actor);
          assert.notOk(gossip._ringID);
          fs.readFile.restore();
          done();
        });
      });

      it("Should fail to load state from disk if call results in error", function (done) {
        sinon.stub(fs, "readFile", (path, cb) => {
          async.nextTick(() => {
            cb(new Error("error"));
          });
        });
        gossip.load((err) => {
          assert.ok(err);
          fs.readFile.restore();
          done();
        });
      });

      it("Should fail to load state from disk if data is not valid JSON", function (done) {
        sinon.stub(fs, "readFile", (path, cb) => {
          async.nextTick(() => {
            cb(null, "foo");
          });
        });
        gossip.load((err) => {
          assert.ok(err);
          fs.readFile.restore();
          done();
        });
      });

      it("Should fail to load state from disk if data is not a JSON object", function (done) {
        sinon.stub(fs, "readFile", (path, cb) => {
          async.nextTick(() => {
            cb(null, "\"foo\"");
          });
        });
        gossip.load((err) => {
          assert.ok(err);
          fs.readFile.restore();
          done();
        });
      });

      it("Should load state from disk", function (done) {
        var chash2 = (new CHash(chash.rfactor(), chash.pfactor())).insert(new Node("id2", host, port+1));
        var vclock2 = new VectorClock("baz", 1);
        sinon.stub(fs, "readFile", (path, cb) => {
          async.nextTick(() => {
            cb(null, JSON.stringify({
              ring: "foo",
              chash: chash2.toJSON(true),
              vclock: vclock2.toJSON(true),
              actor: "bar"
            }));
          });
        });
        gossip.load((err) => {
          assert.notOk(err);
          assert.deepEqual(gossip.ring(), chash2);
          assert.deepEqual(gossip.vclock(), vclock2);
          assert.equal(gossip.actor(), "bar");
          assert.equal(gossip._ringID, "foo");
          fs.readFile.restore();
          done();
        });
      });
    });

    describe("Gossip ring manipulation tests", function () {
      var kernel,
          nKernel,
          gossip,
          vclock,
          chash,
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
        clearInterval(gossip._interval);
        clearInterval(gossip._flush);
      });

      afterEach(function (done) {
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

      it("Should ignore join request if ring ID already exists", function () {
        var res = gossip.join("foo");
        assert.ok(res instanceof Error);
      });

      it("Should handle joining new ring", function () {
        var fooBefore = kernel.listeners("foo").length;
        var leaveBefore = gossip.listeners("leave").length;
        gossip.leave("foo");
        assert.lengthOf(kernel.listeners("foo"), fooBefore-1);
        gossip.join("foo");
        assert.equal(gossip._ringID, "foo");
        assert.lengthOf(kernel.listeners("foo"), fooBefore);
        assert.lengthOf(gossip.listeners("leave"), leaveBefore);
      });

      it("Should ignore meet request if node already exists in this cluster", function () {
        gossip.meet(kernel.self());
        assert.equal(gossip.ring().size(), 3);
        assert.equal(gossip.vclock().size(), 1);
      });

      it("Should join a cluster by meeting a node", function (done) {
        var node2 = new Node("id2", host, port+1);
        var called = 0;
        var beforeActor = gossip.actor();
        var beforeSize = gossip.vclock().size();
        gossip.once("send", (clock, event, ring) => {
          assert.equal(beforeActor, gossip._actor);
          assert.equal(event, "ring");
          assert.equal(gossip.vclock().size(), beforeSize);
          assert(gossip.kernel().isConnected(node2));
          assert.deepEqual(clock.toJSON(true), gossip.vclock().toJSON(true));
          assert.deepEqual(ring, gossip.ring().toJSON(true));
          called++;
          if (called === 2) done();
        });
        gossip.meet(node2);
        gossip.kernel().connection(node2).once("send", (msg, data) => {
          var inner = JSON.parse(Buffer.from(data.data.data));
          var job = inner.data;
          assert.equal(job.type, "join");
          assert.notEqual(job.actor, gossip._actor);
          assert.deepEqual(job.data, gossip._ring.toJSON(true));
          assert.deepEqual(job.vclock, gossip._vclock.toJSON(true));
          assert.equal(job.round, 0);
          called++;
          if (called === 2) done();
        });
      });

      it("Should ignore an insert request into the gossip ring", function () {
        var node2 = new Node("id2", host, port+1);
        gossip.ring().insert(node2);
        var before = gossip.ring().size();
        gossip.insert(node2);
        assert.deepEqual(gossip.ring().size(), before);
        gossip.ring().remove(node2);
      });

      it("Should insert a node into gossip ring when already idle", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 6);
          assert.ok(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          assert.deepEqual(msg, gossip.ring().toJSON(true));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.insert(node2);
        assert.equal(gossip.ring().size(), 6);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.ok(gossip.kernel().isConnected(node2));
        gossip.kernel().connection(node2).once("send", (msg, data) => {
          var inner = JSON.parse(Buffer.from(data.data.data));
          var job = inner.data;
          assert.equal(job.type, "update");
          assert.equal(job.actor, gossip._actor);
          assert.deepEqual(job.data, gossip.ring().toJSON(true));
          assert.deepEqual(job.vclock, gossip.vclock().toJSON(true));
          assert.equal(job.round, GossipRing.maxMsgRound(gossip.ring())-1);
          done();
        });
      });

      it("Should insert a node into gossip ring, waiting for idle state", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.streams(new Map([["key", "value"]]));
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.ok(gossip.ring().isDefined(node2));
          assert.equal(gossip.ring().size(), 6);
          assert.ok(gossip._actor);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.ok(gossip.kernel().isConnected(node2));
          gossip.kernel().connection(node2).once("send", (msg, data) => {
            var inner = JSON.parse(Buffer.from(data.data.data));
            var job = inner.data;
            assert.equal(job.type, "update");
            assert.equal(job.actor, gossip._actor);
            assert.deepEqual(job.data, gossip.ring().toJSON(true));
            assert.deepEqual(job.vclock, gossip.vclock().toJSON(true));
            assert.equal(job.round, GossipRing.maxMsgRound(gossip.ring())-1);
            done();
          });
        });
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          assert.deepEqual(msg, gossip.ring().toJSON(true));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.streams(new Map());
        // trigger test
        gossip.emit("idle");
      });
      
      it("Should forcefully insert a node into gossip ring", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 6);
          assert.ok(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          assert.deepEqual(msg, gossip.ring().toJSON(true));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.insert(node2, true);
        assert.equal(gossip.ring().size(), 6);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.ok(gossip.kernel().isConnected(node2));
        gossip.kernel().connection(node2).once("send", (msg, data) => {
          var inner = JSON.parse(Buffer.from(data.data.data));
          var job = inner.data;
          assert.equal(job.type, "update");
          assert.equal(job.actor, gossip._actor);
          assert.deepEqual(job.data, gossip.ring().toJSON(true));
          assert.deepEqual(job.vclock, gossip.vclock().toJSON(true));
          assert.equal(job.round, GossipRing.maxMsgRound(gossip.ring())-1);
          done();
        });
      });

      it("Should ignore an minsert request into the gossip ring", function () {
        var node2 = new Node("id2", host, port+1);
        gossip.ring().insert(node2);
        var before = gossip.ring().size();
        gossip.minsert([node2]);
        assert.equal(gossip.ring().size(), before);
        gossip.ring().remove(node2);
      });

      it("Should minsert nodes into gossip ring when already idle", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 6);
          assert.ok(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          assert.deepEqual(msg, gossip.ring().toJSON(true));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.minsert([node2]);
        assert.equal(gossip.ring().size(), 6);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.ok(gossip.kernel().isConnected(node2));
        gossip.kernel().connection(node2).once("send", (msg, data) => {
          var inner = JSON.parse(Buffer.from(data.data.data));
          var job = inner.data;
          assert.equal(job.type, "update");
          assert.equal(job.actor, gossip._actor);
          assert.deepEqual(job.data, gossip.ring().toJSON(true));
          assert.deepEqual(job.vclock, gossip.vclock().toJSON(true));
          assert.equal(job.round, GossipRing.maxMsgRound(gossip.ring())-1);
          done();
        });
      });

      it("Should minsert nodes into gossip ring, waiting for idle state", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.streams(new Map([["key", "value"]]));
        gossip.minsert([node2]);
        gossip.once("process", (ring) => {
          assert.ok(gossip.ring().isDefined(node2));
          assert.equal(gossip.ring().size(), 6);
          assert.ok(gossip._actor);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.ok(gossip.kernel().isConnected(node2));
          gossip.kernel().connection(node2).once("send", (msg, data) => {
            var inner = JSON.parse(Buffer.from(data.data.data)).data;
            assert.equal(inner.type, "update");
            assert.equal(inner.actor, gossip._actor);
            assert.deepEqual(inner.data, gossip.ring().toJSON(true));
            assert.deepEqual(inner.vclock, gossip.vclock().toJSON(true));
            assert.equal(inner.round, GossipRing.maxMsgRound(gossip.ring())-1);
            done();
          });
        });
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          assert.deepEqual(msg, gossip.ring().toJSON(true));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.streams(new Map());
        // trigger test
        gossip.emit("idle");
      });

      it("Should forcefully minsert nodes into gossip ring", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 6);
          assert.ok(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          assert.deepEqual(msg, gossip.ring().toJSON(true));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.minsert([node2], true);
        assert.equal(gossip.ring().size(), 6);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.ok(gossip.kernel().isConnected(node2));
        gossip.kernel().connection(node2).once("send", (msg, data) => {
          var inner = JSON.parse(Buffer.from(data.data.data)).data;
          assert.equal(inner.type, "update");
          assert.equal(inner.actor, gossip._actor);
          assert.deepEqual(inner.data, gossip.ring().toJSON(true));
          assert.deepEqual(inner.vclock, gossip.vclock().toJSON(true));
          assert.equal(inner.round, GossipRing.maxMsgRound(gossip.ring())-1);
          done();
        });
      });

      it("Should leave trivial ring", function (done) {
        gossip.once("close", () => {
          assert.equal(gossip._ringID, null);
        });
        gossip.leave();
        done();
      });

      it("Should leave idle ring", function (done) {
        var node2 = new Node("id2", host, port+1);
        var ringID = gossip._ringID;
        var oldRing = gossip.ring();
        var oldClock = gossip.vclock();
        gossip.insert(node2);
        gossip.once("send", (clock, event, msg) => {
          Object.keys(clock._vector).forEach((key) => {
            assert(oldClock.has(key));
          });
          var ring = (new CHash()).fromJSON(msg);
          assert.notOk(ring.isDefined(gossip.kernel().self()));
        });
        gossip.once("close", () => {
          assert(gossip.streams() instanceof Map);
          assert.equal(gossip.streams().size, 0);
          assert.equal(gossip.ring().size(), gossip.ring().rfactor());
          assert(gossip.ring().isDefined(gossip.kernel().self()));
          assert.equal(gossip.vclock().size(), 1);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.equal(gossip._ringID, null);
          assert.lengthOf(gossip.kernel().listeners(ringID), 0);
          done();
        });
        gossip.leave();
      });

      it("Should leave ring gracefully", function (done) {
        var node2 = new Node("id2", host, port+1);
        var ringID = gossip._ringID;
        var oldRing = gossip.ring();
        var oldClock = gossip.vclock();
        gossip.insert(node2);
        gossip.once("send", (clock, event, msg) => {
          Object.keys(clock._vector).forEach((key) => {
            assert(oldClock.has(key));
          });
          var ring = (new CHash()).fromJSON(msg);
          assert.notOk(ring.isDefined(gossip.kernel().self()));
        });
        gossip.once("close", () => {
          assert(gossip.streams() instanceof Map);
          assert.equal(gossip.streams().size, 0);
          assert.equal(gossip.ring().size(), gossip.ring().rfactor());
          assert(gossip.ring().isDefined(gossip.kernel().self()));
          assert.equal(gossip.vclock().size(), 1);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.equal(gossip._ringID, null);
          assert.lengthOf(gossip.kernel().listeners(ringID), 0);
          done();
        });
        gossip.streams().set("key", "value");
        gossip.leave();
        gossip.streams().delete("key");
        gossip.emit("idle");
      });

      it("Should leave ring forcefully", function (done) {
        var node2 = new Node("id2", host, port+1);
        var ringID = gossip._ringID;
        var oldRing = gossip.ring();
        var oldClock = gossip.vclock();
        gossip.insert(node2);
        gossip.once("send", (clock, event, msg) => {
          Object.keys(clock._vector).forEach((key) => {
            assert(oldClock.has(key));
          });
          var ring = (new CHash()).fromJSON(msg);
          assert.notOk(ring.isDefined(gossip.kernel().self()));
        });
        gossip.once("close", () => {
          assert(gossip.streams() instanceof Map);
          assert.equal(gossip.streams().size, 0);
          assert.equal(gossip.ring().size(), gossip.ring().rfactor());
          assert(gossip.ring().isDefined(gossip.kernel().self()));
          assert.equal(gossip.vclock().size(), 1);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.equal(gossip._ringID, null);
          assert.lengthOf(gossip.kernel().listeners(ringID), 0);
          done();
        });
        gossip.leave(true);
      });

      it("Should ignore a remove request to the gossip ring", function (done) {
        var node2 = new Node("id2", host, port+1);
        var before = gossip.ring().size();
        gossip.remove(node2);
        assert.equal(gossip.ring().size(), before);
        done();
      });

      it("Should remove a node from the gossip ring if already idle", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 3);
          assert.notOk(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (ring, clock) => {
          assert.deepEqual(ring, gossip.ring());
          assert.deepEqual(clock, gossip.vclock());
        });
        var conn = gossip.kernel().connection(node2);
        conn.once("idle", done);
        gossip.remove(node2);
        assert.equal(gossip.ring().size(), 3);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.notOk(gossip.kernel().isConnected(node2));
      });

      it("should remove a node from the gossip ring, waiting for 'idle' state", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 3);
          assert.notOk(gossip.ring().isDefined(node2));
          assert.ok(gossip._actor);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.notOk(gossip.kernel().isConnected(node2));
          done();
        });
        gossip.once("send", (ring, clock) => {
          assert.deepEqual(ring, gossip.ring());
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.streams().set("key", "val");
        gossip.remove(node2);
        gossip.streams().delete("key");
        gossip.emit("idle");
      });

      it("Should remove a node from the gossip ring if forced", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 3);
          assert.notOk(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (ring, clock) => {
          assert.deepEqual(ring, gossip.ring());
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.remove(node2, true);
        assert.equal(gossip.ring().size(), 3);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.notOk(gossip.kernel().isConnected(node2));
        done();
      });

      it("Should ignore an mremove request into the gossip ring", function () {
        var node2 = new Node("id2", host, port+1);
        var before = gossip.ring().size();
        gossip.mremove([node2]);
        assert.equal(gossip.ring().size(), before);
      });

      it("Should mremove nodes from gossip ring when already idle", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 3);
          assert.notOk(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (ring, clock) => {
          assert.deepEqual(ring, gossip.ring());
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.mremove([node2]);
        assert.equal(gossip.ring().size(), 3);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.notOk(gossip.kernel().isConnected(node2));
        done();
      });

      it("Should mremove nodes from gossip ring, waiting for idle state", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 3);
          assert.notOk(gossip.ring().isDefined(node2));
          assert.ok(gossip._actor);
          assert.ok(gossip.vclock().has(gossip._actor));
          assert.notOk(gossip.kernel().isConnected(node2));
          done();
        });
        gossip.once("send", (ring, clock) => {
          assert.deepEqual(ring, gossip.ring());
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.streams().set("key", "val");
        gossip.mremove([node2]);
        gossip.streams().delete("key");
        gossip.emit("idle");
      });

      it("Should forcefully mremove nodes from gossip ring", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2);
        gossip.once("process", (ring) => {
          assert.equal(gossip.ring().size(), 3);
          assert.notOk(gossip.ring().isDefined(node2));
        });
        gossip.once("send", (ring, clock) => {
          assert.deepEqual(ring, gossip.ring());
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.mremove([node2], true);
        assert.equal(gossip.ring().size(), 3);
        assert.ok(gossip._actor);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.notOk(gossip.kernel().isConnected(node2));
        done();
      });

      it("Should find data in ring", function () {
        var nodes = gossip.find(Buffer.from("foo"));
        assert.lengthOf(nodes, 1);
        assert.deepEqual(nodes[0], gossip.ring().nodes()[0]);
      });

      it("Should skip polling ring if no actor exists", function () {
        sinon.spy(gossip, "sendRing");
        gossip.poll();
        assert.notOk(gossip.sendRing.called);
        gossip.sendRing.restore();
      });

      it("Should poll ring and broadcast based on interval", function () {
        gossip.actor("foo");
        gossip.ring().insert(new Node("id2", host, port+1));
        sinon.spy(gossip.vclock(), "trim");
        sinon.spy(gossip, "once").withArgs("send");
        gossip.once("send", (clock, event, msg) => {
          assert.equal(event, "ring");
          var ring = (new CHash()).fromJSON(msg);
          assert(ring.equals(gossip.ring()));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.poll();
        assert.ok(gossip.vclock().trim.called);
        gossip.vclock().trim.restore();
        assert.ok(gossip.once.called);
        gossip.once.restore();
      });

      it("Should skip flushing state to disk if no actor", function () {
        sinon.stub(fs, "writeFile");
        gossip.flush();
        assert.notOk(fs.writeFile.called);
        fs.writeFile.restore();
      });

      it("Should flush state to disk", function () {
        sinon.stub(fs, "writeFile", (path, data) => {
          data = JSON.parse(data);
          assert.equal(data.actor, "foo");
          assert.deepEqual(data.chash, gossip.ring().toJSON(true));
          assert.deepEqual(data.vclock, gossip.vclock().toJSON(true));
        });
        gossip._actor = "foo";
        gossip.flush();
        assert.ok(fs.writeFile.called);
        fs.writeFile.restore();
      });

      it("Should skip sending ring externally if n=0", function () {
        gossip.ring().insert(new Node("id2", host, port+1));
        sinon.spy(gossip, "emit");
        gossip.sendRing(0);
        assert.notOk(gossip.emit.called);
        gossip.emit.restore();
      });

      it("Should skip sending ring externally if ring is trivial/singular", function () {
        sinon.spy(gossip, "emit");
        gossip.sendRing(0);
        assert.notOk(gossip.emit.called);
        gossip.emit.restore();
      });

      it("Should send the ring externally", function (done) {
        var node2 = new Node("id2", host, port+1);
        gossip.insert(node2, true);
        gossip.once("send", (clock, event, msg) => {
          var ring = (new CHash()).fromJSON(msg);
          assert(ring.equals(gossip.ring()));
          assert.deepEqual(clock, gossip.vclock());
        });
        gossip.sendRing(1);
        gossip.kernel().connection(node2).once("send", function (msg, data) {
          var parsed = JSON.parse(Buffer.from(data.data.data)).data;
          assert.equal(parsed.type, "update");
          assert.equal(parsed.actor, gossip._actor);
          assert.deepEqual(parsed.data, gossip.ring().toJSON(true));
          assert.deepEqual(parsed.vclock, gossip.vclock().toJSON(true));
          assert.equal(parsed.round, 0);
          done();
        });
      });

      it("Should select default number of random nodes", function () {
        var node2 = new Node("id2", host, port+1);
        gossip.ring().insert(node2);
        var out = gossip.selectRandom();
        assert.lengthOf(out, 1);
        assert.notEqual(out[0].id(), gossip.kernel().self().id());
      });

      it("Should select 'n' number of random nodes", function () {
        var node2 = new Node("id2", host, port+1);
        var node3 = new Node("id3", host, port+2);
        gossip.ring().insert(node2).insert(node3);
        var out = gossip.selectRandom(2);
        assert.lengthOf(out, 2);
        out.forEach((node) => {
          assert.notEqual(node.id(), gossip.kernel().self().id());
        });
      });

      it("Should select default number of random nodes from a list", function () {
        var node2 = new Node("id2", host, port+1);
        gossip.ring().insert(node2);
        var out = gossip.selectRandomFrom(gossip.ring().nodes());
        assert.lengthOf(out, 1);
      });

      it("Should select 'n' number of random nodes", function () {
        var node2 = new Node("id2", host, port+1);
        var node3 = new Node("id3", host, port+2);
        gossip.ring().insert(node2).insert(node3);
        var out = gossip.selectRandomFrom(gossip.ring().nodes(), 2);
        assert.lengthOf(out, 2);
      });

      it("Should compare and return ring based on LWW strategy", function () {
        // trivial clock
        var clock2 = new VectorClock();
        var ring2 = new CHash(3, 3);
        assert.deepEqual(GossipRing.LWW(gossip.ring(), gossip.vclock(), ring2, clock2), gossip.ring());

        // last written clock
        var key = _.first(_.keys(gossip.vclock()._vector));
        var obj = _.first(_.values(gossip.vclock()._vector));
        clock2._size++;
        clock2._vector[key] = {
          insert: obj.insert+1,
          time: obj.insert+1,
          count: 1
        };
        assert.deepEqual(GossipRing.LWW(gossip.ring(), gossip.vclock(), ring2, clock2), ring2);
        
        // older clock
        clock2._vector[key] = {
          insert: obj.insert-1,
          time: obj.insert-1,
          count: 1
        };
        assert.deepEqual(GossipRing.LWW(gossip.ring(), gossip.vclock(), ring2, clock2), gossip.ring());
      });

      it("Should parse incoming job streams", function () {
        var data = Buffer.from(JSON.stringify(chash.toJSON(true)));
        var stream = {stream: uuid.v4(), done: false};
        assert.notOk(gossip.streams().has(stream.stream));
        gossip._parse(data, stream, {});
        assert.ok(gossip.streams().has(stream.stream));
        assert.equal(Buffer.compare(data, gossip.streams().get(stream.stream).data), 0);
      });

      it("Should parse incoming job streams, with existing stream data", function () {
        var data = Buffer.from(JSON.stringify(chash.toJSON(true)));
        var stream = {stream: uuid.v4(), done: false};
        var init = Buffer.from("foo");
        gossip.streams().set(stream.stream, {data: init});
        gossip._parse(data, stream, {});
        assert.ok(gossip.streams().has(stream.stream));
        var exp = Buffer.concat([init, data], init.length + data.length);
        assert.equal(Buffer.compare(exp, gossip.streams().get(stream.stream).data), 0);
      });

      it("Should skip parsing full job if stream errors", function () {
        sinon.spy(gossip, "decodeJob");
        var data = Buffer.from(JSON.stringify(chash.toJSON(true)));
        var stream = {stream: uuid.v4(), error: {foo: "bar"}, done: true};
        var init = Buffer.from("foo");
        gossip.streams().set(stream.stream, {data: init});
        gossip._parse(data, stream, {});
        assert.notOk(gossip.streams().has(stream.stream));
        assert.notOk(gossip.decodeJob.called);
        gossip.decodeJob.restore();
      });

      it("Should parse a full job", function () {
        sinon.stub(gossip, "emit", (event, val) => {
          if (event === "idle") return;
          assert.equal(val.type, "update");
          assert.equal(val.actor, "foo");
          assert.equal(val.round, 0);
          assert.deepEqual(val.vclock, vclock);
          assert(val.data.equals(chash));
        });
        var data = Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        }));
        var stream = {stream: uuid.v4(), done: false};
        gossip._parse(data, stream, {});
        gossip._parse(data, {stream: stream.stream, done: true}, {});
        assert.notOk(gossip._streams.has(stream.stream));
        assert.ok(gossip.emit.called);
        gossip.emit.restore();
      });

      it("Should parse full job, not emitting idle event", function () {
        gossip._streams.set("foo", "bar");
        sinon.stub(gossip, "emit");
        var data = Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        }));
        var stream = {stream: uuid.v4(), done: false};
        gossip._parse(data, stream, {});
        gossip._parse(data, {stream: stream.stream, done: true}, {});
        assert.notOk(gossip._streams.has(stream.stream));
        assert.notOk(gossip.emit.calledWith(["idle"]));
        gossip.emit.restore();
      });

      it("Should decode incoming job", function () {
        var buf = Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "foo",
            actor: "bar",
            data: (new CHash(3, 3)).toJSON(true),
            vclock: (new VectorClock()).toJSON(true),
            round: 0
          }
        }));
        var out = gossip.decodeJob(buf);
        assert.equal(out.event, "ring");
        out = out.data;
        assert.equal(out.type, "foo");
        assert.equal(out.actor, "bar");
        assert.ok(out.data);
        assert.equal(out.vclock.size(), 0);
        assert.equal(out.round, 0);
      });

      it("Should update ring with new job", function () {
        var oldSend = gossip.sendRing;
        var fns = ["_mergeRings", "_updateRound", "_makeConnects", "_makeDisconnects"];
        fns.forEach((val) => {
          sinon.spy(gossip, val);
        }, {});
        sinon.stub(gossip, "sendRing").returnsArg(0);
        // to skip idle emit
        gossip.streams().set("foo", "bar");
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        sinon.stub(gossip, "emit").withArgs("process");
        var out = gossip._updateRing(data.data);
        assert.notOk(gossip.emit.called);
        gossip.emit.restore();
        assert.equal(out, 0);
        gossip.sendRing.restore();
        fns.forEach((val) => {
          gossip[val].restore();
        });
      });

      it("Should update ring with new job, emit process", function () {
        var oldSend = gossip.sendRing;
        var fns = ["_mergeRings", "_updateRound", "_makeConnects", "_makeDisconnects"];
        fns.forEach((val) => {
          sinon.spy(gossip, val);
        }, {});
        sinon.stub(gossip, "sendRing").returnsArg(0);
        // to skip idle emit
        gossip.streams().set("foo", "bar");
        var v2 = new VectorClock(uuid.v4(), 1);
        var chash2 = new CHash(3, 3).insert(new Node("id2", host, port+1));
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: v2.toJSON(true),
            data: chash2.toJSON(true),
            round: 0
          }
        })));
        sinon.spy(gossip, "emit").withArgs("process");
        var priorRing = (new CHash(chash.rfactor(), chash.pfactor(), chash.tree()));
        gossip.once("process", (oldRing, ring) => {
          assert.ok(ring.equals(gossip.ring()));
          assert.ok(oldRing.equals(priorRing));
        });
        var out = gossip._updateRing(data.data);
        assert.ok(gossip.emit.called);
        assert.equal(out, 0);
        gossip.emit.restore();
        gossip.sendRing.restore();
        fns.forEach((val) => {
          gossip[val].restore();
        });
      });

      it("Should merge two rings", function () {
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        var oldCount = gossip.vclock().getCount(data.actor);
        assert.equal(oldCount, undefined);
        data = data.data;
        var nodes = gossip._mergeRings(data);
        assert.isArray(nodes);
        assert.isArray(nodes[0]);
        assert.isArray(nodes[1]);
        assert.equal(gossip._actor, data.actor);
        assert.equal(gossip.vclock().getCount(data.actor), 1);
      });

      it("Should merge two rings on a join event", function () {
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "join",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        sinon.spy(gossip, "_joinNewRing");
        gossip._mergeRings(data.data);
        assert(gossip._joinNewRing.called);
        gossip._joinNewRing.restore();
      });

      it("Should merge two rings with imposed ring", function () {
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        sinon.spy(gossip, "_imposeRing");
        gossip._mergeRings(data.data);
        assert(gossip._imposeRing.called);
        gossip._imposeRing.restore();
      });

      it("should merge two rings with merge conflict", function () {
        gossip.vclock(new VectorClock("foo", 1));
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: (new VectorClock("bar", 1)).toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        sinon.spy(gossip, "_handleRingConflict");
        gossip._mergeRings(data.data);
        assert.ok(gossip._handleRingConflict.called);
        gossip._handleRingConflict.restore();
      });

      it("Should join new ring to this ring", function () {
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "join",
            actor: "foo",
            vclock: vclock.toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        data = data.data;
        var nodes = gossip._joinNewRing(data);
        assert.deepEqual(nodes[0], data.data.nodes());
        assert.lengthOf(nodes[1], 0);
        assert(gossip.vclock().descends(data.vclock));
        data.data.nodes().forEach((node) => {
          assert(gossip.ring().isDefined(node));
        });
      });

      it("Should impose external ring onto this ring", function () {
        var chash2 = new CHash(chash.rfactor(), chash.pfactor());
        var node2 = new Node("id2", host, port+1);
        chash2.insert(node2);
        var vclock2 = new VectorClock(uuid.v4(), 1);
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: vclock2.toJSON(true),
            data: chash2.toJSON(true),
            round: 0
          }
        })));
        data = data.data;
        var nodes = gossip._imposeRing(data);
        assert.lengthOf(nodes[0], 1);
        assert.lengthOf(nodes[1], 1);
        assert(gossip.ring().equals(data.data));
        assert.deepEqual(gossip.vclock(), data.vclock);
      });

      it("Should handle ring conflict logic", function () {
        gossip.vclock(new VectorClock("foo", 1));
        var data = gossip.decodeJob(Buffer.from(JSON.stringify({
          event: "ring",
          data: {
            type: "update",
            actor: "foo",
            vclock: (new VectorClock("bar", 1)).toJSON(true),
            data: chash.toJSON(true),
            round: 0
          }
        })));
        data = data.data;
        gossip.once("conflict", (ring, clock) => {
          assert(ring.equals(data.data));
          assert(clock.equals(data.vclock));
        });
        sinon.spy(GossipRing, "LWW");
        var out = gossip._handleRingConflict(data);
        assert.isArray(out);
        assert.isArray(out[0]);
        assert.isArray(out[1]);
        assert(gossip.vclock().descends(data.vclock));
        assert(GossipRing.LWW.called);
        GossipRing.LWW.restore();
      });

      it("Should update gossip round count", function () {
        var data = {type: "join", round: 0};
        assert.equal(gossip._updateRound(data), GossipRing.maxMsgRound(gossip.ring()));
        data = {type: "update", round: 1};
        assert.equal(gossip._updateRound(data), 1);
      });

      it("Should make IPC client connects", function () {
        var node2 = new Node("id2", host, port+1);
        var nodes = [node2];
        gossip._makeConnects(nodes);
        nodes.forEach((node) => {
          assert(gossip.kernel().isConnected(node));
        });
      });

      it("Should make IPC client disconnects", function () {
        var node2 = new Node("id2", host, port+1);
        var nodes = [node2];
        gossip.kernel().connect(node2);
        gossip._makeDisconnects(nodes);
        nodes.forEach((node) => {
          assert.notOk(gossip.kernel().isConnected(node));
        });
      });

      it("Should close ring", function () {
        gossip.ring().insert(new Node("id2", host, port+1));
        var oldID = gossip._ringID;
        var oldClock = gossip.vclock();
        var fns = ["_leaveClock", "_leaveRing"];
        fns.forEach((fn) => {sinon.spy(gossip, fn);});
        sinon.spy(gossip, "emit").withArgs("leave");
        gossip.once("send", (clock, event, msg) => {
          var ring = (new CHash()).fromJSON(msg);
          assert.notOk(ring.isDefined(kernel.self()));
          assert(clock.descends(oldClock));
        });
        sinon.stub(kernel, "abcast", (nodes, id, state) => {
          assert.equal(_.find(nodes, _.partial(_.eq, kernel.self()).bind(_.eq)), undefined);
          assert.equal(id, oldID);
          state = JSON.parse(state).data;
          assert.equal(state.type, "leave");
          assert.notEqual(state.actor, gossip._actor);
          assert.equal(state.round, 0);
        });
        gossip._closeRing();
        assert.notOk(gossip._ringID);
        assert.ok(gossip.emit.called);
        gossip.emit.restore();
        fns.forEach((fn) => {gossip[fn].restore();});
        kernel.abcast.restore();
      });

      it("Should leave clock", function () {
        var actor = uuid.v4();
        var oldClock = gossip.vclock();
        var oldCount = gossip.vclock().getCount(actor);
        assert.equal(oldCount, undefined);
        var clock = gossip._leaveClock(actor);
        assert.notEqual(actor, gossip._actor);
        assert.equal(gossip.vclock().size(), 1);
        assert.ok(gossip.vclock().has(gossip._actor));
        assert.equal(clock.getCount(actor), 1);
        clock.nodes().forEach((node) => {
          assert(oldClock.has(node));
        });
      });

      it("Should leave ring", function () {
        var node2 = new Node("id2", host, port+1);
        gossip.ring().insert(node2);
        assert(gossip.ring().size() > gossip.ring().rfactor());
        var ring = gossip._leaveRing();
        assert.equal(gossip.ring().size(), gossip.ring().rfactor());
        assert(gossip.ring().isDefined(gossip.kernel().self()));
        assert.notOk(ring.isDefined(gossip.kernel().self()));
      });
    });

    describe("Gossip static tests", function () {
      it("Should encode state", function () {
        var state = "foo";
        var exp = Buffer.from(JSON.stringify(state));
        assert.equal(Buffer.compare(GossipRing._encodeState(state), exp), 0);
      });

      it("Should calculate ring diff", function () {
        var ring1 = new CHash(3, 3);
        var ring2 = new CHash(3, 3);

        // case of only additions
        var node1 = new Node("id", "localhost", 8000);
        ring1.insert(node1);
        assert.deepEqual(GossipRing._ringDiff(ring1, ring2), [[node1], []]);

        // case of only removals
        var node2 = new Node("id2", "localhost", 8001);
        ring2.insert(node1).insert(node2);
        assert.deepEqual(GossipRing._ringDiff(ring1, ring2), [[], [node2]]);

        // case of both
        var node3 = new Node("id3", "localhost", 8002);
        ring1.insert(node3);
        assert.deepEqual(GossipRing._ringDiff(ring1, ring2), [[node3], [node2]]);
      });

      it("Should find max number of message rounds to send message with", function () {
        var ring = new CHash(3, 3);
        assert.equal(GossipRing.maxMsgRound(ring), 0);
        ring.insert(new Node("id", "localhost", 8000));
        assert.equal(GossipRing.maxMsgRound(ring), 1);
        ring.insert(new Node("id2", "localhost", 8001));
        assert.equal(GossipRing.maxMsgRound(ring), 1);
      });
    });
  });
};

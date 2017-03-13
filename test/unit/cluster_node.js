var _ = require("lodash"),
    async = require("async"),
    uuid = require("uuid"),
    sinon = require("sinon"),
    fs = require("fs"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("ClusterNode unit tests", function () {
    var VectorClock = lib.vclock,
        CHash = lib.chash,
        NetKernel = lib.kernel,
        GossipRing = lib.gossip,
        ClusterNode = lib.cluster_node,
        Node = lib.node,
        MockIPC = mocks.ipc;

    describe("ClusterNode state tests", function () {
      var kernel,
          nkernel,
          gossip,
          vclock,
          chash,
          cluster,
          opts,
          id = "id",
          host = "localhost",
          port = 8000;

      before(function () {
        kernel = new NetKernel(new MockIPC(), id, host, port);
        nkernel = new NetKernel(new MockIPC(), "id2", host, port+1);
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
        cluster = new ClusterNode(kernel, gossip);
      });

      it("Should construct a cluster", function () {
        assert.deepEqual(cluster._kernel, kernel);
        assert.deepEqual(cluster._gossip, gossip);
      });

      it("Should grab kernel", function () {
        cluster._kernel = "foo";
        assert.equal(cluster.kernel(), cluster._kernel);
      });

      it("Should set kernel", function () {
        cluster.kernel("foo");
        assert.equal(cluster.kernel(), "foo");
      });

      it("Should grab gossip", function () {
        cluster._gossip = "foo";
        assert.equal(cluster.gossip(), cluster._gossip);
      });

      it("Should set gossip", function () {
        cluster.gossip("foo");
        assert.equal(cluster.gossip(), "foo");
      });

      it("Should load data from disk", function (done) {
        sinon.stub(cluster._gossip, "load", (cb) => {
          cluster._gossip.ring().insert(new Node("id2", host, port+1));
          async.nextTick(cb);
        });
        var adds = [];
        sinon.stub(cluster._kernel, "connect", function (node) {
          adds.push(node);
        });
        cluster.load(() => {
          assert.deepEqual(adds, [nkernel.self()]);
          cluster._gossip.load.restore();
          cluster._kernel.connect.restore();
          done();
        });
      });

      it("Should error out loading data from disk if error occurs in gossip load", function (done) {
        sinon.stub(cluster._gossip, "load", (cb) => {
          async.nextTick(_.partial(cb, new Error("error")));
        });
        cluster.load((err) => {
          assert.ok(err);
          cluster._gossip.load.restore();
          done();
        });
      });

      it("Should fail to start node due to mismatched ring IDs", function (done) {
        cluster.gossip()._ringID = "ring2";
        cluster.start("cookie", "ring", (err) => {
          assert.ok(err);
          cluster.gossip()._ringID = null;
          done();
        });
      });

      it("Should start node successfully", function (done) {
        cluster.start("cookie", "ring");
        cluster.once("ready", () => {
          assert.equal(cluster.gossip()._ringID, "ring");
          assert.equal(cluster.kernel()._cookie, "cookie");
          cluster.stop();
          done();
        });
      });

      it("Should start node successfully, use listener callback", function (done) {
        cluster.start("cookie", "ring", () => {
          assert.equal(cluster.gossip()._ringID, "ring");
          assert.equal(cluster.kernel()._cookie, "cookie");
          cluster.stop();
          done();
        });
      });

      // it("Should not start table if already started", function () {
      //   table._stopped = false;
      //   table.start("foo");
      //   assert.notOk(table._flushInterval);
      //   table._stopped = true;
      // });

      // it("Should start table", function () {
      //   var name = "foo";
      //   var events = ["state", "migrate"];
      //   var gEvents = ["process", "leave"];
      //   var oldCounts = gEvents.reduce((memo, val) => {
      //     memo[val] = gossip.listeners(val).length;
      //     return memo;
      //   }, {});
      //   table.start(name);
      //   assert.equal(table._id, name);
      //   assert.lengthOf(table.kernel().listeners(name), 1);
      //   events.forEach((val) => {
      //     assert.lengthOf(table.listeners(val), 1);
      //   });
      //   gEvents.forEach((val) => {
      //     assert.lengthOf(gossip.listeners(val), oldCounts[val]+1);
      //   });
      //   assert.lengthOf(table.listeners("stop"), events.length + gEvents.length);
      //   assert.lengthOf(table.listeners("pause"), 1);
      //   assert.deepEqual(gossip._tables.get(name), table);
      //   assert.ok(table._pollInterval);
      //   assert.ok(table._flushInterval);
      //   table.stop(true);
      // });

      // it("Should fail to stop table if already stopped", function () {
      //   table._stopped = true;
      //   table.stop();
      //   assert.equal(table._stopped, true);
      // });

      // it("Should pause table", function () {
      //   var name = "foo";
      //   table.start(name);
      //   table.pause();
      //   assert.lengthOf(table.listeners("pause"), 0);
      //   assert.lengthOf(table.kernel().listeners(name), 0);
      //   assert.notOk(table._pollInterval);
      //   assert.notOk(table._flushInterval);
      //   [
      //     {cursor: "_pollCursor", next: "_nextPoll"},
      //     {cursor: "_migrateCursor", next: "nextMigrate"},
      //     {cursor: "_flushCursor"},
      //     {cursor: "_purgeCursor"}
      //   ].forEach((ent) => {
      //     assert.notOk(table[ent.cursor]);
      //     assert.notOk(table[ent.next]);
      //   });
      //   assert.equal(table._migrations.size, 0);
      // });

      // it("Should resume table", function () {
      //   var name = "foo";
      //   table.start(name);
      //   table.pause();
      //   table.resume();
      //   assert.lengthOf(table.listeners("pause"), 1);
      //   assert.lengthOf(table.kernel().listeners(name), 1);
      //   assert.ok(table._pollInterval);
      //   assert.ok(table._flushInterval);
      // });
    });
  });
};

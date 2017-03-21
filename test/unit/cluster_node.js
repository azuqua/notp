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

      it("Should stop node non-forcefully", function (done) {
        cluster.start("cookie", "ring", () => {
          cluster.once("stop", () => {
            assert.notOk(cluster.gossip()._ringID);
            assert.equal(cluster.kernel().sources().size, 0);
            done();
          });
          cluster.stop(false);
        });
      });

      it("Should stop node forcefully", function (done) {
        cluster.start("cookie", "ring", () => {
          cluster.once("stop", () => {
            assert.notOk(cluster.gossip()._ringID);
            assert.equal(cluster.kernel().sources().size, 0);
            done();
          });
          cluster.stop(true);
        });
      });
    });
  });
};

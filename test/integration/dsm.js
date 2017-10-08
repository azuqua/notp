var _ = require("lodash"),
    async = require("async"),
    os = require("os"),
    ipc = require("node-ipc"),
    assert = require("chai").assert;

var host = os.hostname();

function findKeyForOther(node, ring) {
  var node1;
  var total = ring.weights().get(node.id());
  for (var i = 1; i <= total; ++i) {
    var inner = ring.find(node.id() + "_" + i);
    if (inner.id() !== node.id()) node1 = (node.id() + "_" + i);
    break;
  }
  return node1;
}

module.exports = function (mocks, lib) {
  var DSMServer = lib.dsem.DSMServer;
  var Gossip = lib.gossip;
  var NetKernel = lib.kernel;
  var CHash = lib.chash;
  var VClock = lib.vclock;
  var consts = lib.consts;

  var kernelOpts = consts.kernelOpts;

  describe("DSM integration tests", function () {
    var nodes = [];
    var dsms = [];
    var origin, target;
    var key1, key2;
    var node1 = "foo", node2 = "bar";
    var port1 = 8000, port2 = 8001;

    before(function (done) {
      async.each([[node1, port1], [node2, port2]], (config, next) => {
        var inst = new ipc.IPC();
        inst.config.networkHost = host;
        inst.config.networkPort = config[1];
        inst.config.retry = kernelOpts.retry;
        inst.config.maxRetries = kernelOpts.maxRetries;
        inst.config.tls = kernelOpts.tls;
        inst.config.silent = kernelOpts.silent;
        const kernel = new NetKernel(inst, config[0], host, config[1]);
        var chash = (new CHash(3, 2)).insert(kernel.self());
        var vclock = new VClock();
        const gossip = new Gossip(kernel, chash, vclock, consts.gossipOpts);

        const dsm = new DSMServer(gossip, kernel, {
          rquorum: 0.51,
          wquorum: 0.51
        });
        nodes.push({gossip: gossip, kernel: kernel});
        dsms.push(dsm);
        dsm.start("locker");
        gossip.start("ring");
        kernel.start({cookie: "cookie"});
        kernel.once("_ready", next);
      }, function () {
        origin = dsms[0];
        target = dsms[1];
        nodes[0].gossip.meet(nodes[1].kernel.self());
        nodes[0].gossip.once("process", () => {
          key1 = findKeyForOther(nodes[1].kernel.self(), nodes[0].gossip.ring());
          key2 = findKeyForOther(nodes[0].kernel.self(), nodes[0].gossip.ring());
          origin.create(key1, 2, () => {
            target.create(key2, 2, done);
          });
        });
      });
    });

    after(function () {
      dsms.forEach(function (dsm) {
        dsm.stop();
      });
      nodes.forEach(function (node) {
        node.gossip.stop(true);
        node.kernel.sinks().forEach(function (sink) {
          node.kernel.disconnect(sink, true);
        });
        node.kernel.stop();
      });
    });

    it("Should post to a semaphore", function (done) {
      origin.post(key1, "holder", 30000, (err, nodes) => {
        assert.notOk(err);
        assert.ok(origin._semaphores.has(key1));
        assert.notOk(target._semaphores.has(key1));
        origin.close(key1, "holder", done);
      }, 1000, 0);
    });

    it("Should post to an external semaphore", function (done) {
      origin.post(key2, "holder", 30000, (err, nodes) => {
        assert.notOk(err);
        assert.ok(target._semaphores.has(key2));
        assert.notOk(origin._semaphores.has(key2));
        target.close(key2, "holder", done);
      }, 1000, 0);
    });

    it("Should exhaust semaphore count on a node", function (done) {
      async.series([
        function (next) {
          origin.post(key1, "holder", 30000, (err) => {
            assert.notOk(err);
            assert.equal(origin._semaphores.get(key1).timeouts().size, 1);
            assert.notOk(target._semaphores.has(key1));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.post(key1, "holder2", 30000, (err) => {
            assert.notOk(err);
            assert.equal(origin._semaphores.get(key1).timeouts().size, 2);
            assert.notOk(target._semaphores.has(key1));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.close(key1, "holder", (err) => {
            assert.notOk(err);
            assert.equal(origin._semaphores.get(key1).timeouts().size, 1);
            assert.notOk(target._semaphores.has(key1));
            next();
          });
        },
        function (next) {
          origin.close(key1, "holder2", (err) => {
            assert.notOk(err);
            assert.equal(origin._semaphores.get(key1).timeouts().size, 0);
            assert.notOk(target._semaphores.has(key1));
            next();
          });
        }
      ], done);
    });

    it("Should exhaust semaphore count on an external node", function (done) {
      async.series([
        function (next) {
          origin.post(key2, "holder", 30000, (err) => {
            assert.notOk(err);
            assert.equal(target._semaphores.get(key2).timeouts().size, 1);
            assert.notOk(origin._semaphores.has(key2));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.post(key2, "holder2", 30000, (err) => {
            assert.notOk(err);
            assert.equal(target._semaphores.get(key2).timeouts().size, 2);
            assert.notOk(origin._semaphores.has(key2));
            next();
          }, 1000, 0);
        },
        function (next) {
          target.close(key2, "holder", (err) => {
            assert.notOk(err);
            assert.equal(target._semaphores.get(key2).timeouts().size, 1);
            assert.notOk(origin._semaphores.has(key2));
            next();
          });
        },
        function (next) {
          target.close(key2, "holder2", (err) => {
            assert.notOk(err);
            assert.equal(target._semaphores.get(key2).timeouts().size, 0);
            assert.notOk(origin._semaphores.has(key2));
            next();
          });
        }
      ], done);
    });

    it("Should fail to write to semaphore that doesn't exist", function (done) {
      async.series([
        function (next) {
          origin.post("id", "holder", 30000, (err) => {
            assert.ok(err);
            next();
          }, 1000, 0);
        },
        function (next) {
          target.post("id", "holder2", 30000, (err) => {
            assert.ok(err);
            next();
          }, 1000, 0);
        }
      ], done);
    });

    it("Should read semaphores", function (done) {
      async.series([
        function (next) {
          origin.read(key1, (err, out) => {
            assert.notOk(err);
            assert.equal(out.n, 2);
            assert.equal(out.active, 0);
            next();
          }, 1000);
        },
        function (next) {
          origin.read(key2, (err, out) => {
            assert.notOk(err);
            assert.equal(out.n, 2);
            assert.equal(out.active, 0);
            next();
          }, 1000, 0);
        }
      ], done);
    });
  });
};

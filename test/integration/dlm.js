var _ = require("lodash"),
    async = require("async"),
    os = require("os"),
    ipc = require("node-ipc"),
    assert = require("chai").assert;

var host = os.hostname();

module.exports = function (mocks, lib) {
  var DLMServer = lib.dlm.DLMServer;
  var Gossip = lib.gossip;
  var NetKernel = lib.kernel;
  var CHash = lib.chash;
  var VClock = lib.vclock;
  var consts = lib.consts;

  var kernelOpts = consts.kernelOpts;
  var gossipOpts = consts.gossipOpts;

  describe("DLM integration tests", function () {
    var nodes = [];
    var dlms = [];
    var origin, target;
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

        const dlm = new DLMServer(gossip, kernel, {
          rquorum: 0.51,
          wquorum: 0.51
        });
        nodes.push({gossip: gossip, kernel: kernel});
        dlms.push(dlm);
        dlm.start("locker");
        gossip.start("ring");
        kernel.start({cookie: "cookie"});
        kernel.once("_ready", next);
      }, function () {
        origin = dlms[0];
        target = dlms[1];
        nodes[0].gossip.meet(nodes[1].kernel.self());
        nodes[0].gossip.once("process", _.ary(done, 0));
      });
    });

    after(function () {
      dlms.forEach(function (dlm) {
        dlm.stop();
      });
      nodes.forEach(function (node) {
        node.gossip.stop(true);
        node.kernel.sinks().forEach(function (sink) {
          node.kernel.disconnect(sink, true);
        });
        node.kernel.stop();
      });
    });

    it("Should create write lock", function (done) {
      origin.wlock("id", "holder", 30000, (err, nodes) => {
        assert.notOk(err);
        assert.ok(origin._locks.has("id"));
        assert.ok(target._locks.has("id"));
        origin.wunlock("id", "holder", done);
      }, 1000, 0);
    });

    it("Should create write lock, and then fail subsequent write lock", function (done) {
      async.series([
        function (next) {
          origin.wlock("id", "holder", 30000, (err) => {
            assert.notOk(err);
            assert.ok(origin._locks.has("id"));
            assert.ok(target._locks.has("id"));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.wlock("id", "holder2", 30000, (err) => {
            assert.ok(err);
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.wunlock("id", "holder", (err) => {
            assert.notOk(err);
            assert.notOk(origin._locks.has("id"));
            assert.notOk(target._locks.has("id"));
            next();
          });
        }
      ], done);
    });

    it("Should create write lock, and then fail subsequent read lock", function (done) {
      async.series([
        function (next) {
          origin.wlock("id", "holder", 30000, (err) => {
            assert.notOk(err);
            assert.ok(origin._locks.has("id"));
            assert.ok(target._locks.has("id"));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.rlock("id", "holder2", 30000, (err) => {
            assert.ok(err);
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.wunlock("id", "holder", (err) => {
            assert.notOk(err);
            assert.notOk(origin._locks.has("id"));
            assert.notOk(target._locks.has("id"));
            next();
          });
        }
      ], done);
    });

    it("Should create read lock", function (done) {
      origin.rlock("id", "holder", 30000, (err) => {
        assert.notOk(err);
        assert.ok(origin._locks.has("id"));
        assert.ok(target._locks.has("id"));
        origin.runlock("id", "holder", done);
      }, 1000, 0);
    });

    it("Should create read lock, and then fail subsequent write lock", function (done) {
      async.series([
        function (next) {
          origin.rlock("id", "holder", 30000, (err) => {
            assert.notOk(err);
            assert.ok(origin._locks.has("id"));
            assert.ok(target._locks.has("id"));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.wlock("id", "holder2", 30000, (err) => {
            assert.ok(err);
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.runlock("id", "holder", (err) => {
            assert.notOk(err);
            assert.notOk(origin._locks.has("id"));
            assert.notOk(target._locks.has("id"));
            next();
          });
        }
      ], done);
    });

    it("Should create read lock, and then succeed subsequent read lock", function (done) {
      async.series([
        function (next) {
          origin.rlock("id", "holder", 30000, (err) => {
            assert.notOk(err);
            assert.ok(origin._locks.has("id"));
            assert.ok(target._locks.has("id"));
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.rlock("id", "holder2", 30000, (err) => {
            assert.notOk(err);
            assert.equal(origin._locks.get("id").timeout().size, 2);
            assert.equal(target._locks.get("id").timeout().size, 2);
            next();
          }, 1000, 0);
        },
        function (next) {
          origin.runlock("id", "holder", (err) => {
            assert.notOk(err);
            assert.equal(origin._locks.get("id").timeout().size, 1);
            assert.equal(target._locks.get("id").timeout().size, 1);
            next();
          });
        },
        function (next) {
          origin.runlock("id", "holder2", (err) => {
            assert.notOk(err);
            assert.notOk(origin._locks.has("id"));
            assert.notOk(target._locks.has("id"));
            next();
          });
        }
      ], done);
    });
  });
};

const cl = require("../../../index"),
      _ = require("lodash"),
      os = require("os"),
      assert = require("chai").assert,
      debug = require("debug")("examples:usage:dlm:test"),
      async = require("async"),
      DLMServer = cl.DLMServer;

const nodeID = process.argv[2],
      port = parseInt(process.argv[3]),
      nodeID2 = process.argv[4],
      port2 = parseInt(process.argv[5]),
      host = os.hostname();

var nodes = [];
var dlms = [];
async.each([[nodeID, port], [nodeID2, port2]], (config, next) => {
  const node = cl.createCluster(config[0], host, config[1]),
        gossip = node.gossip(),
        kernel = node.kernel();

  const dlm = new DLMServer(gossip, kernel, {
    rquorum: 0.51,
    wquorum: 0.51
  });
  nodes.push(node);
  dlms.push(dlm);
  node.load(() => {
    dlm.start("locker");
    node.start("cookie", "ring", () => {
      debug("Node %s listening on hostname %s, port %s!", config[0], host, config[1]);
      next();
    });
  });
}, () => {
  setTimeout(() => {
    // make sure nodes know about each other
    assert.ok(_.find(nodes[0].gossip().ring().nodes(), (node) => {
      return node.equals(nodes[1].kernel().self());
    }));
    var dlm = dlms[0];
    async.series([
      (next) => {
        dlm.wlock("id", "holder", 30000, (err, nodes) => {
          assert.notOk(err);
          debug("Successfully grabbed write lock on id '%s' with holder '%s'", "id", "holder");
          assert.lengthOf(nodes, 2);
          next();
        }, 1000, 1);
      },
      (next) => {
        dlm.wlock("id", "holder2", 30000, (err, wNodes) => {
          assert.ok(err);
          debug("Failed to grab write lock id '%s' with holder '%s'", "id", "holder2");
          next();
        }, 1000, 1);
      },
      (next) => {
        dlm.wunlock("id", "holder", (err) => {
          assert.notOk(err);
          next();
        });
      },
      (next) => {
        dlm.rlock("id", "holder", 30000, (err, nodes) => {
          assert.notOk(err);
          debug("Successfully grabbed read lock on id '%s' with holder '%s'", "id", "holder");
          assert.lengthOf(nodes, 2);
          next();
        }, 1000, 1);
      },
      (next) => {
        dlm.rlock("id", "holder2", 30000, (err, nodes) => {
          assert.notOk(err);
          debug("Successfully grabbed read lock on id '%s' with holder '%s'", "id", "holder2");
          next();
        }, 1000, 1);
      },
      (next) => {
        dlm.wlock("id", "holder3", 30000, (err, wNodes) => {
          assert.ok(err);
          debug("Failed to grab write lock id '%s' with holder '%s'", "id", "holder2");
          next();
        }, 1000, 1);     
      },
      (next) => {
        dlm.runlockAsync("id", "holder");
        dlm.runlockAsync("id", "holder2");
        next();
      }
    ], () => {
      dlms.forEach((dlm) => {
        dlm.stop();
      });
      debug("Done!");
      process.exit(0);
    });
  }, 1000);
  // make nodes meet each other first
  nodes[0].gossip().meet(nodes[1].kernel().self());
  debug("Waiting for nodes to meet each other, swear I had something for this...");
});

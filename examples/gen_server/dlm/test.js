const cl = require("../../../index"),
      os = require("os"),
      assert = require("chai").assert,
      debug = require("debug")("examples:gen_server:dlm:test"),
      async = require("async");

const nodeID = process.argv[2],
      port = parseInt(process.argv[3]),
      host = os.hostname();

const node = cl.createCluster(nodeID, host, port),
      gossip = node.gossip(),
      kernel = node.kernel();

const DLMServer = require("./dlm");
const dlm = new DLMServer(gossip, kernel, {
  rquorum: 0.51,
  wquorum: 0.51
});

// load node state
node.load(() => {
  // first start dlm, then start network kernel
  dlm.start("locker");
  node.start("cookie", "ring", () => {
    debug("Node %s listening on hostname %s, port %s!", nodeID, host, port);
    var holdingNodes;
    async.series([
      (next) => {
        dlm.wlock("id", "holder", 30000, (err, nodes) => {
          assert.notOk(err);
          debug("Successfully grabbed write lock on id '%s' with holder '%s'", "id", "holder");
          assert.isArray(nodes);
          holdingNodes = nodes;
          next();
        });
      },
      (next) => {
        dlm.wlock("id", "holder2", 30000, (err, wNodes) => {
          assert.ok(err);
          debug("Failed to grab write lock id '%s' with holder '%s'", "id", "holder2");
          next();
        });
      },
      (next) => {
        dlm.wunlock(holdingNodes, "id", "holder", (err) => {
          assert.notOk(err);
          next();
        });
      },
      (next) => {
        dlm.rlock("id", "holder", 30000, (err, nodes) => {
          assert.notOk(err);
          debug("Successfully grabbed read lock on id '%s' with holder '%s'", "id", "holder");
          assert.isArray(nodes);
          holdingNodes = nodes;
          next();
        });
      },
      (next) => {
        dlm.rlock("id", "holder2", 30000, (err, nodes) => {
          assert.notOk(err);
          debug("Successfully grabbed read lock on id '%s' with holder '%s'", "id", "holder2");
          next();
        });
      },
      (next) => {
        dlm.wlock("id", "holder3", 30000, (err, wNodes) => {
          assert.ok(err);
          debug("Failed to grab write lock id '%s' with holder '%s'", "id", "holder2");
          next();
        });     
      },
      (next) => {
        dlm.runlockAsync(holdingNodes, "id", "holder");
        dlm.runlockAsync(holdingNodes, "id", "holder2");
        next();
      }
    ], () => {
      dlm.stop(true);
      debug("Done!");
      process.exit(0);
    });
  });
});

const cl = require("../../../index"),
      _ = require("lodash"),
      os = require("os"),
      assert = require("chai").assert,
      debug = require("debug")("examples:usage:dsm:test"),
      async = require("async"),
      DSMServer = cl.DSMServer;

const nodeID = process.argv[2],
      port = parseInt(process.argv[3]),
      nodeID2 = process.argv[4],
      port2 = parseInt(process.argv[5]),
      host = os.hostname();

var nodes = [];
var dsms = [];
async.each([[nodeID, port], [nodeID2, port2]], (config, next) => {
  const node = cl.createCluster(config[0], host, config[1]),
        gossip = node.gossip(),
        kernel = node.kernel();

  const dsm = new DSMServer(gossip, kernel, {
    rquorum: 0.51,
    wquorum: 0.51
  });
  nodes.push(node);
  dsms.push(dsm);
  node.load(() => {
    dsm.start("sem_server");
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
    var dsm = dsms[0];
    async.series([
      (next) => {
        dsm.create("id", 2, (err) => {
          assert.notOk(err);
          debug("Successfully created semaphore 'id' with concurrency limit of 3");
          next();
        });
      },
      (next) => {
        dsm.read("id", (err, out) => {
          assert.notOk(err);
          assert.deepEqual(out, {
            n: 2,
            active: 0
          });
          next();
        });
      },
      (next) => {
        dsm.post("id", "holder", 10000, (err) => {
          assert.notOk(err);
          debug("Successfully grabbed semaphore 'id' with holder 'holder'");
          next();
        });
      },
      (next) => {
        dsm.post("id", "holder2", 10000, (err) => {
          assert.notOk(err);
          debug("Successfully grabbed semaphore 'id' with holder 'holder2'");
          next();
        });
      },
      (next) => {
        dsm.post("id", "holder3", 10000, (err) => {
          assert.ok(err);
          debug("Failed to grab semaphore 'id' with holder 'holder3', limit reached");
          next();
        }, 1000, 0);
      },
      (next) => {
        dsm.read("id", (err, out) => {
          assert.notOk(err);
          assert.deepEqual(out, {
            n: 2,
            active: 2
          });
          next();
        });
      },
      (next) => {
        async.each(["holder", "holder2"], (holder, done) => {
          dsm.close("id", holder, done);
        }, next);
      },
      (next) => {
        dsm.read("id", (err, out) => {
          assert.notOk(err);
          assert.deepEqual(out, {
            n: 2,
            active: 0
          });
          next();
        });
      },
      (next) => {
        dsm.destroy("id", next);
      }
    ], () => {
      dsms.forEach((dsm) => {
        dsm.stop();
      });
      debug("Done!");
      process.exit(0);
    });
  }, 1000);
  // make nodes meet each other first
  nodes[0].gossip().meet(nodes[1].kernel().self());
  debug("Waiting for nodes to meet each other, swear I had something for this...");
});

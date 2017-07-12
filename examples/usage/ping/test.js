const cl = require("../../../index"),
      _ = require("lodash"),
      os = require("os"),
      assert = require("chai").assert,
      debug = require("debug")("examples:usage:ping:test"),
      async = require("async"),
      PingServer = require("./ping");

const nodeID = process.argv[2],
      port = parseInt(process.argv[3]),
      nodeID2 = process.argv[4],
      port2 = parseInt(process.argv[5]),
      host = os.hostname();

const nodes = [];
const servers = [];
async.each([[nodeID, port], [nodeID2, port2]], (config, next) => {
  const node = cl.createCluster(config[0], host, config[1]),
        kernel = node.kernel();

  const ping = new PingServer(kernel);
  nodes.push(node);
  servers.push(ping);
  node.load(() => {
    ping.start("ping_server");
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
    const server = servers[0];
    server.ping(nodes[1].kernel().self(), (err, res) => {
      assert.notOk(err);
      assert.equal(res, "pong");
      debug("Successfully received pong!");
      process.exit(0);
    });
  }, 1000);
  // make nodes meet each other first
  nodes[0].gossip().meet(nodes[1].kernel().self());
  debug("Waiting for nodes to meet each other, swear I had something for this...");
});

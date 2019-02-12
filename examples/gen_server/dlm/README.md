DLM example
===========

A distributed lock manager `gen_server` implementation for reference, using the [Redlock algorithm](https://redis.io/topics/distlock). May be added into notp as an optional feature if there's community interest.

Example Usage
-------------

A simple example usage of this `gen_server` follows. For a more detailed integration test on a single node, refer to `test.js`. For a multi-node integration test, refer to `multi_test.js`. In this test, we try to lock the same resource with two different requesters.

``` javascript
const cl = require("../../index"),
      os = require("os"),
      assert = require("chai").assert;

const nodeID = "name",
      port = 7022,
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
    console.log("Node %s listening on hostname %s, port %s", nodeID, host, port);
    dlm.wlock("id", "holder", 30000, (err, nodes) => {
      assert.notOk(err);
      assert.isArray(nodes);
      dlm.wlock("id", "holder", 30000, (err, wNodes) => {
        assert.ok(err);
      });
    });
  });
});
```

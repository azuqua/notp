const cl = require("../../../index"),
      DLMServer = require("./index");

const node = cl.createCluster("node_id");
const server = new DLMServer(node.gossip(), node.kernel());

server.start("dlm_server");
server.load((err) => {
  if (err) process.exit(1);
  server.on("rlock", (data, from) => {
    console.log("Received rlock request on %s with holder %s", data.id, data.holder);
  });
  server.on("wlock", (data, from) => {
    console.log("Received rlock request on %s with holder %s", data.id, data.holder);
  });
  
  node.start("cookie", "ring", () => {
    console.log("Node %s listening!", node.kernel().self().id());
  });
});

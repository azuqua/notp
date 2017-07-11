const cl = require("../../../index"),
      DSMServer = require("./index");

const node = cl.createCluster("node_id");
const server = new DSMServer(node.gossip(), node.kernel());

server.start("dsm_server");
server.load((err) => {
  if (err) process.exit(1);
  server.on("create", (data, from) => {
    console.log("Received create request on semaphore %s with concurrency limit %i", data.id, data.n);
  });
  server.on("post", (data, from) => {
    console.log("Received post request on %s with holder %s", data.id, data.holder);
  });
  server.on("close", (data, from) => {
    console.log("Received close request on %s with holder %s", data.id, data.holder);
  });
  
  node.start("cookie", "ring", () => {
    console.log("Node %s listening!", node.kernel().self().id());
  });
});

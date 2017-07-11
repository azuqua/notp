const cl = require("../../../index"),
      PingServer = require("./ping");

const node = cl.createCluster("node_id");
const pingServe = new PingServer(node.kernel());

pingServe.start("ping_server");
pingServe.on("ping", (data, from) => {
  console.log("Received ping request from:", from);
});

node.start("cookie", "ring", () => {
  console.log("Node %s listening!", node.kernel().self().id());
});

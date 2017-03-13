module.exports = function (mocks, lib) {
  describe("Unit tests", function () {
    require("./node")(mocks, lib);
    require("./queue")(mocks, lib);
    require("./conn")(mocks, lib);
    require("./chash")(mocks, lib);
    require("./vclock")(mocks, lib);
    require("./kernel")(mocks, lib);
    require("./gen_server")(mocks, lib);
    require("./gossip")(mocks, lib);
    require("./table")(mocks, lib);
    require("./dtable")(mocks, lib);
    require("./table_term")(mocks, lib);
    require("./utils")(mocks, lib);
    require("./cluster_node")(mocks, lib);
  });
};

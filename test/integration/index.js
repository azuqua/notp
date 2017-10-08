module.exports = function (mocks, lib) {
  describe("Integration tests", function () {
    require("./gen_server")(mocks, lib);
    require("./dlm")(mocks, lib);
    require("./dsm")(mocks, lib);
  });
};

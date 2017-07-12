module.exports = function (mocks, lib) {
  describe("DLM unit tests", function () {
    require("./lock")(mocks, lib);
    require("./dlm")(mocks, lib);
  });
};

module.exports = function (mocks, lib) {
  describe("DSem unit tests", function () {
    require("./semaphore")(mocks, lib);
    require("./dsm")(mocks, lib);
  });
};

var _ = require("lodash"),
    path = require("path");

describe("Clusterluck tests", function () {
  var mocks = require("./mocks");
  var lib = require("../lib");
  require(path.join(__dirname, "unit"))(mocks, lib);
  // require(path.join(__dirname, "integration"))(mocks, lib);
});

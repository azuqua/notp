var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Node unit tests", function () {
    var Node = lib.node;
    var node,
        id = "id",
        host = "localhost",
        port = 8000;
    beforeEach(function () {
      node = new Node(id, host, port);
    });

    it("Should construct a node", function () {
      assert.equal(node._id, id);
      assert.equal(node._host, host);
      assert.equal(node._port, port);

      node = new Node("id2", host, port);
      assert.equal(node._id, "id2");
      assert.equal(node._host, host);
      assert.equal(node._port, port);
    });

    it("Should get/set the id of a node", function () {
      assert.equal(node.id(), id);
      node.id("id2");
      assert.equal(node.id(), "id2");
      node.id(5);
      assert.equal(node.id(), "id2");
    });

    it("Should get/set the host of a node", function () {
      assert.equal(node.host(), host);
      node.host("localhost2");
      assert.equal(node.host(), "localhost2");
      node.host(5);
      assert.equal(node.host(), "localhost2");
    });

    it("Should get/set the port of a node", function () {
      assert.equal(node.port(), port);
      node.port(8001);
      assert.equal(node.port(), 8001);
      node.port("foo");
      assert.equal(node.port(), 8001);
    });

    it("Should compare two different nodes", function () {
      assert(node.equals(node));
      var node2 = new Node("id2", "localhost2", 8001);
      assert.notOk(node.equals(node2));
    });

    it("Should turn node into JSON", function () {
      var out = node.toJSON();
      assert.deepEqual(out, {
        id: node.id(),
        host: node.host(),
        port: node.port()
      });
      out = node.toJSON(true);
      assert.deepEqual(out, {
        id: node.id(),
        host: node.host(),
        port: node.port()
      });
    });

    it("Should create node from JSON", function () {
      var out = node.toJSON(true);
      var node2 = Node.from(out);
      assert(node.equals(node2));
    });
  });
};

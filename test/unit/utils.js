var _ = require("lodash"),
    async = require("async"),
    assert = require("chai").assert;

module.exports = function (mocks, lib) {
  describe("Utils unit tests", function () {
    var utils = lib.utils;
    var Node = lib.node;

    it("Should return map values", function () {
      var m = new Map([["foo", "bar"]]);
      assert.deepEqual(utils.mapValues(m), ["bar"]);
    });

    it("Should make an object out of an error", function () {
      var err = _.extend(new Error("foo"), {hello: "world"});
      var old = process.env.NODE_ENV;
      process.env.NODE_ENV = "production";
      assert.deepEqual(utils.errorToObject(err), {
        message: "foo",
        hello: "world",
        _error: true
      });

      process.env.NODE_ENV = old;
      assert.deepEqual(_.omit(utils.errorToObject(err), "stack"), {
        message: "foo",
        hello: "world",
        _error: true
      });

      assert.deepEqual(utils.errorToObject({message: "foo"}), {
        message: "foo",
        _error: true
      });
    });

    it("Should scan iterator, consuming all values", function () {
      var m = new Map([["foo", "bar"]]);
      var out = utils.scanIterator(m.entries(), 2);
      assert.deepEqual(out.values, [["foo", "bar"]]);
      assert.equal(out.iterator.next().done, true);
    });

    it("Should scan iterator, consuming exact amount of values", function () {
      var m = new Map([["foo", "bar"]]);
      var out = utils.scanIterator(m.entries(), 1);
      assert.deepEqual(out.values, [["foo", "bar"]]);
      assert.equal(out.iterator.next().done, true);
    });

    it("Should safely parse JSON", function () {
      var val = JSON.stringify("foo");
      var out = utils.safeParse(val);
      assert.equal(out, "foo");

      val = "foo";
      out = utils.safeParse(val);
      assert.ok(out instanceof Error);
    });

    it("Should return if value is a plain object", function () {
      var val = "foo";
      assert.notOk(utils.isPlainObject(val));
      
      val = {};
      assert.ok(utils.isPlainObject(val));
    });

    it("Should check if data payload has id param", function () {
      var data = {id: ""};
      var out = utils.hasID(data);
      assert.ok(out instanceof Error);

      data = {id: 5};
      out = utils.hasID(data);
      assert.ok(out instanceof Error);

      data = {id: "foo"};
      out = utils.hasID(data);
      assert.deepEqual(out, data);
    });

    it("Should fail to parse node, bad format", function () {
      var out = utils.parseNode({node: ""});
      assert.ok(out instanceof Error);
    });

    it("Should fail to parse node, bad entry", function () {
      var bads = [
        {id: 1, host: 1, port: "foo"},
        {id: 1, host: "localhost", port: 8000},
        {id: 1, host: 1, port: 8000},
        {id: 1, host: "localhost", port: "foo"},
        {id: "foo", host: 1, port: 8000},
        {id: "foo", host: 1, port: "foo"},
        {id: "foo", host: "localhost", port: "foo"}
      ];
      bads.forEach((bad) => {
        var out = utils.parseNode({node: bad});
        assert.ok(out instanceof Error);
      });
    });

    it("Should successfully parse node", function () {
      var out = utils.parseNode({node: {id: "foo", host: "localhost", port: 8000}});
      assert.ok(out.node.equals(new Node("foo", "localhost", 8000)));
    });

    it("Should fail to parse list of nodes with memo, bad format", function () {
      // first entry not an object
      var out = utils.parseNodeList({nodes: [""]}, []);
      assert.ok(out instanceof Error);
    });

    it("Should fail to parse list of nodes with memo, bad entry", function () {
      // id should be a string
      var out = utils.parseNodeList({nodes: [{id: 5}]}, []);
      assert.ok(out instanceof Error);
    });

    it("Should successfully parse list of nodes with memo", function () {
      var out = utils.parseNodeList({
        nodes: [{id: "foo", host: "localhost", port: 8000}]
      }, []);
      assert.ok(Array.isArray(out.nodes));
      assert.lengthOf(out.nodes, 1);
      assert.ok(out.nodes[0].equals(new Node("foo", "localhost", 8000)));
    });

    it("Should fail to parse list of nodes, bad format", function () {
      var out = utils.parseNodeList({nodes: ""});
      assert.ok(out instanceof Error);
    });

    it("Should transform map to object", function () {
      var list = [["key", "val"], ["key2", "val2"]];
      var map = new Map(list);
      var out = utils.mapToObject(map);
      assert.deepEqual(out, {
        key: "val",
        key2: "val2"
      });
    });

    it("Should transform set to list", function () {
      var list = ["val", "val2"];
      var set = new Set(list);
      var out = utils.setToList(set);
      assert.deepEqual(out, list);
    });

    it("Should transform map to list", function () {
      var list = [["key", "val"], ["key2", "val2"]];
      var map = new Map(list);
      var out = utils.mapToList(map);
      assert.deepEqual(out, list);
    });
  });
};

var _ = require("lodash"),
    async = require("async"),
    os = require("os"),
    ipc = require("node-ipc"),
    assert = require("chai").assert;

var host = os.hostname();

module.exports = function (mocks, lib) {
  var GenServer = lib.gen_server;
  var Gossip = lib.gossip;
  var NetKernel = lib.kernel;
  var CHash = lib.chash;
  var VClock = lib.vclock;
  var consts = lib.consts;

  var kernelOpts = consts.kernelOpts;

  describe("GenServer integration tests", function () {
    var nodes = [];
    var serves = [];
    var origin, target;
    var node1 = "foo", node2 = "bar";
    var port1 = 8000, port2 = 8001;

    before(function (done) {
      async.each([[node1, port1], [node2, port2]], (config, next) => {
        var inst = new ipc.IPC();
        inst.config.networkHost = host;
        inst.config.networkPort = config[1];
        inst.config.retry = kernelOpts.retry;
        inst.config.maxRetries = kernelOpts.maxRetries;
        inst.config.tls = kernelOpts.tls;
        inst.config.silent = kernelOpts.silent;
        const kernel = new NetKernel(inst, config[0], host, config[1]);
        var chash = (new CHash(3, 2)).insert(kernel.self());
        var vclock = new VClock();
        const gossip = new Gossip(kernel, chash, vclock, consts.gossipOpts);

        const server = new GenServer(kernel, {
          rquorum: 0.51,
          wquorum: 0.51
        });
        server.on("echo", (data, from) => {
          server.reply(from, data);
        });
        server.on("one-way", (data, from) => {
          server.emit("have a bone");
        });

        nodes.push({gossip: gossip, kernel: kernel});
        serves.push(server);
        server.start("server_here");
        gossip.start("ring");
        kernel.start({cookie: "cookie"});
        kernel.once("_ready", next);
      }, function () {
        origin = serves[0];
        target = serves[1];
        nodes[0].gossip.meet(nodes[1].kernel.self());
        nodes[0].gossip.once("process", _.ary(done, 0));
      });
    });

    after(function () {
      serves.forEach(function (server) {
        server.stop(true);
      });
      nodes.forEach(function (node) {
        node.gossip.stop(true);
        node.kernel.sinks().forEach(function (sink) {
          node.kernel.disconnect(sink, true);
        });
        node.kernel.stop();
      });
    });

    it("Should send a call internally", function (done) {
      origin.call("server_here", "echo", "bar", (err,res) => {
        assert.notOk(err);
        assert.equal(res, "bar");
        done();
      });
    });

    it("Should send a call externally", function (done) {
      origin.call({id: "server_here", node: target.kernel().self()}, "echo", "bar", (err,res) => {
        assert.notOk(err);
        assert.equal(res, "bar");
        done();
      });
    });

    it("Should send a cast internally", function (done) {
      origin.once("have a bone", _.ary(done, 0));
      origin.cast("server_here", "one-way", "bar");
    });

    it("Should send a cast externally", function (done) {
      origin.cast({id: "server_here", node: target.kernel().self()}, "one-way", "bar");
      target.once("have a bone", () => {
        target.cast({id: "server_here", node: origin.kernel().self()}, "one-way", "baz");
      });
      origin.once("have a bone", _.ary(done, 0));
    });
  });
};

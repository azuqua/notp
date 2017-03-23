var _ = require("lodash"),
    ipc = require("node-ipc"),
    os = require("os"),
    util = require("util"),
    lib = require("./lib");

var utils = lib.utils;

var consts = lib.consts;

function createCHash(rfactor = 3, pfactor = 2) {
  return new lib.chash(rfactor, pfactor);
}

function createVClock(id, count) {
  return new lib.vclock(id, count);
}

function createGossip(kernel, opts) {
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, consts.gossipOpts);
  var chash = createCHash(opts.rfactor, opts.pfactor).insert(kernel.self());
  var vclock = createVClock();
  return new lib.gossip(kernel, chash, vclock, opts);
}

function createKernel(id, host = os.hostname(), port = 7022, opts = {}) {
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, consts.kernelOpts);
  var inst = new ipc.IPC();
  inst.config.networkHost = host || opts.networkHost;
  inst.config.networkPort = port || opts.networkPort;
  inst.config.retry = opts.retry;
  inst.config.maxRetries = opts.maxRetries;
  inst.config.tls = opts.tls;
  inst.config.silent = opts.silent;
  return new lib.kernel(inst, id, opts.networkHost, opts.networkPort);
}

function createCommServer(gossip, kernel) {
  return new lib.command_server(gossip, kernel);
}

function createCluster(id, host = os.hostname(), port = 7022, opts = {}) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  var kernel = createKernel(id, host, port, opts.kernelOpts);
  var gossip = createGossip(kernel, opts.gossipOpts);
  var comms = createCommServer(gossip, kernel);
  return new lib.cluster_node(kernel, gossip, comms);
}

function createGenServer(cluster) {
  return new lib.gen_server(cluster.kernel());
}

module.exports = {
  CHash: lib.chash,
  ClusterNode: lib.cluster_node,
  GenServer: lib.gen_server,
  GossipRing: lib.gossip,
  NetKernel: lib.kernel,
  Node: lib.node,
  StateTable: lib.table,
  TableTerm: lib.table_term,
  VectorClock: lib.vclock,
  createCHash: createCHash,
  createVClock: createVClock,
  createGossip: createGossip,
  createCluster: createCluster,
  createKernel: createKernel,
  createGenServer: createGenServer,
  consts: consts
};

var _ = require("lodash"),
    ipc = require("node-ipc"),
    os = require("os"),
    util = require("util"),
    lib = require("./lib");

var utils = lib.utils;

var vclockOpts = {
  lowerBound: 10,
  youndBound: 20000,
  upperBound: 50,
  oldBound: 86400000
};

var consts = {
  networkHost: os.hostname(),
  networkPort: 7022,
  kernelOpts: Object.freeze({
    networkHost: consts.networkHost,
    networkPort: consts.networkPort,
    retry: 5000,
    tls: {},
    silent: true
  }),
  gossipOpts: Object.freeze({
    interval: 1000,
    flushInterval: 1000,
    flushPath: null,
    vclockOpts: _.cloneDeep(vclockOpts)
  }),
  tableOpts: Object.freeze({
    pollOpts: {
      interval: 5000,
      block: 100
    },
    disk: false,
    vclockOpts: _.cloneDeep(vclockOpts),
    purgeMax: 5
  }),
  vclockOpts: Object.freeze(_.cloneDeep(vclockOpts))
};

function createCHash(rfactor, pfactor) {
  return new lib.chash(rfactor, pfactor);
}

function createVClock(id, count) {
  return new lib.vclock(id, count);
}

function createGossip(kernel, opts) {
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, consts.gossipOpts);
  var chash = (new lib.chash()).insert(kernel.self());
  var vclock = new lib.vclock();
  return new lib.gossip(kernel, chash, vclock, opts);
}

function createKernel(id, opts) {
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, consts.kernelOpts);
  var inst = new ipc.IPC();
  inst.config.networkHost = opts.networkHost;
  inst.config.networkPort = opts.networkPort;
  inst.config.retry = opts.retry;
  inst.config.tls = opts.tls;
  inst.config.silent = opts.silent;
  return new lib.kernel(ipc, id, opts.networkHost, opts.networkPort);
}

function createCluster(id, host, port, opts) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  var kernel = createKernel(id, host, port, opts.kernelOpts);
  var gossip = createGossip(kernel, opts.gossipOpts);
  return new lib.cluster_node(kernel, gossip);
}

function createTable(cluster, name, opts) {
  var table;
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, opts.tableOpts);
  if (opts.disk !== true) {
    table = new lib.table(cluster.kernel(), cluster.gossip(), opts);
  }
  else {
    if (!utils.isPlainObject(opts.flushOpts)) {
      throw new Error("Expected 'flushOpts' key inside options input for disk-based table.");
    }
    table = new lib.dtable(cluster.kernel(), cluster.gossip(), opts);
  }
  table.id(name);
  return table;
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
  createTable: createTable,
  createGenServer: createGenServer,
  consts: consts
};

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

var networkHost = os.hostname();
var networkPort = 7022;

var consts = {
  networkHost: networkHost,
  networkPort: networkPort,
  kernelOpts: Object.freeze({
    networkHost: networkHost,
    networkPort: networkPort,
    retry: 5000,
    tls: null,
    silent: true
  }),
  gossipOpts: Object.freeze({
    rfactor: 3,
    pfactor: 2,
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
  var chash = createCHash(opts.rfactor, opts.pfactor).insert(kernel.self());
  var vclock = createVClock();
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
  return new lib.kernel(inst, id, opts.networkHost, opts.networkPort);
}

function createCluster(id, opts) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  var kernel = createKernel(id, opts.kernelOpts);
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

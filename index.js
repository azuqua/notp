var _ = require("lodash"),
    ipc = require("node-ipc"),
    os = require("os"),
    util = require("util"),
    lib = require("./lib");

var utils = lib.utils;

var consts = lib.consts;

/**
 *
 * Constructs a new instance of the consistent hash ring class.
 *
 * @method createCHash
 * @memberof Clusterluck
 *
 * @param {Number} rfactor - Replication factor for every node inserted into the ring. Defaults to 3.
 * @param {Number} pfactor - Persistence factor for every node inserted into the ring (used when calling .next on a consistent hash ring). Defaults to 2.
 *
 * @return {Clusterluck.CHash} A consistent hash ring instance.
 *
 * @example
 * let chash = clusterluck.createCHash(3, 2);
 * assert.equal(chash.rfactor(), 3);
 * assert.equal(chash.pfactor(), 2);
 *
 */
function createCHash(rfactor = 3, pfactor = 2) {
  return new lib.chash(rfactor, pfactor);
}

/**
 *
 * Constructs a new instance of the vector clock class.
 *
 * @method createVClock
 * @memberof Clusterluck
 *
 * @param {String} id - Identifier to insert this new vector clock on creation.
 * @param {Number} count - Count to initialize `id` at in this new vector clock.
 *
 * @return {Clusterluck.VectorClock} A vector clock instance.
 *
 * @example
 * let vclock = clusterluck.createVClock();
 * assert.equal(vclock.size(), 0);
 * vclock = clutserluck.createVClock("id", 1);
 * assert.equal(vclock.size(), 1);
 * assert.ok(vclock.has("id"));
 *
 */
function createVClock(id, count) {
  return new lib.vclock(id, count);
}

/**
 *
 * Constructs an instance of a gossip processor against network kernel `kernel`.
 *
 * @method createGossip
 * @memberof Clusterluck
 *
 * @param {Clusterluck.NetKernel} kernel - Network kernel this new gossip processor instance will listen for jobs against.
 * @param {Object} [opts] - Gossip ring options to instantiate with. Affects vector clock trimming options, consistent hash ring instantiation, how often to gossip ring state against the cluster, and when/where to flush state to disk.
 * @param {Number} [opts.rfactor] - Replication factor for every node inserted into the ring. Defaults to 3.
 * @param {Number} [opts.pfactor] - Persistence factor for every node inserted into the ring (used when calling .next on a consistent hash ring). Defaults to 2.
 * @param {Number} [opts.interval] - Interval to select a random node from the cluster and gossip the state of the ring with, with a granularity of milliseconds. Defaults to 1000.
 * @param {Number} [opts.flushInterval] - Interval to flush the state of the ring to disk, with a granularity of milliseconds. Defaults to 1000.
 * @param {String} [opts.flushPath] - Path string to flush the state of the ring to; if set to `null`, the gossip ring will just skip flushing state to disk. Defaults to `null`.
 * @param {Object} [opts.vclockOpts] - Vector clock options for trimming; occurs at the same interval as `interval`. Defaults to `clusterluck.consts.vclockOpts`.
 * @param {Object} [opts.connOpts] - Connection options for when connecting to new nodes.
 *
 * @return {Clusterluck.GossipRing} A new gossip ring instance.
 *
 * @example
 * // initializes gossip ring with defaults found in `clusterluck.consts.gossipOpts`
 * let gossip = clusterluck.createGossip(kernel);
 * assert.equal(gossip.ring().rfactor(), 3);
 * assert.equal(gossip.ring().pfactor(), 2);
 * assert.deepEqual(gossip.kernel(), kernel);
 *
 */
function createGossip(kernel, opts) {
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, consts.gossipOpts);
  var chash = createCHash(opts.rfactor, opts.pfactor).insert(kernel.self());
  var vclock = createVClock();
  return new lib.gossip(kernel, chash, vclock, opts);
}

/**
 *
 * Constructs an instance of a network kernel with `id`, listening on hostname `host` and port `port`.
 *
 * @method createKernel
 * @memberof Clusterluck
 *
 * @param {String} id - Identifier for the node associated with this network kernel. Needs to be unique across the cluster, since nodes are addressed by id this way.
 * @param {String} host - Hostname for this network kernel to bind to. Can be an IPV4 address, IPV6 address, or a hostname. Hostname resolution isn't done when checking the existence of a node inside a cluster, so this hostname is taken literally for the lifetime of the node (i.e. localhost vs. 127.0.0.1 vs `> hostname`). Defaults to `os.hostname()`.
 * @param {Number} port - Port for this network kernel to listen on. Defaults to 7022.
 * @param {Object} [opts] - Network kernel options to instantiate with. Affects whether the server runs with TLS or just TCP, on what interval to attempt reconnect logic on a closed socket, and how many times to retry.
 * @param {String} [opts.networkHost] - Default network hostname to set on this network kernel. Defaults to `os.hostname()`.
 * @param {Number} [opts.networkPort] - Default network port to listen on for this network kernel. Defaults to 7022.
 * @param {Number} [opts.retry] - Default amount of time to wait before retrying a connection attempt between two nodes. Defaults to 5000.
 * @param {Object} [opts.tls] - TLS options to set on this network kernel. Defaults to `null`.
 * @param {Number} [opts.maxRetries] - Maximum number of attempts to reconnect to a node; currently, Infinity is the most stable option, since the Connection class only listens for the 'connect' and 'disconnect' events on the underlying IPC socket. Defaults to Infinity.
 * @param {Boolean} [opts.silent] - Whether to silence underlying IPC logs emitted by the `node-ipc` module. Defaults to true.
 *
 * @return {Clusterluck.NetKernel} A new network kernel instance.
 *
 * @example
 * let kernel = clusterluck.createKernel("foo", "localhost", 7022);
 * assert.equal(kernel.id(), "foo");
 * assert.equal(kernel.host(), "localhost");
 * assert.equal(kernel.port(), 7022);
 * assert.ok(kernel.self().equals(new Node("foo", "localhost", 7022)));
 *
 */
function createKernel(id, host = os.hostname(), port = 7022, opts = {}) {
  opts = _.defaultsDeep(utils.isPlainObject(opts) ? _.cloneDeep(opts) : {}, consts.kernelOpts);
  var inst = new ipc.IPC();
  inst.config.networkHost = host || opts.networkHost;
  inst.config.networkPort = port || opts.networkPort;
  inst.config.retry = opts.retry;
  inst.config.maxRetries = opts.maxRetries;
  inst.config.tls = opts.tls;
  inst.config.silent = opts.silent;
  return new lib.kernel(inst, id, inst.config.networkHost, inst.config.networkPort);
}

/**
 *
 * Constructs an instance of a command server, which responds to CLI commands.
 *
 * @method createCommServer
 * @memberof Clusterluck
 * 
 * @param {Clusterluck.Gossip} gossip - Gossip processor for this command server to report/manipulate the state of.
 * @param {Clusterluck.Kernel} kernel - Network kernel this command server uses to reply over to a CLI process' IPC socket.
 *
 * @return {Clusterluck.CommandServer} A new command server instance.
 *
 * @example
 * let comms = clusterluck.createCommServer(gossip, kernel);
 * assert.deepEqual(comms.gossip(), gossip);
 * assert.deepEqual(comms.kernel(), kernel);
 *
 */
function createCommServer(gossip, kernel) {
  return new lib.command_server(gossip, kernel);
}

/**
 *
 * Constructs an instance of a cluster node, the preferred and encompassing way to start/stop the underlying IPC node, as well as refer to underlying actors in the cluster (gossip ring, kernel, command server, forthcoming actors, etc.).
 *
 * @method createCluster
 * @memberof Clusterluck
 *
 * @param {String} Identifier for the node associated with this network kernel. Needs to be unique across the cluster, since nodes are addressed by id this way.
 * @param {String} host - Hostname for this network kernel to bind to. Can be an IPV4 address, IPV6 address, or a hostname. Hostname resolution isn't done when checking the existence of a node inside a cluster, so this hostname is taken literally for the lifetime of the node (i.e. localhost vs. 127.0.0.1 vs `> hostname`). Defaults to `os.hostname()`.
 * @param {Number} port - Port for this network kernel to listen on. Defaults to 7022.
 * @param {Object} [opts] - Options object that controls configuration options for the constructed network kernel and gossip ring.
 * @param {Object} [opts.kernelOpts] - Refer to `createKernel` for an explanation of available options.
 * @param {Object} [opts.gossipOpts] - Refer to `createGossip` for an explanation of available options.
 *
 * @return {Clusterluck.ClusterNode} A new cluster node instance.
 *
 * @example
 * let node = clusterluck.createCluster("foo", "localhost", 7022);
 * assert.equal(node.kernel().id(), "foo");
 * assert.equal(node.kernel().host(), "localhost");
 * assert.equal(node.kernel().port(), 7022);
 * assert.ok(node.kernel().self().equals(new Node("foo", "localhost", 7022)));
 * assert.equal(node.gossip().ring().rfactor(), 3);
 * assert.equal(node.gossip().ring().pfactor(), 2);
 *
 */
function createCluster(id, host = os.hostname(), port = 7022, opts = {}) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  var kernel = createKernel(id, host, port, opts.kernelOpts);
  var gossip = createGossip(kernel, opts.gossipOpts);
  var comms = createCommServer(gossip, kernel);
  return new lib.cluster_node(kernel, gossip, comms);
}

/**
 *
 * Constructs a generic server instance. Generic servers listen to the network kernel for events targetted at it's name/ID. For example, the gossip ring is a generic server that listens for events on the ID of the ring it belongs to.
 *
 * @method createGenServer
 * @memberof Clusterluck
 *
 * @param {Clusterluck.ClusterNode} cluster - Cluster for this generic server to bind to.
 * @param {Object} [opts] - Options object for creating generic server.
 * @param {Number} [opts.streamTimeout] - Timeframe a generic server will receive data for a given stream before invalidating it.
 *
 * @return {Clusterluck.GenServer} A new generic server instance.
 *
 * @example
 * let server = clusterluck.createGenServer(cluster);
 * // based on how messages are parsed, will operate on event 'command_name' sent by another actor to this node
 * server.on("command_name", handlerForCommand);
 * // will listen on server.kernel() for messages emitted on event 'foo'.
 * server.start("foo");
 *
 */
function createGenServer(cluster, opts) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  return new lib.gen_server(cluster.kernel(), opts);
}

/**
 *
 * Constructs a dtable instance, an in-memory key/value storage that persists to disk periodically.
 *
 * @method createDTable
 * @memberof Clusterluck
 *
 * @param {Object} opts - Options object for creating dtable.
 * @param {String} opts.path - Directory store table snapshot and log files under.
 * @param {Number} [opts.writeThreshold] - Number of write operations to the log file before triggering a snapshot flush to disk. Defaults to 100 writes.
 * @param {Number} [opts.autoSave] - Number of milliseconds this table will wait in an idle state before triggering a snapshot flush to disk. Defaults to 180000 milliseconds.
 * @param {Number} [opts.fsyncInterval] - Internval in milliseconds to fsync the log file. Defaults to 1000 milliseconds.
 * @param {Boolean} [opts.compress] - Whether to run RDB snapshot streams through a GZIP compression stream. Defaults to `false`.
 * @param {Function} [opts.encodeFn] - Encoding function to use when serializing writes to the AOF file and when saving to the RDB snapshot. Defaults to `DTable.encodeValue`.
 * @param {Function} [opts.decodeFn] - Decoding function to use when loading contents from disk. Defaults to `DTable.decodeValue`.
 * @param {String} [opts.name] - Name to start table with; can be used as a replacement for passing `name` to the start function. Required to be passed if you don't want a race condition between table loads and the idle interval that runs to trigger RDB snapshot logic. Defaults to `undefined`.
 *
 * @return {Clusterluck.DTable} A new dtable instance.
 *
 * @example
 * let table = clusterluck.createDTable({
 *   path: "/path/to/dir",
 *   writeThreshold: 100,
 *   autoSave: 180000,
 *   fsyncInterval: 1000
 * });
 * table.start("foo");
 *
 * @example
 * let table = clusterluck.createDTable({
 *   path: "/path/to/dir",
 *   writeThreshold: 100,
 *   autoSave: 180000,
 *   fsyncInterval: 1000,
 *   name: "TABLE_NAME"
 * });
 * table.load((err) => {
 *   if (err) process.exit(1);
 *   table.start();
 * });
 *
 */
function createDTable(opts) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  return new lib.dtable(opts);
}

/**
 *
 * Constructs a mtable instance, an in-memory key/value storage.
 *
 * @method createMTable
 * @memberof Clusterluck
 *
 * @return {Clusterluck.MTable} A new mtable instance.
 *
 * @example
 * let table = clusterluck.createMTable();
 * table.start("foo");
 *
 */
function createMTable() {
  return new lib.mtable();
}

/**
 *
 * Constructs a DLM server instance. Handles creating read locks, write locks, as well as removing such locks across a cluster of nodes. See documentation of the DLMServer class for how locks are routed and partitioned across the cluster.
 *
 * @method createDLM
 * @memberof Clusterluck
 *
 * @param {Clusterluck.ClusterNode} cluster - Cluster for this generic server to bind to.
 * @param {Object} [opts] - Options object for creating DLM server.
 * @param {Number} [opts.rquorum] - Quorum for read lock requests.
 * @param {Number} [opts.wquorum] - Quorum for write lock requests.
 * @param {Number} [opts.minWaitTimeout] - Minimum amount of time in milliseconds to wait for a retry on a locking request.
 * @param {Number} [opts.maxWaitTimeout] - Maximum amount of time in milliseconds to wait for a retry on a locking request.
 * @param {Boolean} [opts.disk] - Whether to persist lock state to disk. If `true` is passed, the following options will be read.
 * @param {String} [opts.path] - Path for underlying DTable instance to flush state to.
 * @param {Number} [opts.writeThreshold] - Write threshold of underlying DTable instance.
 * @param {Number} [opts.autoSave] - Autosave interval of underlying DTable instance.
 * @param {Number} [opts.fsyncInterval] - Fsync interval of underlying DTable instance.
 *
 * @return {Clusterluck.DLMServer} A new generic server instance.
 *
 * @example
 * let server = clusterluck.createDLM(cluster, {disk: true, path: "/path/to/dir"});
 * server.load((err) => {
 *   server.start("foo");
 * });
 *
 */
function createDLM(cluster, opts) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  return new lib.dlm.DLMServer(cluster.gossip(), cluster.kernel(), opts);
}

/**
 *
 * Constructs a DSM server instance. Handles creating/reading/destroying semaphores, as well as posting and closing semaphores with requesters/actors. See documentation of the DSMServer class for how semaphores are routed and partitioned across the cluster.
 *
 * @method createDSM
 * @memberof Clusterluck
 *
 * @param {Clusterluck.ClusterNode} cluster - Cluster for this generic server to bind to.
 * @param {Object} [opts] - Options object for creating DSM server.
 * @param {Number} [opts.minWaitTimeout] - Minimum amount of time in milliseconds to wait for a retry on a post request.
 * @param {Number} [opts.maxWaitTimeout] - Maximum amount of time in milliseconds to wait for a retry on a post request.
 * @param {Boolean} [opts.disk] - Whether to persist semaphore state to disk. If `true` is passed, the following options will be read.
 * @param {String} [opts.path] - Path for underlying DTable instance to flush state to.
 * @param {Number} [opts.writeThreshold] - Write threshold of underlying DTable instance.
 * @param {Number} [opts.autoSave] - Autosave interval of underlying DTable instance.
 * @param {Number} [opts.fsyncInterval] - Fsync interval of underlying DTable instance.
 *
 * @return {Clusterluck.DSMServer} A new generic server instance.
 *
 * @example
 * let server = clusterluck.createDSM(cluster, {disk: true, path: "/path/to/dir"});
 * server.load((err) => {
 *   server.start("foo");
 * });
 *
 */
function createDSM(cluster, opts) {
  opts = utils.isPlainObject(opts) ? _.cloneDeep(opts) : {};
  return new lib.dsem.DSMServer(cluster.gossip(), cluster.kernel(), opts);
}

module.exports = {
  CHash: lib.chash,
  ClusterNode: lib.cluster_node,
  GenServer: lib.gen_server,
  GossipRing: lib.gossip,
  NetKernel: lib.kernel,
  Node: lib.node,
  VectorClock: lib.vclock,
  DTable: lib.dtable,
  MTable: lib.mtable,
  DLMServer: lib.dlm.DLMServer,
  DSMServer: lib.dsem.DSMServer,
  Lock: lib.dlm.Lock,
  Semaphore: lib.dsem.Semaphore,
  createCHash: createCHash,
  createVClock: createVClock,
  createGossip: createGossip,
  createCluster: createCluster,
  createKernel: createKernel,
  createGenServer: createGenServer,
  createDTable: createDTable,
  createMTable: createMTable,
  createDLM: createDLM,
  createDSM: createDSM,
  consts: consts
};

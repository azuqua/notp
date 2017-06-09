var _ = require("lodash"),
    os = require("os");

// vector clock trim defaults
var vclockOpts = {
  // number of elements that need to exist for trimming to occur
  lowerBound: 10,
  // how old the youngest member needs to be before considering trimming
  youngBound: 20000000,
  // when trimming, trim to at least this number of elements
  upperBound: 50,
  // when trimming, trim any members at least this old
  oldBound: 86400000000
};

var dtableOpts = {
  writeThreshold: 100,
  autoSave: 180000,
  fsyncInterval: 1000
};

var networkHost = os.hostname();
var networkPort = 7022;

module.exports = {
  networkHost: networkHost,
  networkPort: networkPort,
  // defaults for network kernel
  kernelOpts: Object.freeze({
    networkHost: networkHost,
    networkPort: networkPort,
    // how long to wait before retrying a connection attempt between
    // two nodes
    retry: 5000,
    // TLS options; see https://riaevangelist.github.io/node-ipc/ for accepted options
    tls: null,
    // number of attempts to reconnect to a node; currently Infinity is supported since
    // the Connection class only listens on the 'connect' and 'disconnect' events
    maxRetries: Infinity,
    // silence all node-ipc logs
    silent: true
  }),
  // defaults for gossip processor
  gossipOpts: Object.freeze({
    // replication factor for the consistent hash ring
    rfactor: 3,
    // persistence factor for the consistent hash ring
    pfactor: 2,
    // interval to communicate with a random member of the cluster
    interval: 1000,
    // interval to flush the ring to disk
    flushInterval: 1000,
    // path to flush state to; by default, state is not flushed
    flushPath: null,
    // vector clock options for managing the internal vector clock corresponding
    // to the hash ring
    vclockOpts: _.cloneDeep(vclockOpts)
  }),
  // vector clock defaults, in the event we want direct creation/manipulation of vector
  // clocks
  vclockOpts: Object.freeze(_.cloneDeep(vclockOpts)),
  dtableOpts: Object.freeze(dtableOpts),
  dlmOpts: Object.freeze(_.extend({
    rquorum: 0.51,
    wquorum: 0.51,
    minWaitTimeout: 10,
    maxWaitTimeout: 100,
    disk: false
  }, dtableOpts))
};

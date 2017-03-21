var _ = require("lodash"),
    os = require("os");

var vclockOpts = {
  lowerBound: 10,
  youndBound: 20000,
  upperBound: 50,
  oldBound: 86400000
};

var networkHost = os.hostname();
var networkPort = 7022;

module.exports = {
  networkHost: networkHost,
  networkPort: networkPort,
  kernelOpts: Object.freeze({
    networkHost: networkHost,
    networkPort: networkPort,
    retry: 5000,
    tls: null,
    maxRetries: Infinity,
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

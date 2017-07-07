var _ = require("lodash"),
    util = require("util");

var Node = require("./node");
var types = [
  {key: "id", valid: [util.isString], str: "Node 'id' must be a JSON string."},
  {key: "host", valid: [util.isString], str: "Node 'host' must be a JSON string."},
  {key: "port", valid: [util.isNumber], str: "Node 'port' must be a JSON number."}
];

function parseNode(node) {
  if (!util.isObject(node)) {
    return new Error("foo");
  }
  var misconfig = types.reduce((memo, check) => {
    var valid = _.some(check.valid, (fn) => {
      return fn(node[check.key]);
    });
    if (!valid) memo[check.key] = check.str;
    return memo;
  }, {});
  if (Object.keys(misconfig).length > 0) {
    return _.extend(new Error("Invalid node parameter types."), {
      fails: misconfig
    });
  }
  return new Node(node.id, node.host, node.port);
}

function parseNodeListMemo(nodes, memo) {
  if (nodes.length === 0) return memo;
  var first = nodes.pop();
  var out = parseNode(first);
  if (out instanceof Error) return out;
  memo.push(out);
  return parseNodeListMemo(nodes, memo);
}

var utils = {
  scanIterator: (iterator, num, memo = []) => {
    var value = iterator.next();
    if (value.done === true) return {iterator: iterator, done: true, values: memo};
    memo.push(value.value);
    num--;
    if (num === 0) return {iterator: iterator, values: memo};
    return utils.scanIterator(iterator, num, memo);
  },

  errorToObject: (err) => {
    err._error = err._error ? err._error : true;
    if (err instanceof Error) {
      return _.transform(err, (out, v, k) => {
        out[k] = v;
      }, _.extend(process.env.NODE_ENV === "production" ? {} : {stack: err.stack}, {
        message: err.message
      }));
    }
    return err;
  },

  mapValues: (map) => {
    var memo = [];
    map.forEach((val) => {
      memo.push(val);
    });
    return memo;
  },

  safeParse: (val, reviver) => {
    try {
      return JSON.parse(val, reviver);
    } catch (e) {
      return e;
    }
  },

  isPlainObject: (val) => {
    return util.isObject(val) && !Array.isArray(val);
  },

  hasID: (data) => {
    if (!data || (!util.isString(data.id) || !data.id)) {
      return new Error("Missing required 'id' parameter.");
    }
    return data;
  },

  parseNode: (data) => {
    if (!data) return new Error("Input data is not an object.");
    var out = parseNode(data.node);
    if (out instanceof Error) return out;
    data.node = out;
    return data;
  },

  parseNodeList: (data) => {
    if (!data || !Array.isArray(data.nodes)) {
      return new Error("Invalid node list format, should be: [{id: <id>, host: <host>, port: <port>}].");
    }
    var out = parseNodeListMemo(data.nodes, []);
    if (out instanceof Error) return out;
    data.nodes = out;
    return data;
  },

  mapToObject: (data) => {
    var obj = {};
    data.forEach((val, key) => {
      obj[key] = val;
    });
    return obj;
  },

  setToList: (data) => {
    var memo = [];
    data.forEach((val) => {
      memo.push(val);
    });
    return memo;
  },

  mapToList: (data) => {
    var memo = [];
    data.forEach((val, key) => {
      memo.push([key, val]);
    });
    return memo;
  },

  isPositiveNumber: (val) => {
    return util.isNumber(val) && val > 0;
  }
};

module.exports = utils;

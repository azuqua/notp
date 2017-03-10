var _ = require("lodash");

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
  }
};

module.exports = utils;

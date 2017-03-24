var fs = require("fs"),
    path = require("path"),
    _ = require("lodash");

var hidden = /^\./;
var files = fs.readdirSync(__dirname);
// filter out hidden files
files = _.filter(files, function (val) {
  return !hidden.test(val);
});
var lib = _.reduce(files, function(memo, file){
  memo[path.basename(file, ".js")] = require(path.join(__dirname, file));
  return memo;
}, {});

module.exports = lib;

var path = require("path"),
  _ = require("lodash");

module.exports = function (grunt) {
  var files = [
    "index.js",
    "lib/chash.js",
    "lib/cluster_node.js",
    "lib/conn.js",
    "lib/gen_server.js",
    "lib/gossip.js",
    "lib/kernel.js",
    "lib/node.js",
    "lib/vclock.js",
    "lib/dtable.js",
    "lib/dlm/dlm.js",
    "lib/dlm/lock.js",
    "lib/dsem/semaphore.js"
  ];

  grunt.initConfig({
    pkg: grunt.file.readJSON("package.json"),

    jsdoc: {
      dist: {
        src: files,
        options: {
          readme: "README.md",
          destination: "doc",
          configure: ".jsdoc.conf.json"
        }
      }
    },

    jsdoc2md: {
      separateOutputFilePerInput: {
        files: files.map((file) => {
          var path = file.split("/");
          var last = _.last(path);
          last = last.slice(0, last.length-3) + ".md";
          path[path.length-1] = last;
          return {src: file, dest: "doc/" + _.drop(path).join("/")};
        })
      }
    },

    mochaTest: {
      run: {
        options: {reporter: "spec", checkLeaks: true},
        src: ["test/**/*.js"]
      }
    }
  });

  grunt.loadNpmTasks("grunt-jsdoc");
  grunt.loadNpmTasks("grunt-jsdoc-to-markdown");
  // create documentation
  grunt.registerTask("docs", ["jsdoc"]);
  grunt.registerTask("md", ["jsdoc2md"]);
  
  grunt.loadNpmTasks("grunt-contrib-jshint");
  grunt.loadNpmTasks("grunt-mocha-test");

  grunt.registerTask("test", ["mochaTest:run"]);

  grunt.registerTask("default", ["test"]);
};

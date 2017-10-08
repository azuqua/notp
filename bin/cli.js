#!/usr/local/bin/node

const path = require("path"),
      os = require("os"),
      ipc = require("node-ipc"),
      NetKernel = require("../lib/kernel"),
      Queue = require("../lib/queue"),
      EventEmitter = require("events").EventEmitter,
      uuid = require("uuid"),
      crypto = require("crypto"),
      _ = require("lodash"),
      util = require("util");

const vorpal = require("vorpal")();
const argv = require("yargs")
  .usage("Usage: $0 [OPTIONS] [cmd [arg [arg ...]]]")
  .demand([])
  .help("help")
  .describe("I", "Unique instance identifier of the node being connected to.")
  .alias("I", "instance")
  .describe("H", "Server hostname of the node being connected to.")
  .alias("H", "hostname")
  .describe("p", "Server port of the node being connected to.")
  .alias("p", "port")
  .describe("a", "Distributed cookie to use for signing requests against the connecting node.")
  .alias("a", "key")
  .default({
    I: os.hostname(),
    H: os.hostname(),
    p: 7022,
    a: "",
  })
  .number(["port"])
  .string(["instance", "hostname", "key"])
  .check((args, opts) => {
    return util.isNumber(args.port) && !isNaN(args.port);
  })
  .argv;

var singular = argv._.length === 0;

function log(...args) {
  if (singular) console.log.apply(null, args);
}

const from = {id: argv.I + "_" + process.pid};
let client;

class Client extends EventEmitter {
  constructor(ipc, id, host, port, cookie) {
    super();
    this._ipc = ipc;
    this._id = id;
    this._host = host;
    this._port = port;
    this._cookie = cookie;
    this._rcv = new Queue();
    this._connected = true;
    this._disLog = false;
  }

  start() {
    this._ipc.of[this._id].on("message", (data) => {
      if (data && data.tag === this._rcv.peek().tag) {
        this._rcv.dequeue().cb(data);
      }
    });
    this._ipc.of[this._id].on("connect", _.partial(this._handleConnect).bind(this));
    this._ipc.of[this._id].on("disconnect", _.partial(this._handleDisconnect).bind(this));
  }

  stop() {
    this._ipc.disconnect(this._id);
    return this;
  }

  send(comm, message, cb) {
    const data = Buffer.from(JSON.stringify({event: comm, data: message}));
    const msg = {
      id: "command",
      tag: uuid.v4(),
      from: from,
      stream: {stream: uuid.v4(), done: true},
      data: data
    };
    if (this._connected) {
      ipc.of[argv.I].emit("message", NetKernel._encodeMsg(this._cookie, msg));
      this._rcv.enqueue({
        tag: msg.tag,
        cb: (data) => {
          data = _.omit(NetKernel._decodeMsg(this._cookie, data), "tag");
          console.log(JSON.stringify(data, null, 2));
          cb();
        }
      });
      return this;
    }
    cb(new Error("Disconnected from server."));
    return this;
  }

  _handleConnect() {
    if (singular) {
      log("Connected to %s", this._id);
    }
    this._connected = true;
    this._disLog = false;
    this.emit("connect");
    return this;
  }

  _handleDisconnect() {
    if (!this._disLog) {
      this._disLog = true;
      log("Disconnected from %s", this._id);
    }
    this._connected = false;
    this._rcv.flush().forEach((el) => {
      el.cb(new Error("Disconnected from server."));
    });
    this.emit("disconnect");
    return this;
  }
}

let forceStr = "Execute this command before any existing " +
      "message streams on this node's gossip processor. Defaults to " +
      "false.";

function parseNodeList(nodeList) {
  return _.chunk(nodeList, 3).reduce((memo, node) => {
    if (node.length !== 3) return memo;
    memo.push({
      id: node[0],
      host: node[1],
      port: parseInt(node[2])
    });
    return memo;
  }, []);
}

vorpal
  .command("inspect")
  .description("Prints the ring of this node to the console.")
  .action(function (args, cb) {
    client.send("inspect", null, cb);
  });

vorpal
  .command("nodes")
  .description("Prints the nodes of the ring of this node to the console.")
  .action(function (args, cb) {
    client.send("nodes", null, cb);
  });

vorpal
  .command("get <id>")
  .description("Returns information about a node's hostname and port in this node's cluster.")
  .types({
    string: ["id"]
  })
  .action(function (args, cb) {
    client.send("get", {id: args.id}, cb);
  });

vorpal
  .command("has <id>")
  .description("Returns whether the targeted node exists in this node's cluster.")
  .types({
    string: ["id"]
  })
  .action(function (args, cb) {
    client.send("has", {id: args.id}, cb);
  });

vorpal
  .command("join <id>")
  .description("Joins a new cluster if not already present or not a member of any cluster.")
  .types({
    string: ["id"]
  })
  .action(function (args, cb) {
    client.send("join", {id: args.id}, cb);
  });

vorpal
  .command("meet <id> <host> <port>")
  .description("Meets a node in this node's cluster. This is the only way to do transitive additions to the cluster.")
  .types({
    string: ["id", "host", "port"],
  })
  .action(function (args, cb) {
    client.send("meet", {
      node: {id: args.id, host: args.host, port: parseInt(args.port)}
    }, cb);
  });

vorpal
  .command("weight <id>")
  .types({
    string: ["id"]
  })
  .action(function (args, cb) {
    client.send("weight", {
      id: args.id
    }, cb);
  });

vorpal
  .command("weights")
  .action(function (args, cb) {
    client.send("weights", null, cb);
  });

vorpal
  .command("leave")
  .description("Leaves the cluster this node is a part of. " + 
      "If force is passed, the gossip processor on this node " + 
      "won't wait for current message streams to be processed " +
      "before executing this command.")
  .option("-f, --force", forceStr, [false])
  .types({
    boolean: ["force"]
  })
  .action(function (args, cb) {
    client.send("leave", {force: args.options.force}, cb);
  });

vorpal
  .command("insert <id> <host> <port>")
  .description("Inserts a node into this node's cluster. " +
      "If force is passed, the gossip processor on this node " +
      "won't wait for current message streams to be processed " +
      "before executing this command.")
  .option("-f, --force", forceStr)
  .option("-w, --weight <weight>", "Number of virtual nodes to assign to the node being inserted. Defaults to the `rfactor` of the session node.")
  .types({
    string: ["id", "host", "port", "weight"]
  })
  .action(function (args, cb) {
    client.send("insert", {
      force: args.options.force,
      weight: parseInt(args.options.weight),
      node: {id: args.id, host: args.host, port: parseInt(args.port)}
    }, cb);
  });

vorpal
  .command("minsert [nodes...]")
  .description("Inserts multiple nodes into this node's cluster. " +
      "If force is passed, the gossip processor on this node " +
      "won't wait for current message streams to be processed " +
      "before executing this command.")
  .option("-f, --force", forceStr)
  .option("-w, --weight <w>", "Number of virtual nodes to assign to the nodes being inserted. Defaults to the `rfactor` of the session node.")
  .action(function (args, cb) {
    const nodes = parseNodeList(args.nodes);
    client.send("minsert", {
      force: args.options.force,
      weight: parseInt(args.options.weight),
      nodes: nodes
    }, cb);
  });

vorpal
  .command("update <id> <host> <port> [weight]")
  .description("Inserts a node into this node's cluster. " +
      "If force is passed, the gossip processor on this node " +
      "won't wait for current message streams to be processed " +
      "before executing this command.")
  .option("-f, --force", forceStr)
  .types({
    string: ["id", "host", "port", "weight"]
  })
  .action(function (args, cb) {
    client.send("update", {
      force: args.options.force,
      weight: parseInt(args.weight),
      node: {id: args.id, host: args.host, port: parseInt(args.port)}
    }, cb);
  });

vorpal
  .command("remove <id> <host> <port>")
  .description("Removes a node from this node's cluster. " +
      "If force is passed, the gossip processor on this node " +
      "won't wait for current message streams to be processed " +
      "before executing this command.")
  .option("-f, --force", forceStr)
  .types({
    string: ["id", "host", "port"]
  })
  .action(function (args, cb) {
    client.send("remove", {
      force: args.options.force,
      node: {id: args.id, host: args.host, port: parseInt(args.port)}
    }, cb);
  });

vorpal
  .command("mremove [nodes...]")
  .description("Removes multiple nodes from this node's cluster. " +
      "If force is passed, the gossip processor on this node " +
      "won't wait for current message streams to be processed " +
      "before executing this command.")
  .option("-f, --force", forceStr)
  .action(function (args, cb) {
    const nodes = parseNodeList(args.nodes);
    client.send("mremove", {
      force: args.options.force,
      nodes: nodes
    }, cb);
  });

vorpal
  .command("ping")
  .description("Ping a node in the cluster.")
  .action(function (args, cb) {
    client.send("ping", null, cb);
  });

if (singular) {
  log("Connecting to IPC server on node: %s, host: %s, port: %s", argv.I, argv.H, argv.p);
}
ipc.config.silent = true;
ipc.config.sync = true;
ipc.connectToNet(argv.I, argv.H, argv.p, () => {
  client = new Client(ipc, argv.I, argv.H, argv.p, argv.a);
  client.start();
  client.once("connect", () => {
    if (singular) {
      vorpal
        .delimiter("> ")
        .show();
    } else {
      vorpal.exec(argv._.join(" "), function (err, res) {
        client.stop();
        if (err) {
          process.exit(1);
        } else {
          process.exit(0);
        }
      });
    }
  });
});

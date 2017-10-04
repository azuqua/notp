Clusterluck
===========

[![Build Status](https://travis-ci.org/azuqua/clusterluck.svg?branch=master)](https://travis-ci.org/azuqua/clusterluck)
[![Code Coverage](https://coveralls.io/repos/github/azuqua/clusterluck/badge.svg?branch=master)](https://coveralls.io/github/azuqua/clusterluck?branch=master)

[Documentation](https://azuqua.github.io/clusterluck)

A library for writing distributed systems that use a gossip protocol to communicate state management, consistent hash rings for sharding, and vector clocks for history.

## Install

```
$ npm install clusterluck
```

## Dependencies

This module uses a native module called `node-microtime` for microsecond insert/update granularity for vector clocks, and by extension requires a C++-11 compatible compiler. The `.travis.yml` file lists g++-4.8 as an addon in response, but other compatible versions of g++ or clang should suffice. The following g++ compiler versions have been tested:
  - 6.*.*
  - 4.8

## Test

To run tests, you can use:
```
$ grunt test
```

or just:
```
$ mocha test
```

For code coverage information, you can use `istanbul` and run:
```
$ istanbul cover _mocha test
```

If `istanbul` isn't installed, just run:
```
$ npm install --global istanbul
```

## Index

- [Usage](#Usage)
  - [Creating Clusters](#CreatingClusters)
  - [Manipulating Clusters](#ManipulatingClusters)
  - [Example Cluster](#ExampleCluster)
  - [Writing GenServers](#WritingGenServers)
  - [Using the CLI](#UsingTheCLI)
    - [`inspect`](#inspect)
    - [`get`](#get)
    - [`has`](#has)
    - [`join`](#join)
    - [`meet`](#meet)
    - [`leave`](#leave)
    - [`insert`](#insert)
    - [`minsert`](#minsert)
    - [`remove`](#remove)
    - [`mremove`](#mremove)
  - [Consistent Hash Ring](#ConsistentHashRing)
  - [Vector Clocks](#VectorClocks)
- [TODO](#TODO)

### <a name="Usage"></a>Usage

Clusterluck can be used as a module to write decentralized distributed systems with full-mesh IPC network topologies, or in isolation to use the consistent hash ring and vector clock data structures.

#### <a name="CreatingClusters"></a>Creatings Clusters

To get started, we can use the following code:
```javascript
const cl = require("clusterluck");

let node = cl.createCluster("foo", "localhost", 7022);
node.start("cookie", "ring", () => {
  console.log("Listening on port 7022!");
});
```

This will create a single node in cluster `ring` with name `foo`, using cookie `cookie` to sign messages within the cluster.
If another program tries to communicate with this node, but doesn't sign requests with this cookie, the message will be ignored with an `INVALID_CHECKSUM` error emitted in the debug logs.
When nodes are added to `ring`, each node will attempt to form TCP-based IPC connections to any new node added into the cluster.
Similarly, each IPC server will generate new IPC-client connections. Each external connection will queue up messages if the socket goes down, sending all queued up messages once the socket reconnects.
Once nodes are removed, both ends of the connection are closed forcibly.

In the background, several listeners attached to the network kernel for this node will be created, including a gossip ring listener and a command line server listener.
In short, the command line server listener exists to handle requests made by the CLI tool under `bin/cli.js`, while the gossip ring listens for ring maniuplations on the cluster. On additions to the cluster, new IPC connections to external nodes will be created, and vice versa for removals from the cluster.

#### <a name="ManipulatingClusters"></a>Manipulating Clusters

To manipulate the cluster, we interact with the gossip property of the cluster node. For example:

``` javascript
// meeting another node
let gossip = node.gossip();
let kernel = node.kernel();
let nNode = new Node("bar", "localhost", 7023);
gossip.meet(nNode);
// wait some time, eventually the node will show up in this node's ring...
assert.ok(gossip.ring().has(nNode));

// inserting nodes
gossip.insert(nNode);
assert.ok(gossip.ring().has(nNode));
// after some time, node "bar" should have "foo" in its ring...

// removing nodes
gossip.remove(nNode);
assert.notOk(gossip.ring().has(nNode));
// after some time, ndoe "bar" should remove "foo" from its ring...

// leaving a cluster
gossip.leave();
gossip.once("leave", () => {
  assert.lengthOf(gossip.ring().nodes(), 1);
  assert.ok(gossip.ring.nodes()[0].equals(kernel.self()));
});

// joining a cluster
gossip.join("another_ring_id");
// after some time, this node will receive messages from the existing nodes in the cluster
// (if any exist)
```

For documentation on available methods/inputs for cluster manipulation, visit the documentation for the `GossipRing` class.

#### <a name="ExampleCluster"></a>Example Cluster

In an example.js file, insert the following:

```javascript
const cl = require("clusterluck"),
      os = require("os");

let id = process.argv[2],
    port = parseInt(process.argv[3]);

let node = cl.createCluster(id, os.hostname(), port);
node.start("cookie", "ring", () => {
  console.log("Listening on port %s!", port);
});
```

Then, in one terminal, run:
```
$ node example.js foo 7022
```

And in another terminal, run:
```
$ node example.js bar 7023
```

Now, if we spin up the CLI and connect to `foo`, we can then run:
```
// whatever os.hostname() resolves to, replace localhost with that
$ meet bar localhost 7023
```

If we then go to inspect the ring on each node, we should see both node `foo` and node `bar` in the ring.

#### <a name="WritingGenServers"></a>Writing GenServers

The `GenServer` class is used to create actors that send messages around and receive messages from the rest of the cluster.
They're the basic unit of logic handling in clusterluck, and heavily derived off of Erlang's gen_server's, but incorporated into node.js' EventEmitter model.
To start a `GenServer` with no event handling, we can use the following code:

```javascript
let serve = cl.createGenServer(cluster);
serve.start("name_to_listen_for");
```

This will tell the network kernel `kernel` that any messages with id `name_to_listen_for` received on this node should be routed to `serve` for processing.
Names for `GenServer`s on the same node have a uniqueness property, so trying to declare multiple instances listening on the same name will raise an error.

To add some event handling to our `GenServer`, we can modify the above code as such:

```javascript
let serve = cl.createGenServer(cluster);
serve.on("hello", (data, from) => {
  serve.reply(from, "world");
});
serve.start("name_to_listen_for");
```

With this additional logic, any "hello" event sent to this node with id `name_to_listen_for` will be responded to with "world".
This includes messages sent from the local node, as well from other nodes in the cluster.

Once we've declared our `GenServer` and added event handling logic, we can start sending and receiving messages to/from other nodes in the cluster.

```javascript
// serve is a GenServer instance, kernel is serve's network kernel
// synchronous requests
// this makes a call to a GenServer listening on "server_name" locally
serve.call("server_name", "event", "data", (err, out) => {...});
// this makes the same call
serve.call({id: "server_name", node: kernel.self()}, "event", "data", (err, out) => {...});
// this makes the same call but to another node
serve.call({id: "server_name", node: another_node}, "event", "data", (err, out) => {...});

// asynchronous requests
// this makes an async call to a GenServer listening on "server_name" locally
serve.cast("server_name", "event", "data");
// this makes the same call
serve.cast({id: "server_name", node: kernel.self()}, "event", "data");
// this makes the same call but to another node
serve.cast({id: "server_name", node: another_node}, "event", "data");
```

Here, we see the true power of `GenServer`s as a unified interface for distributed communication with a local node as well as external nodes!


As an implementation note, `GenServer`s should not be used as a replacement for EventEmitters when orchestrating state local to a node.
Generally speaking, there is little overhead in using this over a raw EventEmitter, but there are conditional branches and extra V8 constructions
that may be unneeded for your implementation.
Instead, making other `GenServer`s part of the constructor of other `GenServer`s is preferred (using OOP principles to enforce actor relations), similar to how the `CommandServer` class works.
In fact, both the `GossipRing` and `CommandServer` classes, built into every node in the cluster, are `GenServer`s themselves!

#### <a name="UsingTheCLI"></a>Using the CLI

In the working directory of this module, we see the `bin/cli.js` script. This node script communicates with a single node in a cluster to manipulate the ring.
The following options specify which node to communicate with and how:
  - `-I, --instance`: The unique instance identifier of the node being connected to.
  - `-H, --hostname`: Server hostname of the node being connected to.
  - `-p, --port`: Server port of the node being connected to.
  - `-a, --key`: Distributed cookie to use for signing requests to the connecting node.

Once run, a CLI session is created that provides the following commands. For any given command, help documentation can be printed to the console by typing `help <command_name>`.

##### inspect

In the CLI session, type `inspect`. This command will print the cluster at a node on the console. For example, if we've just started a new node with id `foo` at hostname `localhost` with port `7022`, we'd see the following output:

```
> inspect
{ ok: true,
  data: 
   { rfactor: 3,
     pfactor: 2,
     tree: 
      [ { key: 'avmox6bKHfmLdzmObwjwIrh2WC6XM471ods56FWbDo0=',
          value: { id: 'foo', host: 'localhost', port: 7022 } },
        { key: 'kL2YfHLEuxHGaEz4nOxWYyPSiFlGBsFMzoYDXXxuXK0=',
          value: { id: 'foo', host: 'localhost', port: 7022 } },
        { key: 'kzMt7C+SJZbxNQmrL3vhpfJ+a0RgPiGlRhrxwS57RWI=',
          value: { id: 'foo', host: 'localhost', port: 7022 } } ] } }
```

##### get

This command will print metadata about an input node in the cluster, or will return an error if the node doesn't exist in the cluster (according to the node our session targets). For example, given the previous setup:

```
> get foo
{ ok: true,
  data: { id: 'foo', host: 'localhost', port: 7022 } }

> get bar
{ ok: false,
  error: 
   { message: '\'bar\' is not defined in this ring.',
     _error: true } }
```

##### has

This command will print whether an input node exists in the cluster (according to the node our session targets). For example, given the previous setup:

```
> has foo
{ ok: true,
  data: true }

> has bar
{ ok: true,
  data: false }
```

##### join

This command will attempt to join a cluster if it doesn't already belong to a cluster. For example, given the previous setup:

```
// assuming 'foo' isn't a part of a ring
> join ring
{ ok: true }

// if it's in a ring
> join ring
{ ok: false,
  error: 
   { message: 'Node already belongs to ring \'ring\'',
     _error: true } }
```

##### meet

This command will tell the targeted node by this session to meet another node in the cluster. Currently, this is the only way to make ring insertions transitive.
Subsequently, if as a result two nodes are meeting for the first time, a ring merge will occur.
The resulting state will be gossiped around the cluster, eventually resulting in every node thinking the input node belongs in the cluster.
For example, given the previous setup:

```
> meet bar localhost 7023
{ ok: true }

// wait some time...
> get bar
// metadata about node bar... 
```

##### leave

This command will tell the targeted node by this session to leave its current cluster (if it belongs to one). For example:

```
> leave
{ ok: true }
// immediately following this command...
> has bar
{ ok: true,
  data: false }

// leave is done forcefully
> leave --force
{ ok: true }
```

For documentation on how the `--force` option works for this command, just run `help leave`.

##### insert

This command will tell the targeted node by this session to insert a node into its cluster (as it currently views it).
Subsequently, this information will be gossiped around the cluster, eventually resulting in every node thinking the input node belongs in the cluster.
This differs from `meet` in that insertions are not transitive between nodes; it's a new event on the ring state, and therefore overriding when state conflicts occur between nodes sharing ring history.
For example:

```
> insert bar localhost 7023
{ ok: true }
> get bar
{ ok: true,
  data: { id: 'bar', host: 'localhost', port: 7023 } }

// insert is done forcefully
> insert --force bar localhost 7023
{ ok: true }
> get bar
{ ok: true,
  data: { id: 'bar', host: 'localhost', port: 7023 } }
```

For documentation on how the `--force` option works for this command, or any other option, just run `help insert`.

##### minsert

This command will tell the targeted node by this session to insert multiple nodes into its cluster (as it currently views it).
Similar to how `insert` works, only it allows batch insertion.
For example:

```
> minsert bar localhost 7023 baz localhost 7024
{ ok: true }
> get bar
{ ok: true,
  data: { id: 'bar', host: 'localhost', port: 7023 } }
> get baz
{ ok: true,
  data: { id: 'baz', host: 'localhost', port: 7024 } }

// minsert is done forcefully
> minsert --force bar localhost 7023 baz localhost 7024
{ ok: true }
> get bar
{ ok: true,
  data: { id: 'bar', host: 'localhost', port: 7023 } }
> get baz
{ ok: true,
  data: { id: 'baz', host: 'localhost', port: 7024 } }
```

For documentation on how the `--force` option works for this command, or any other option, just run `help minsert`.

##### remove

This command will tell the targeted node by this session to remove a node from its cluster (as it currently views it).
Subsequently, this information will be gossiped around the cluster, eventually resulting in every node thinking the input node no longer belongs in the cluster.
For example:

```
> remove bar localhost 7023
{ ok: true }
> has bar
{ ok: true,
  data: false }

// remove is done forcefully
> remove --force bar localhost 7023
{ ok: true }
> has bar
{ ok: true,
  data: false }
```

For documentation on how the `--force` option works for this command, or any other option, just run `help remove`.

##### mremove

This command will tell the targeted node by this session to remove multiple nodes from its cluster (as it currently views it).
Similar to how `remove` works, only it allows batch removal.
For example:

```
> mremove bar localhost 7023 baz localhost 7024
{ ok: true }
> has bar
{ ok: true,
  data: false }
> has baz
{ ok: true,
  data: false }

// mremove is done forcefully
> mremove --force bar localhost 7023 baz localhost 7024
{ ok: true }
> has bar
{ ok: true,
  data: false }
> has baz
{ ok: true,
  data: false }
```

For documentation on how the `--force` option works for this command, or any other option, just run `help mremove`.

#### <a name="ConsistentHashRing"></a>Consistent Hash Ring

Some helpful resources for learning about consistent hash rings:
  
  - [Wikipedia Entry](https://en.wikipedia.org/wiki/Consistent_hashing)
  - [libketama](https://github.com/RJ/ketama), which has a corresponding blog post, and an alternative way of implementing a consistent hash ring.

From here, you can reference the documentation found on the github pages for the CHash class.

#### <a name="VectorClocks"></a>Vector Clocks

Some helpful resources for learning about vector clocks:
	
  - [Wikipedia Entry](https://en.wikipedia.org/wiki/Vector_clock)
  - [Basho's Why Vector Clocks Are Easy](http://basho.com/posts/technical/why-vector-clocks-are-easy/)
  - [Basho's Why Vector Clocks Are Hard](http://basho.com/posts/technical/why-vector-clocks-are-hard/)
  - [Riak Documentation](http://docs.basho.com/riak/kv/2.2.1/learn/concepts/causal-context/)

From here, you can reference the documentation found on the github pages for the VectorClock class.

### <a name="TODO"></a>TODO

In addition to what currently exists in this library, here's a list of features to possibly add:
  - Provide listener for permanent close on connection between two nodes (`maxRetries` option on kernel creation).
  - Add a GenStream class similar to GenServer, but strictly uses streams for communication instead of JS natives (will also require a protocol definition for indicating stream start, etc).
  - Discuss making disconnects between nodes on a node departure forceful or not (it's forceful right now).

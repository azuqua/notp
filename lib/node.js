var _ = require("lodash"),
    debug = require("debug")("clusterluck:lib:node");

class Node {
  /**
   *
   * Node abstraction class.
   *
   * @class Node Node
   * @memberof NetKernel
   *
   * @param {String} id - ID of this node.
   * @param {String} host - Hostname of this node.
   * @param {Number} port - Port number of this node.
   *
   */
  constructor(id, host, port) {
    this._id = id;
    this._host = host;
    this._port = port;
  }

  /**
   *
   * Acts as a getter/setter for the ID of this node.
   *
   * @method id
   * @memberof NetKernel.Node
   * @instance
   *
   * @param {String} [id] - ID to set.
   *
   * @return {String} ID of this instance.
   *
   */
  id(id) {
    if (typeof id === "string") {
      this._id = id;
    }
    return this._id;
  }

  /**
   *
   * Acts as a getter/setter for the hostname of this node.
   *
   * @method host
   * @memberof NetKernel.Node
   * @instance
   *
   * @param {String} [host] - Hostname to set.
   *
   * @return {String} Hostname of this instance.
   *
   */
  host(host) {
    if (typeof host === "string") {
      this._host = host;
    }
    return this._host;
  }

  /**
   *
   * Acts as a getter/setter for the port of this node.
   *
   * @method port
   * @memberof NetKernel.Node
   * @instance
   *
   * @param {Number} [port] - Port to set.
   *
   * @return {Number} Port of this instance.
   *
   */
  port(port) {
    if (typeof port === "number") {
      this._port = port;
    }
    return this._port;
  }

  /**
   *
   * Returns whether this instance equals another node. Existence means the id, host, and port match on both instances.
   *
   * @method equals
   * @memberof NetKernel.Node
   * @instance
   *
   * @param {Node} node - Node to check equality against.
   *
   * @return {Boolean} Whether this instance equals `node`.
   *
   */
  equals(node) {
    return this._id === node.id() &&
      this._host === node.host() &&
      this._port === node.port();
  }

  /**
   *
   * Computes the JSON serialization of this instance.
   *
   * @method toJSON
   * @memberof NetKernel.Node
   * @instance
   *
   * @param {Boolean} [fast] - Whether to create a copy of internal state when exporting to JSON or not.
   *
   * @return {Object} JSON serialization of this instance.
   *
   */
  toJSON(fast) {
    var out = {
      id: this._id,
      host: this._host,
      port: this._port
    };
    return fast === true ? out : _.clone(out);
  }

  /**
   *
   * Create an instance of the Node class from JSON data.
   *
   * @method from
   * @memberof NetKernel.Node
   * @static
   *
   * @param {Object} data - Data to initialize node state from.
   * @param {String} data.id - ID of node.
   * @param {String} data.host - Hostname of node.
   * @param {Number} data.port - Port of node.
   *
   * @return {Node} New instance of Node class.
   *
   */
  static from(data) {
    return new Node(data.id, data.host, data.port);
  }
}

module.exports = Node;

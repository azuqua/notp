class Lock {
  /**
   *
   * Lock class to hold the type, id, and timeout information for a lock. Additional metadata about the holder(s) of a lock, such as start time and initial TTL, are stored in the DLM server's underlying table.
   *
   * @class Lock
   * @memberof Clusterluck
   *
   * @param {String} type - Type of lock. Can either be 'read' be 'write'.
   * @param {String} id - ID of lock.
   * @param {Timeout|Map} timeout - Timeout object for the lock. If a read lock, this value will be a map from holders to timeout objects.
   *
   */
  constructor(type, id, timeout) {
    this._type = type;
    this._id = id;
    this._timeout = timeout;
  }

  /**
   *
   * Acts as a getter/setter for the type of this lock.
   *
   * @method type
   * @memberof Clusterluck.Lock
   * @instance
   *
   * @param {String} [type] - Type to set on this lock.
   *
   * @return {String} This lock's type.
   *
   */
  type(type) {
    if (type !== undefined) {
      this._type = type;
    }
    return this._type;
  }

  /**
   *
   * Acts as a getter/setter for the ID of this lock.
   *
   * @method id
   * @memberof Clusterluck.Lock
   * @instance
   *
   * @param {String} [id] - ID to set on this lock.
   *
   * @return {String} This lock's ID.
   *
   */
  id(id) {
    if (id !== undefined) {
      this._id = id;
    }
    return this._id;
  }

  /**
   *
   * Acts as a getter/setter for the timeout of this lock.
   *
   * @method timeout
   * @memberof Clusterluck.Lock
   * @instance
   *
   * @param {Timeout|Map} [timeout] - Timeout to set on this lock.
   *
   * @return {Timeout|Map} This lock's timeout object(s).
   *
   */
  timeout(timeout) {
    if (timeout !== undefined) {
      this._timeout = timeout;
    }
    return this._timeout;
  }
}

module.exports = Lock;

class Semaphore {
  /**
   *
   * Semaphore class to hold the id, concurrency limit, and timeouts information for a semaphore. Additional metadata about the holders of a semaphore, such as start time and initial TTL, are stored in the DSem server's underlying table.
   *
   * @class Semaphore
   * @memberof Clusterluck
   * 
   * @param {String} id - ID of semaphore.
   * @param {Number} size - Concurrency limit of semaphore.
   * @param {Map} timeouts - Map from holders to timeout objects.
   *
   */
  constructor(id, size, timeouts) {
    this._id = id;
    this._size = size;
    this._timeouts = timeouts;
  }

  /**
   *
   * Acts as a getter/setter for the ID of this semaphore.
   *
   * @method id
   * @memberof Clusterluck.Semaphore
   * @instance
   *
   * @param {String} [id] - ID to set on this semaphore.
   *
   * @return {String} This semaphore's ID.
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
   * Acts as a getter/setter for the concurrency limit of this semaphore.
   *
   * @method size
   * @memberof Clusterluck.Semaphore
   * @instance
   *
   * @param {Number} [size] - Concurrency limit to set on this semaphore.
   *
   * @return {Number} This semaphore's concurrency limit.
   *
   */
  size(size) {
    if (size !== undefined) {
      this._size = size;
    }
    return this._size;
  }

  /**
   *
   * Acts as a getter/setter for the timeout map of this semaphore.
   *
   * @method timeouts
   * @memberof Clusterluck.Semaphore
   * @instance
   *
   * @param {Map} [timeouts] - Timeout map to set on this semaphore.
   *
   * @return {Map} Timeout map of this semaphore.
   *
   */
  timeouts(timeouts) {
    if (timeouts !== undefined) {
      this._timeouts = timeouts;
    }
    return this._timeouts;
  }
}

module.exports = Semaphore;

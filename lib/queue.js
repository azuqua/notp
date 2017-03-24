class Queue {
  constructor() {
    this._front = [];
    this._back = [];
  }

  enqueue(val) {
    this._back.push(val);
    return this;
  }

  dequeue() {
    if (this._front.length > 0) {
      return this._front.pop();
    }
    this._front = this._back.reverse();
    this._back = [];
    return this._front.pop();
  }

  flush() {
    var ret = this._front.reverse().concat(this._back);
    this._front = [];
    this._back = [];
    return ret;
  }

  size() {
    return this._front.length + this._back.length;
  }

  peek() {
    if (this._front.length > 0) return this._front[this._front.length-1];
    return this._back[0];
  }
}

module.exports = Queue;

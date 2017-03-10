var _ = require("lodash");

class TableTerm {
  constructor(value, state, clock, actor) {
    this._value = value;
    this._state = state;
    this._vclock = clock;
    this._actor = actor;
  }

  value(value) {
    if (value !== undefined) this._value = value;
    return this._value;
  }

  state(state) {
    if (state !== undefined) this._state = state;
    return this._state;
  }

  vclock(vclock) {
    if (vclock !== undefined) this._vclock = vclock;
    return this._vclock;
  }

  actor(actor) {
    if (actor !== undefined) this._actor = actor;
    return this._actor;
  }
}

module.exports = TableTerm;

var _ = require("lodash"),
    assert = require("assert");

var VectorClock = require("./vclock"),
    utils = require("./utils");

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

  toJSON(fast) {
    var out = {
      value: this._value,
      state: this._state,
      vclock: this._vclock.toJSON(true),
      actor: this._actor
    };
    if (fast === true) out = _.cloneDeep(out);
    return out;
  }

  fromJSON(data) {
    assert.ok(TableTerm.validJSON(data), "Input data contained invalid JSON.");
    this._value = data.value;
    this._state = data.state;
    this._vclock = (new VectorClock()).fromJSON(data.vclock);
    this._actor = data.actor;
    return this;
  }

  static validJSON(ent) {
    return utils.isPlainObject(ent) &&
      ent.value !== undefined &&
      typeof ent.state === "string" &&
      typeof ent.actor === "string" &&
      VectorClock.validJSON(ent.vclock);
  }
}

module.exports = TableTerm;

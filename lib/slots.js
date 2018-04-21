
class Slots {
  constructor() {
    this.lookup = {};
  }

  addSlots(min, max, list) {
    for (let i = min; i < max; i += 1) {
      this.lookup[i] = list;
    }
  }

  getList(slot) {
    return this.lookup[slot];
  }
}

module.exports = Slots;

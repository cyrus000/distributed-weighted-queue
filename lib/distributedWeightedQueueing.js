const Events = require('events');
const DataList = require('./dataList');
const Slots = require('./slots');

const DefaultBaseKey = 'weightLists';
const DefaultMaxSize = 1000;

class DistributedWeightedQueueing extends Events {
  /**
   *
   * @param {object} config
   * @param {object} config.redis
   * @param {string} config.redis.host
   * @param {number} config.redis.port
   * @param {string} [config.baseKey]
   * @param {number} [config.maxSize]
   * @param {Array} config.weights //weights for each shard
   */
  constructor(config) {
    super();

    this.totalWeight = 0;
    this.generateWeightsShards(config.weights);
    this.shardsLength = config.weights.length;
    this.shards = [];
    this.config = config;
    this.config.baseKey = config.baseKey || DefaultBaseKey;
    this.config.maxSize = config.maxSize || DefaultMaxSize;
    this.maxShardSize = Math.floor(config.maxSize / this.shardsLength);
    this.lookup = new Slots();

    this.setupLists(config.redis);
  }

  /**
   * get total size of queue
   * @returns {number}
   */
  getSize() {
    let size = 0;

    this.shards.forEach((shard) => {
      size += shard.getLastSize();
    });

    return size;
  }

  /**
   * get random slot
   *
   * @returns {number}
   */
  getRandomSlot() {
    return Math.floor(Math.random() * this.totalWeight);
  }

  /**
   *
   * @param {number} shard
   * @param {*} data
   * @param {function} cb
     */
  put(shard, data, cb) {
    this.shards[shard].put(data, cb);
  }

  /**
   * get the last item added from one of the queues
   *
   * @param {function} cb
   */
  get(cb) {
    const slot = this.getRandomSlot();
    const list = this.lookup.getList(slot);

    if (list.getLastSize() === 0) {
      if (this.getSize() === 0) {
        cb(null, null);
        return;
      }

      this.get(cb);
      return;
    }

    list.get((err, payload) => {
      if (err) {
        cb(err);
        return;
      }

      if (payload === null && this.getSize() !== 0) {
        list.getSize((sizeErr) => {
          if (sizeErr) {
            cb(sizeErr);
            return;
          }

          this.get(cb);
        });
        return;
      }

      cb(err, payload);
    });
  }

  /**
   *
   * @param {number} shard
   * @returns {{listName: string, sizeChannel: string, maxListSize: (number|*)}}
   */
  generateListConfig(shard) {
    return {
      listName: `${this.config.baseKey}:l:listName::${shard}`,
      sizeChannel: `${this.config.baseKey}:channelSize:${shard}`,
      maxListSize: this.maxShardSize,
    };
  }

  /**
   * setup
   * @param {object} redisConfig
   */
  setupLists(redisConfig) {
    let location = 0;

    for (let i = 0; i < this.shardsLength; i += 1) {
      const lastSlot = this.normalizedWeights[i] + location;
      const dataList = new DataList(redisConfig, this.generateListConfig(i));
      dataList.on('size_change', () => {
        this.emit('size_change', this.getSize());
      });
      this.shards.push(dataList);
      this.lookup.addSlots(location, lastSlot, dataList);
      location = this.normalizedWeights[i] + location;
    }
  }

  /**
   * setup
   * @param {number} shards
   */
  generateWeightsShards(shards) {
    let max = shards[0];
    let total = 0;
    const normalizedShards = [];

    shards.forEach((shard) => {
      if (shard > max) {
        max = shard;
      }
    });

    shards.forEach((shard) => {
      const weight = Math.floor((shard / max) * 100);
      total += weight;
      normalizedShards.push(weight);
    });

    this.normalizedWeights = normalizedShards;
    this.totalWeight = total;
  }
}

module.exports = DistributedWeightedQueueing;

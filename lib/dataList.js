const Redis = require('redis');
const Events = require('events');

class DataList extends Events {
  /**
   * Data object for keeping a list
   *
   * @param {object} redisConfig
   * @param {object} listConfig
   * @param {string} listConfig.listName
   * @param {string} listConfig.sizeChannel
   * @param {number} listConfig.maxListSize
   * @constructor
   */
  constructor(redisConfig, listConfig) {
    super();
    this.redis = Redis.createClient(redisConfig);
    this.publisher = this.redis.duplicate();
    this.subscriber = this.redis.duplicate();
    this.log(`connecting to redis config=${JSON.stringify(redisConfig)}`);
    this.config = listConfig;
    this.size = null;

    this.subscriber.on('message', (channel, message) => {
      if (channel !== this.config.sizeChannel) {
        return;
      }

      const size = parseInt(message, 10);

      if (this.size !== size) {
        this.emit('size_change', size);
      }

      this.size = size;
    });

    this.getSize();

    this.subscriber.subscribe(this.config.sizeChannel);
  }

  /**
   * emits log messages
   *
   * @param {object} msg
   */
  log(msg) {
    this.emit('log', msg);
  }

  /**
   * Put data into a a redis list
   * This will maintain a max size of a list
   * Emit the new size of the list after put
   *
   * @param {object} data
   * @param {function} cb
   */
  put(data, cb) {
    let payload = null;

    try {
      payload = JSON.stringify(data);
    } catch (err) {
      cb(err);
      return;
    }

    const multi = this.redis.multi();
    multi.lpush(this.config.listName, payload);
    multi.ltrim(this.config.listName, 0, this.config.maxListSize);
    multi.llen(this.config.listName);
    multi.exec((err, results) => {
      if (err) {
        cb(err);
        return;
      }

      const size = results[2];
      this.emitSize(size, () => {
        cb(null, payload);
      });
    });
  }

  /**
   * get the last element of a list and removes it
   * emit new size of the list after get
   *
   * @param {function} cb
   */
  get(cb) {
    const multi = this.redis.multi();
    multi.rpop(this.config.listName);
    multi.llen(this.config.listName);
    multi.exec((err, data) => {
      if (err) {
        cb(err);
        return;
      }

      let payload = null;

      try {
        payload = JSON.parse(data[0]);
      } catch (e) {
        cb(e);
        return;
      }

      const size = data[1];
      this.emitSize(size, () => {
        cb(null, payload);
      });
    });
  }

  /**
   * emits queue size
   *
   * @param {number} size
   */
  emitSize(size, cb) {
    this.publisher.publish(this.config.sizeChannel, size, (err) => {
      this.emit('size', parseInt(size, 10));
      cb(err);
    });
  }

  getLastSize() {
    return this.size;
  }

  /**
   * get the size of the list
   *
   * @param {function} [cb]
   */
  getSize(cb) {
    this.redis.llen(this.config.listName, (err, size) => {
      if (err && cb) {
        cb(err);
        return;
      }

      this.emitSize(size, () => {
        if (cb) {
          cb(null, size);
        }
      });
    });
  }
}

module.exports = DataList;

const WeightedList = require('../lib/distributedWeightedQueueing');

const config = {
  redis: {
    host: 'localhost',
    port: 6379,
  },
  maxSize: 1000000, // optional but will default to a value
  baseKey: 'mylists', // optional will default to a value
  weights: [10, 3, 2, 1],
};

const list = new WeightedList(config);

list.on('size_change', (size) => {
  console.log(`size ${size}`);
});

function getRandomShard() {
  return Math.floor(Math.random() * config.weights.length);
}

function put(count) {
  if (count === 0) {
    return;
  }

  const shard = getRandomShard();
  list.put(shard, (shard + 1) * 100, () => {
    put(--count);
  });
}

put(1000);

function get(count) {
  if (count === 0) {
    return;
  }

  count += -1;

  list.get((err, data) => {
    console.log(`size=${list.getSize()} data=${data}`);
    process.nextTick(() => {
      get(count);
    });
  });
}

setTimeout(() => {
  get(1001);
}, 2000);


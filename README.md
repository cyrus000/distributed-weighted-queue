# distributed-weighted-queue

## purpose
    To have a weighted queue in a distributed mantor
    You can set shards and each shard can have a different prioity

    Example: weights: [10, 3, 2, 1],
    First shard with weight of 10 will be 10x more likely to
    return its item when you request the next item over the last shard

## requirements
    node >6
    redis-server


# Example
```javascript
const WeightedList = require('distributed-weighted-queue');
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
```
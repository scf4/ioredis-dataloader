// Do not use

const DataLoader = require('dataloader');

module.exports = ({ redis }) => {
  const mgetLoader = new DataLoader(keys => redis.mget(keys));
  return class RedisDataLoader {
    constructor(keyPrefix, defaultLoader, opts = {}) {
      this.opts = { exprire: opts.expire };
      // prefixKey function
      this.prefixKey = (key) => `${keyPrefix}:${key}`;
      this.keyPrefix = keyPrefix;
      // Option custom de/serialize functions:
      if (opts.serialize) this.customSerialize = opts.serialize;
      if (opts.deserialize) this.customDeserialize = opts.deserialize;
      // Create internal loader
      this.loader = new DataLoader(
        (keys) => this.batchLoad(keys, defaultLoader),
        defaultLoader,
        opts,
      );
      this.setAndGet = this.setAndGet.bind(this);
      this.serialize = this.serialize.bind(this);
      this.deserialize = this.deserialize.bind(this);
      this.batchLoad = this.batchLoad.bind(this);
      this.loadMany = this.loadMany.bind(this);
      this.load = this.load.bind(this);
      this.prime = this.prime.bind(this);
      this.clear = this.clear.bind(this);
      this.clearLocal = this.clearLocal.bind(this);
      this.clearAllLocal = this.clearAllLocal.bind(this);
    }
    async setAndGet(key, val) {
      const serialized = this.serialize(val);
      const multi = redis.multi().set(key, serialized);
      // Expire?
      if (this.opts.expire) multi.expire(key, this.opts.expire);
      multi.get(key);
      // Execute and get response
      const [,, [, response]] = await multi.exec();
      return this.deserialize(response);
    }
    serialize(val) {
      if (this.customSerialize) return this.customSerialize(val);
      if (val === null) return '';
      if (typeof val === 'object') return JSON.stringify(val);
      throw new Error('Must be Object or Null');
    }
    deserialize(string) {
      if (this.customDeserialize) return this.customDeserialize(string);
      if (string === '' || string === null) return string;
      return JSON.parse(string);
    }
    async batchLoad(keys, defaultLoader) {
      // Check for results in redis:
      const prefixedKeys = keys.map(this.prefixKey);
      const results = await mgetLoader.loadMany(prefixedKeys);
      const returning = results.map(this.deserialize).map(async (result, i) => {
        // If we have a result
        if (result === '') return null;
        if (result !== null) return result;
        // Load from the user-provided loader
        const val = await defaultLoader.load(keys[i]);
        // And add to the redis cache
        const key = this.prefixKey(keys[i]);
        const response = await this.setAndGet(key, val);
        if (response === '') return null;
        return response;
      });
      return returning;
    }

    load(key) {
      if (!key) throw new TypeError('key parameter is required');
      return this.loader.load(key);
    }

    loadMany(keys) {
      if (!keys) throw new TypeError('keys parameter is required');
      return this.loader.loadMany(keys);
    }

    async prime(key, val) {
      if (!key) throw new TypeError('key parameter is required');
      if (val === undefined) throw new TypeError('keys parameter is required');
      const result = await this.setAndGet(this.prefixKey(key), val, this.opts);
      this.loader.clear(key);
      this.loader.prime(key, (result === '') ? null : result);
    }

    async clear(key) {
      if (!key) throw new TypeError('key parameter is required');
      await redis.del(this.prefixKey(key));
      return this.loader.clear(key);
    }

    async clearAllLocal() {
      return this.loader.clearAll();
    }

    async clearLocal(key) {
      return this.loader.clear(key);
    }
  }
};

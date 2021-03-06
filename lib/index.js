'use strict';

var redis = require('redis');
var async = require('async');
var rarity = require('rarity');
var url = require('url');

module.exports = function init(opts) {
  if(!opts) {
    opts = {};
  }

  if(typeof opts.redis === 'string') {
    var components = url.parse(opts.redis);

    opts.redis = {
      port: components.port || 6379,
      host: components.hostname || "localhost",
      pass: (components.auth) ? ((components.auth.split(':').length > 1) ? components.auth.split(':')[1] : components.auth) : undefined
    };
  }

  module.exports.opts = {};
  module.exports.opts.prefix = opts.prefix || 'yaqs';
  module.exports.opts.defaultConcurrency = opts.defaultConcurrency || 1;
  module.exports.opts.defaultPriority = opts.defaultPriority || module.exports.PRIORITY.NORMAL;
  module.exports.opts.defaultRetry = opts.defaultRetry || 0;
  module.exports.opts.defaultRetryDelay = opts.defaultRetryDelay || 2000;
  module.exports.opts.defaultTimeout = opts.defaultTimeout || -1;
  module.exports.opts.defaultTtl = opts.defaultTtl || -1;
  module.exports.opts.defaultTimeoutOnStop = opts.defaultTimeoutOnStop || 2000;

  module.exports.opts.redis = opts.redis || {};

  // Normal connection
  module.exports.conn = module.exports.getConn('conn');

  // Subscribe connection
  module.exports.sconn = module.exports.getConn('sconn');

  // Publish connection
  module.exports.pconn = module.exports.getConn('pconn');

  module.exports.queues = {};

  return module.exports;
};

module.exports.getConn = function getConn(type) {
  if(this[type]) {
    return this[type];
  }

  this[type] = redis.createClient(module.exports.opts.redis.port || 6379, module.exports.opts.redis.host || "localhost", {
    auth_pass: module.exports.opts.redis.pass
  });

  this[type].on('error', function(err) {
    Object.keys(module.exports.queues).forEach(function(name) {
      module.exports.queues[name].emit('error', err);

      if(!module.exports.queues[name].subscribed) {
        return;
      }

      module.exports.queues[name].subscribed = false;
      module.exports.queues[name].sconn.once('ready', function() {
        if(!module.exports.queues[name]) {
          return;
        }

        module.exports.queues[name].subscribe();
      });
    });
  });

  return this[type];
};

module.exports.createQueue = function createQueue(name, opts) {
  if(typeof name !== 'string') {
    return new Error("Expect a string as name");
  }

  var Queue = require('./queue.js');

  if(module.exports.queues[name]) {
    return module.exports.queues[name];
  }

  var queue = new Queue(name, opts);
  module.exports.queues[name] = queue;

  return queue;
};

module.exports.stopAllQueues = function stopAllQueues(cb) {
  async.each(Object.keys(module.exports.queues), function stopQueue(name, cb) {
    module.exports.queues[name].stop(cb);
  }, cb);
};

module.exports.getPrefix = function getPrefix(key) {
  return (module.exports.opts.prefix || 'yaqs') + ':' + key;
};

module.exports.listQueues = function listQueues(prefix, cb) {
  async.concat(['pending', 'processing'], function getKeys(suffix, cb) {
    module.exports.getConn('conn').keys((prefix ? prefix + ':' : '*:') + suffix, cb);
  }, function(err, keys) {
    if(err) {
      return cb(err);
    }

    cb(null, keys.map(function(key) {
      var elems = key.split(':');
      elems.pop();

      return elems.join(':');
    }).filter(function(key, idx, keys) {
      return keys.lastIndexOf(key) === idx;
    }).map(function(key) {
      var elems = key.split(':');
      var prefix = elems.shift();

      return {
        prefix: prefix,
        name: elems.join(':')
      };
    }));
  });
};

module.exports.stats = function stats(cb) {
  async.waterfall([
    function getQueues(cb) {
      module.exports.getConn('conn').keys(module.exports.getPrefix('*'), cb);
    },
    function getQueuesName(keys, cb) {
      keys = keys.filter(function(key) {
        return key.split(':').length === 2;
      });

      cb(null, keys.map(function(key) {
        return key.split(':')[1];
      }));
    },
    function getQueuesStats(queues, cb) {
      var conn = module.exports.getConn('conn').multi();

      queues.forEach(function(name) {
        conn = conn
          .zrange(module.exports.getPrefix(name + ':pending'), 0, -1)
          .zrange(module.exports.getPrefix(name + ':processing'), 0, -1)
          .hget(module.exports.getPrefix(name), 'total');
      });

      conn.exec(rarity.carry([queues], cb));
    },
    function returnStats(queues, replies, cb) {
      var stats = {};

      queues.forEach(function(name, index) {
        stats[name] = {
          pending: replies[index * 3].length,
          processing: replies[index * 3 + 1].length,
          total: replies[index * 3 + 2] ? parseInt(replies[index * 3 + 2]) : 0
        };
      });

      cb(null, stats);
    }
  ], cb);
};

module.exports.PRIORITY = {
  VERY_HIGH: 100,
  HIGH: 50,
  NORMAL: 0,
  LOW: -50,
  VERY_LOW: -100
};

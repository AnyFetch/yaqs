'use strict';

var redis = require('redis');
var async = require('async');
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
  module.exports.opts.defaultPriority = opts.defaultPriority || module.exports.priority.normal;
  module.exports.opts.defaultRetry = opts.defaultRetry || 0;
  module.exports.opts.defaultRetryDelay = opts.defaultRetryDelay || 2000;
  module.exports.opts.defaultTimeout = opts.defaultTimeout || -1;
  module.exports.opts.defaultTimeoutOnStop = opts.defaultTimeoutOnStop || 2000;

  module.exports.opts.redis = opts.redis || {};

  module.exports.conn = module.exports.getConn('conn');
  module.exports.sconn = module.exports.getConn('sconn');
  module.exports.pconn = module.exports.getConn('pconn');

  module.exports.queues = [];

  return module.exports;
};

module.exports.getConn = function getConn(type) {
  if(this[type]) {
    return this[type];
  }

  this[type] = redis.createClient(module.exports.opts.redis.port || 6379, module.exports.opts.redis.host || "localhost", {
    auth_pass: module.exports.opts.redis.pass
  });
  return this[type];
};

module.exports.createQueue = function createQueue(name, opts) {
  var queue = new (require('./queue.js'))(name, opts);

  module.exports.queues.push(queue);

  queue.once('remove', function removeQueue(queue) {
    for(var i = 0, c = module.exports.queues.length; i < c; i += 1) {
      if(module.exports.queues[i].id === queue.id) {
        module.exports.queues.splice(i, 1);
        c -= 1;
      }
    }
  });

  return queue;
};

module.exports.stopAllQueues = function stopAllQueues(cb) {
  async.each(module.exports.queues, function stopQueue(queue, cb) {
    queue.stop(cb);
  }, cb);
};

module.exports.getPrefix = function getPrefix(key) {
  return (module.exports.opts.prefix || 'yaqs') + ':' + key;
};

module.exports.priority = {
  very_high: 100,
  high: 50,
  normal: 0,
  low: -50,
  very_low: -100
};

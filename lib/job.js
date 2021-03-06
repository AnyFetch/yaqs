'use strict';

var async = require('async');
var rarity = require('rarity');

function Job(queue, data, opts) {
  opts = opts || {};

  this.id = null;
  this.queue = queue;
  this.data = data;

  this.lastUpdate = Date.now();

  this.opts = {};
  this.opts.priority = opts.priority || queue.opts.defaultPriority;
  this.opts.timeout = opts.timeout || queue.opts.defaultTimeout;
  this.opts.retry = opts.retry || queue.opts.defaultRetry;
  this.opts.retryDelay = opts.retryDelay || queue.opts.defaultRetryDelay;
  this.opts.ttl = opts.ttl || queue.opts.defaultTtl;

  this.launchCount = 0;
}

Job.prototype.loadFromData = function loadFromData(data) {
  this.id = parseInt(data.id);
  this.opts.priority = data.priority;
  this.opts.timeout = data.timeout;
  this.opts.retry = data.retry;
  this.opts.retryDelay = data.retryDelay;
  this.lastUpdate = data.lastUpdate;
  this.data = JSON.parse(data.data);

  return this;
};

Job.prototype.saveToData = function saveToData() {
  return {
    id: this.id,
    priority: this.opts.priority,
    timeout: this.opts.timeout,
    retry: this.opts.retry,
    retryDelay: this.opts.retryDelay,
    lastUpdate: this.lastUpdate,
    data: JSON.stringify(this.data)
  };
};

Job.prototype.setPriority = function setPriority(priority) {
  this.opts.priority = priority;

  return this;
};

Job.prototype.save = function save(finalCb) {
  var self = this;

  if(!finalCb) {
    finalCb = function(err) {
      if(err) {
        console.warn(err);
      }
    };
  }

  async.waterfall([
    function generateId(cb) {
      function getAnId(cb) {
        // We use total to stock the last job ID
        // So we increment it and use the last value as the new job ID
        self.queue.conn.multi()
          .hsetnx(self.queue.getPrefix(), 'total', 0)
          .hincrby(self.queue.getPrefix(), 'total', 1)
          .exec(function(err, replies) {
            if(err) {
              return cb(err);
            }

            var jobId = parseInt(replies[1]);

            if(typeof jobId !== "number" || isNaN(jobId)) {
              return self.queue.conn.hset(self.queue.getPrefix(), 'total', 0, function(err) {
                if(err) {
                  return cb(err);
                }

                getAnId(cb);
              });
            }

            cb(null, jobId);
          });
      }

      getAnId(cb);
    },
    function saveData(jobId, cb) {
      self.id = jobId;

      var conn = self.queue.conn.multi()
        .hmset(self.queue.getPrefix('jobs:' + self.id), self.saveToData())
        .zadd(self.queue.getPrefix('pending'), -self.opts.priority, module.exports.padId(self.id));

      if(self.opts.ttl > 0) {
        conn = conn.pexpire(self.queue.getPrefix('jobs:' + self.id), self.opts.ttl);
      }

      conn
        .exec(cb);
    },
    function emitEvent(replies, cb) {
      self.queue.pconn.publish(self.queue.getPrefix(), 'enqueue');
      cb();
    }
  ], finalCb);

  return this;
};

Job.prototype.remove = function remove(cb) {
  var self = this;
  if(!cb) {
    cb = function(err) {
      if(err) {
        console.warn(err);
      }
    };
  }

  if(!this.id) {
    return cb(new Error("Can't remove unsaved job"));
  }

  async.waterfall([
    function removeKeys(cb) {
      self.queue.conn.multi()
        .zrem(self.queue.getPrefix('pending'), module.exports.padId(self.id))
        .del(self.queue.getPrefix('jobs:' + self.id))
        .exec(cb);
    }
  ], rarity.slice(1, cb));
};

module.exports = Job;

module.exports.padId = function padId(id) {
  id = id.toString();

  for(var i = id.length; i < 10; i += 1) {
    id = "0" + id;
  }

  return id;
};

'use strict';

var util = require('util');
var events = require('events');
var async = require('async');
var rarity = require('rarity');

var client = require('./index.js');
var Worker = require('./worker.js');
var Job = require('./job.js');

function Queue(name, opts) {
  opts = opts || {};
  Queue.id = Queue.id || 0;

  Queue.id += 1;
  this.id = Queue.id;

  this.state = Queue.STOPPED;
  this.name = name;
  this.prefix = client.getPrefix(this.name);
  this.empty = true;

  this.conn = client.conn;
  this.sconn = client.sconn;
  this.pconn = client.pconn;

  this.workers = [];
  this.workersRunning = 0;
  this.workerFunction = function(job, cb) {
    cb(new Error("No worker function defined"));
  };

  this.opts = {};
  this.opts.concurrency = opts.concurrency || client.opts.defaultConcurrency;
  this.opts.defaultPriority = opts.defaultPriority || client.opts.defaultPriority;
  this.opts.defaultRetry = opts.defaultRetry || client.opts.defaultRetry;
  this.opts.defaultRetryDelay = opts.defaultRetryDelay || client.opts.defaultRetryDelay;
  this.opts.defaultTimeout = opts.defaultTimeout || client.opts.defaultTimeout;
  this.opts.timeoutOnStop = opts.timeoutOnStop || client.opts.defaultTimeoutOnStop;

  var self = this;
  this.sconn.on('message', function(channel) {
    if(channel === self.getPrefix('enqueue')) {
      self.launchJob();
    }
  });

  this.sconn.subscribe(this.getPrefix('enqueue'));
}

util.inherits(Queue, events.EventEmitter);

Queue.STARTING = 1;
Queue.STARTED = 2;
Queue.STOPPING = 3;
Queue.STOPPED = 4;

Queue.prototype.getPrefix = function getPrefix(key) {
  return this.prefix + (key ? ':' + key : '');
};

Queue.prototype.fetchJobId = function fetchJobId(cb) {
  var self = this;
  async.waterfall([
    function retrieveJobId(cb) {
      self.conn.multi()
        .zrange(self.getPrefix('pending'), 0, 0)
        .zremrangebyrank(self.getPrefix('pending'), 0, 0)
        .exec(cb);
    },
    function returnJobId(replies, cb) {
      var jobId = replies[0] && replies[0][0];

      if(jobId) {
        cb(null, parseInt(jobId));
      }
      else {
        var err = new Error("No pending jobs");
        err.warning = true;

        cb(err);
      }
    }
  ], cb);
};

Queue.prototype.fetchJobData = function fetchJobData(jobId, cb) {
  var self = this;
  async.waterfall([
    function retrieveJobData(cb) {
      self.conn.multi()
        .hgetall(self.getPrefix('jobs:' + jobId))
        .exec(cb);
    },
    function returnJobData(replies, cb) {
      var jobData = replies[0];

      if(jobData) {
        cb(null, jobId, jobData);
      }
      else {
        cb(new Error("Can't retrieve job data for job " + jobId));
      }
    }
  ], cb);
};

Queue.prototype.launchJob = function launchJob(cb) {
  var self = this;

  var errorEmitter = function(err) {
    if(err) {
      self.emit('error', err);
    }
  };

  if(!cb) {
    cb = errorEmitter;
  }

  if(this.workersRunning >= this.opts.concurrency || this.state !== Queue.STARTED) {
    return cb();
  }

  this.workersRunning += 1;

  async.waterfall([
    function retrieveJobId(cb) {
      self.fetchJobId(cb);
    },
    function retrieveJobData(jobId, cb) {
      self.empty = false;
      self.fetchJobData(jobId, cb);
    },
    function createWorker(jobId, jobData, cb) {
      var job = (new Job(self)).loadFromData(jobData);
      var worker = new Worker();

      worker.setFunction(self.workerFunction).setJob(job);

      self.workers.push(worker);

      worker.once('finish', function finishJob() {
        self.workersRunning -= 1;

        for(var i = 0, c = self.workers.length; i < c; i += 1) {
          if(self.workers[i].id === worker.id) {
            self.workers.splice(i, 1);
            c -= 1;
          }
        }

        async.waterfall([
          function removeJob(cb) {
            job.remove(cb);
          },
          function removeFromProcessing(cb) {
            self.conn.multi()
              .zrem(self.getPrefix('processing'), Job.padId(self.id))
              .exec(cb);
          },
          function launchJob(replies, cb) {
            self.launchJob(cb);
          },
          function checkEmptyQueue(cb) {
            if(self.workersRunning !== 0 || self.workers.length !== 0) {
              return cb();
            }

            self.conn.multi()
              .zcount(self.getPrefix('processing'), '-inf', '+inf')
              .zcount(self.getPrefix('pending'), '-inf', '+inf')
              .exec(function(err, replies) {
                if(err) {
                  return cb(err);
                }

                if(replies[0] === 0 && replies[1] === 0 && !self.empty) {
                  self.empty = true;
                  self.emit('empty', self);
                }

                cb();
              });
          }
        ], errorEmitter);
      });

      cb(null, worker);
    },
    function addToProcessing(worker, cb) {
      self.conn.multi()
        .zadd(self.getPrefix('processing'), self.opts.priority, Job.padId(worker.job.id))
        .exec(rarity.carry([worker], cb));
    },
    function startWorker(worker, replies, cb) {
      worker.start(cb);
    }
  ], function(err) {
    if(err && err.warning) {
      self.workersRunning -= 1;
      cb();
    }
    else if(err) {
      cb(err);
    }
    else {
      self.launchJob(cb);
    }
  });

  return this;
};

Queue.prototype.start = function start(cb) {
  var self = this;

  if(!cb) {
    cb = function(err) {
      if(err) {
        self.emit('error', err);
      }
    };
  }

  if(this.state !== Queue.STOPPED) {
    return cb(new Error("Queue isn't stopped"));
  }

  this.state = Queue.STARTED;

  // TO-DO
  // - Create all workers

  this.launchJob(function(err) {
    if(err) {
      self.state = Queue.STOPPED;
      return cb(err);
    }

    self.emit('start', self);
    return cb();
  });


  return this;
};

Queue.prototype.setWorker = function setWorker(fn) {
  this.workerFunction = fn;

  // TO-DO
  // - Change function of workers

  return this;
};

Queue.prototype.stop = function stop(cb) {
  var self = this;

  if(!cb) {
    cb = function(err) {
      if(err) {
        self.emit('error', err);
      }
    };
  }

  this.state = Queue.STOPPED;

  async.waterfall([
    function checkStates(cb) {
      var allWorkersAreStopped = false;

      var timeout;
      if(self.opts.timeoutOnStop > 0) {
        timeout = setTimeout(function() {
          cb();
          cb = function() {};
        }, self.opts.timeoutOnStop);
      }

      async.until(function() {
        return allWorkersAreStopped;
      }, function(cb) {
        allWorkersAreStopped = self.workers.every(function(worker) {
          if(worker.state === Worker.STARTED) {
            worker.state = Worker.STOPPING;
          }

          return worker.state !== Worker.STOPPING && worker.state !== Worker.STARTING;
        });

        setTimeout(cb, 500);
      }, function(err) {
        if(timeout) {
          clearTimeout(timeout);
        }

        cb(err);
      });
    },
    function emitEvent(cb) {
      self.state = Queue.STOPPED;
      self.emit('stop', self);

      cb();
    }
  ], cb);

  return this;
};

Queue.prototype.remove = function remove(cb) {
  var self = this;

  if(!cb) {
    cb = function(err) {
      if(err) {
        self.emit('error', err);
      }
    };
  }

  async.waterfall([
    function stopQueue(cb) {
      self.stop(cb);
    },
    function getKeys(cb) {
      self.conn.keys(self.getPrefix() + '*', cb);
    },
    function removeKeys(keys, cb) {
      async.eachLimit(keys, 5, function removeKey(key, cb) {
        self.conn.del(key, cb);
      }, cb);
    },
    function emitEvent(cb) {
      self.emit('remove', self);
      cb();
    }
  ], cb);

  return this;
};

Queue.prototype.createJob = function createJob(data, opts) {
  return new Job(this, data, opts);
};

module.exports = Queue;

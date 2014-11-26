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
  this.empty = false;

  this.conn = client.conn;
  this.sconn = client.sconn;
  this.pconn = client.pconn;

  var hiddingProps = {
    enumerable: false,
    configurable: false,
    writable: false
  };

  Object.defineProperty(this, "conn", hiddingProps);
  Object.defineProperty(this, "sconn", hiddingProps);
  Object.defineProperty(this, "pconn", hiddingProps);

  this.runningWorkers = [];
  this.availableWorkers = [];

  this.workerFunction = function(job, cb) {
    cb(new Error("No worker function defined"));
  };

  this.opts = {};
  this.opts.concurrency = opts.concurrency || client.opts.defaultConcurrency;
  this.opts.defaultPriority = opts.defaultPriority || client.opts.defaultPriority;
  this.opts.defaultRetry = opts.defaultRetry || client.opts.defaultRetry;
  this.opts.defaultRetryDelay = opts.defaultRetryDelay || client.opts.defaultRetryDelay;
  this.opts.defaultTimeout = opts.defaultTimeout || client.opts.defaultTimeout;
  this.opts.defaultTtl = opts.defaultTtl || client.opts.defaultTtl;
  this.opts.timeoutOnStop = opts.timeoutOnStop || client.opts.defaultTimeoutOnStop;

  var self = this;
  var messageListener = function(channel) {
    if(channel === self.getPrefix('enqueue')) {
      self.launchJob();
    }
  };

  this.sconn.on('message', messageListener);

  this.once('remove', function() {
    self.sconn.removeListener('message', messageListener);
  });

  this.cleanProcessingStuckJobs(function() {});
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
        var err = new Error("Can't retrieve job data for job " + jobId);

        err.warning = true;
        err.retry = true;

        cb(err);
      }
    }
  ], cb);
};

Queue.prototype.removeWorkerFromRunning = function removeWorkerFromRunning(worker) {
  for(var i = 0, c = this.runningWorkers.length; i < c; i += 1) {
    if(this.runningWorkers[i].id === worker.id) {
      this.runningWorkers.splice(i, 1);
      this.availableWorkers.push(worker);

      c -= 1;
    }
  }
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

  if(this.availableWorkers.length === 0 || this.state !== Queue.STARTED) {
    return cb();
  }

  var worker = this.availableWorkers.pop();
  this.runningWorkers.push(worker);

  async.waterfall([
    function retrieveJobId(cb) {
      self.fetchJobId(cb);
    },
    function retrieveJobData(jobId, cb) {
      self.fetchJobData(jobId, cb);
    },
    function createWorker(jobId, jobData, cb) {
      if(self.empty) {
        self.empty = false;
        self.emit('resumed', self);
      }

      var job = (new Job(self)).loadFromData(jobData);
      worker.setFunction(self.workerFunction).setJob(job);

      worker.once('finish', function finishJob() {
        self.removeWorkerFromRunning(worker);

        async.waterfall([
          function removeFromProcessing(cb) {
            self.conn.multi()
              .zrem(self.getPrefix('processing'), Job.padId(job.id))
              .exec(cb);
          },
          function removeJob(replies, cb) {
            job.remove(cb);
          },
          function launchJob(cb) {
            self.launchJob(cb);
          },
          function checkEmptyQueue(cb) {
            self.checkEmpty(cb);
          }
        ], errorEmitter);
      });

      cb(null, worker);
    },
    function addToProcessing(worker, cb) {
      worker.job.lastUpdate = Date.now();

      self.conn.multi()
        .zadd(self.getPrefix('processing'), -worker.job.opts.priority, Job.padId(worker.job.id))
        .hset(self.getPrefix('jobs:' + worker.job), 'lastUpdate', worker.job.lastUpdate)
        .exec(rarity.carry([worker], cb));
    },
    function startWorker(worker, replies, cb) {
      worker.start(cb);
    }
  ], function(err) {
    if(err) {
      self.removeWorkerFromRunning(worker);

      // console.log LAUNCH JOB ERROR

      if(err.retry) {
        return self.launchJob(cb);
      }

      // A warning isn't a serious error, so we skip it
      if(err.warning) {
        return self.checkEmpty(cb);
      }

      cb(err);
    }
    else {
      self.launchJob(cb);
    }
  });

  return this;
};

Queue.prototype.checkEmpty = function checkEmpty(cb) {
  if(this.runningWorkers.length !== 0) {
    return cb();
  }

  var self = this;
  this.conn.multi()
    .zcount(this.getPrefix('pending'), '-inf', '+inf')
    .exec(function(err, replies) {
      if(err) {
        return cb(err);
      }

      if(replies[0] === 0 && !self.empty) {
        self.empty = true;
        self.emit('empty', self);
      }

      cb();
    });
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

  for(var i = 0; i < this.opts.concurrency; i += 1) {
    this.availableWorkers.push(new Worker());
  }

  async.waterfall([
    function subscribeEnqueue(cb) {
      self.sconn.subscribe(self.getPrefix('enqueue'), rarity.slice(1, cb));
    },
    function launchJob(cb) {
      self.state = Queue.STARTED;
      self.launchJob(function(err) {
        if(err) {
          self.state = Queue.STOPPED;
          return cb(err);
        }

        self.emit('start', self);
        cb();
      });
    }
  ], cb);

  return this;
};

Queue.prototype.setWorker = function setWorker(fn) {
  this.workerFunction = fn;

  this.runningWorkers.forEach(function(worker) {
    worker.setFunction(fn);
  });

  this.availableWorkers.forEach(function(worker) {
    worker.setFunction(fn);
  });

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

  if(this.state !== Queue.STARTED) {
    return cb(new Error("Queue isn't started"));
  }

  this.state = Queue.STOPPING;

  async.waterfall([
    function unsubscribeEnqueue(cb) {
      self.sconn.unsubscribe(self.getPrefix('enqueue'), rarity.slice(1, cb));
    },
    function waitWorkers(cb) {
      async.each(self.runningWorkers, function(worker, cb) {
        worker.stop(self.opts.timeoutOnStop, cb);
      }, cb);
    },
    function emitEvent(cb) {
      self.availableWorkers = [];
      self.runningWorkers = [];

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
      if(self.state === Queue.STARTED) {
        return self.stop(cb);
      }

      cb();
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
      delete client.queues[self.name];

      cb();
    }
  ], cb);

  return this;
};

Queue.prototype.createJob = function createJob(data, opts) {
  return new Job(this, data, opts);
};

Queue.prototype.stats = function stats(cb) {
  this.conn.multi()
    .zrange(this.getPrefix('pending'), 0, -1)
    .zrange(this.getPrefix('processing'), 0, -1)
    .hget(this.getPrefix(), 'total')
    .exec(function(err, replies) {
      if(err) {
        return cb(err);
      }

      cb(null, {
        pending: replies[0].length,
        processing: replies[1].length,
        total: replies[2] ? parseInt(replies[2]) : 0
      });
    });
};

Queue.prototype.cleanProcessingStuckJobs = function cleanProcessingStuckJobs(cb) {
  var self = this;
  async.waterfall([
    function retrieveProcessingJobs(cb) {
      self.conn.zrange(self.getPrefix('processing'), 0, -1, cb);
    },
    function retrieveJobsData(jobs, cb) {
      var conn = self.conn.multi();

      (jobs || []).forEach(function(jobId) {
        conn = conn.hgetall(self.getPrefix('jobs:' + parseInt(jobId)));
      });

      conn.exec(rarity.carry([jobs || []], cb));
    },
    function filterStuckJobs(jobIds, jobs, cb) {
      jobs.forEach(function(job, index) {
        if(!job) {
          jobs[index] = {
            id: parseInt(jobIds[index])
          };
        }
      });

      async.filter(jobs || [], function(job, cb) {
        // After 24h, a job is considered to be stucked
        cb(!job.lastUpdate || Date.now() - job.lastUpdate > 24 * 3600 * 1000);
      }, rarity.pad([null], cb));
    },
    function removeStuckJobs(jobs, cb) {
      var conn = self.conn.multi();

      jobs.forEach(function(job) {
        conn = conn
          .del(self.getPrefix('jobs:' + job.id))
          .zrem(self.getPrefix('processing'), Job.padId(job.id));
      });

      conn.exec(cb);
    }
  ], rarity.slice(1, cb));
};

module.exports = Queue;

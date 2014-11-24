'use strict';

var util = require('util');
var events = require('events');

function Worker() {
  Worker.id = Worker.id || 0;

  Worker.id += 1;
  this.id = Worker.id;

  this.state = Worker.STOPPED;

  this.fn = function(job, cb) {
    cb(new Error("No worker function defined"));
  };

  this.job = null;
}

util.inherits(Worker, events.EventEmitter);

Worker.STARTING = 1;
Worker.STARTED = 2;
Worker.STOPPING = 3;
Worker.STOPPED = 4;

Worker.prototype.setFunction = function setFunction(fn) {
  this.fn = fn;
  return this;
};

Worker.prototype.setJob = function setJob(job) {
  this.job = job;
  return this;
};

Worker.prototype.start = function start(cb) {
  if(!this.job) {
    return cb(new Error("No job for this worker"));
  }

  this.state = Worker.STARTING;

  var self = this;

  var launchJob = function launchJob() {
    process.nextTick(function() {
      self.state = Worker.STARTED;
      self.emit('start', self, self.job);

      self.job.launchCount += 1;
      self.fn(self.job, endOfJob);

      cb();
    });
  };

  var endOfJob = function endOfJob(err, data) {
    if(self.state === Worker.STOPPED) {
      return;
    }

    if(err) {
      if(self.job.launchCount <= self.job.opts.retry) {
        if(self.job.opts.retryDelay > 0 && self.state !== Worker.STOPPING) {
          return setTimeout(launchJob, self.job.opts.retryDelay);
        }

        return launchJob();
      }

      return self.failed(err);
    }

    self.completed(data);
  };

  launchJob();

  if(this.job.opts.timeout > 0) {
    setTimeout(function() {
      endOfJob = function() {};
      self.timeout();
    }, this.job.opts.timeout);
  }
};

Worker.prototype.stop = function stop(timeout, cb) {
  if(!cb) {
    cb = timeout;
    timeout = -1;
  }

  if(this.state === Worker.STOPPED) {
    return cb();
  }

  var self = this;
  if(this.state === Worker.STARTING) {
    self.once('start', function() {
      self.state = Worker.STOPPING;
    });
  }
  else {
    this.state = Worker.STOPPING;
  }

  if(timeout > 0) {
    setTimeout(function() {
      if(self.state === Worker.STOPPED) {
        return;
      }

      self.timeout(new Error("Worker take too long when we stop queue"));
    }, timeout);
  }

  self.once('finish', function() {
    return cb();
  });
};

Worker.prototype.completed = function completed(data) {
  this.state = Worker.STOPPED;

  this.emit('completed', this, data);
  this.emit('finish', this, data);

  this.job.queue.emit('job.completed', this.job, data);
  this.job.queue.emit('job.finish', this.job, data);

  this.job = null;
};

Worker.prototype.failed = function failed(err) {
  this.state = Worker.STOPPED;

  this.emit('failed', this, err);
  this.emit('finish', this);

  this.job.queue.emit('job.failed', this.job, err);
  this.job.queue.emit('job.finish', this.job);

  this.job = null;
};

Worker.prototype.timeout = function timeout() {
  this.state = Worker.STOPPED;

  this.emit('timeout', this);
  this.emit('finish', this);

  this.job.queue.emit('job.timeout', this.job);
  this.job.queue.emit('job.finish', this.job);

  this.job = null;
};

module.exports = Worker;

'use strict';

var util = require('util');
var events = require('events');

function Worker(fn, job) {
  Worker.id = Worker.id || 0;

  this.id = Worker.id += 1;
  this.state = 'stopped';
  this.fn = fn;
  this.job = job;
}

util.inherits(Worker, events.EventEmitter);

Worker.prototype.start = function start(cb) {
  this.state = 'starting';

  var self = this;

  var launchJob = function launchJob() {
    process.nextTick(function() {
      self.state = 'started';
      self.emit('start', self, self.job);

      self.job.launchCount += 1;
      self.fn(self.job, function(err) {
        endOfJob(err);
      });

      cb();
    });
  };

  var endOfJob = function endOfJob(err) {
    if(err) {
      if(self.job.launchCount <= self.job.opts.retry) {
        if(self.job.opts.retryDelay > 0 && self.state !== 'stopping') {
          return setTimeout(function() {

          }, self.job.opts.retryDelay);
        }

        return launchJob();
      }

      return self.failed(err);
    }

    self.completed();
  };

  launchJob();

  if(this.job.opts.timeout > 0) {
    setTimeout(function() {
      endOfJob = function() {};
      self.timeout();
    }, this.job.opts.timeout);
  }
};

Worker.prototype.completed = function completed() {
  this.state = 'completed';

  this.emit('completed', this);
  this.emit('finish', this);

  this.job.queue.emit('job.completed', this.job);
  this.job.queue.emit('job.finish', this.job);
};

Worker.prototype.failed = function failed(err) {
  this.state = 'failed';

  this.emit('failed', this, err);
  this.emit('finish', this);

  this.job.queue.emit('job.failed', this.job, err);
  this.job.queue.emit('job.finish', this.job);
};

Worker.prototype.timeout = function timeout() {
  this.state = 'timeout';

  this.emit('timeout', this);
  this.emit('finish', this);

  this.job.queue.emit('job.timeout', this.job);
  this.job.queue.emit('job.finish', this.job);
};

module.exports = Worker;

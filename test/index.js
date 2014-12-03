'use strict';

require('should');

var async = require('async');
var rarity = require('rarity');

var client = require('../lib')();
var Queue = require('../lib/queue');

describe('Workflow', function() {
  var queue;
  beforeEach(function createQueue() {
    queue = client.createQueue('test');
  });

  afterEach(function removeQueue(done) {
    queue.removeAllListeners(['stop', 'empty']);
    queue.remove(done);
  });

  it('should execute all jobs', function(done) {
    var executedJobs = [];

    queue.once('empty', function() {
      executedJobs.should.have.lengthOf(3);
      executedJobs.should.eql(['test-1', 'test-2', 'test-3']);
      done();
    });

    queue.setWorker(function(job, cb) {
      executedJobs.push(job.data.identifier);
      return cb();
    });

    queue.once('error', done);

    async.waterfall([
      function addJobs(cb) {
        async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
          queue.createJob({identifier: identifier}).save(cb);
        }, cb);
      },
      function startQueue(cb) {
        queue.should.have.property('state', Queue.STOPPED);
        queue.start(cb);
      }
    ], function(err) {
      if(err) {
        return done(err);
      }

      queue.should.have.property('state', Queue.STARTED);
    });
  });

  it('should execute all jobs after starting', function(done) {
    var executedJobs = [];

    queue.on('empty', function() {
      if(executedJobs.length === 3) {
        executedJobs.should.eql(['test-1', 'test-2', 'test-3']);
        done();
      }
    });

    queue.setWorker(function(job, cb) {
      executedJobs.push(job.data.identifier);
      return cb();
    });

    queue.once('error', done);

    async.waterfall([
      function startQueue(cb) {
        queue.should.have.property('state', Queue.STOPPED);
        queue.start(cb);
      },
      function addJobs(cb) {
        setTimeout(function() {
          async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
            queue.createJob({identifier: identifier}).save(cb);
          }, cb);
        }, 200);
      },
    ], function(err) {
      if(err) {
        return done(err);
      }

      queue.should.have.property('state', Queue.STARTED);
    });
  });

  describe('Concurrency', function() {
    [1, 5, 10].forEach(function(concurrency) {
      var nbJobs = concurrency * 10;

      it('should execute ' + nbJobs + ' jobs with concurrency = ' + concurrency, function(done) {
        var executedJobs = [];

        queue.once('empty', function() {
          executedJobs.should.have.lengthOf(nbJobs);
          done();
        });

        queue.setWorker(function(job, cb) {
          executedJobs.push(job.data.identifier);
          return cb();
        });

        queue.once('error', done);

        async.waterfall([
          function addJobs(cb) {
            async.timesSeries(nbJobs, function(n, cb) {
              queue.createJob({identifier: 'test-' + n, n: n}).save(cb);
            }, rarity.slice(1, cb));
          },
          function startQueue(cb) {
            queue.start(cb);
          }
        ], function(err) {
          if(err) {
            return done(err);
          }
        });
      });
    });
  });

  describe('Priority', function() {
    it('should execute all jobs in priority order', function(done) {
      var jobs = [
        {identifier: 'test-1', priority: client.PRIORITY.VERY_LOW},
        {identifier: 'test-2', priority: client.PRIORITY.VERY_HIGH},
        {identifier: 'test-2', priority: client.PRIORITY.NORMAL},
        {identifier: 'test-3', priority: client.PRIORITY.LOW},
        {identifier: 'test-4', priority: client.PRIORITY.HIGH},
        {identifier: 'test-5', priority: client.PRIORITY.LOW},
        {identifier: 'test-6', priority: client.PRIORITY.NORMAL},
        {identifier: 'test-7', priority: client.PRIORITY.HIGH},
        {identifier: 'test-8', priority: client.PRIORITY.VERY_LOW},
        {identifier: 'test-9', priority: client.PRIORITY.VERY_HIGH}
      ];

      var expectedJobs = [
        'test-2', 'test-9', 'test-4', 'test-7', 'test-2', 'test-6', 'test-3', 'test-5', 'test-1', 'test-8'
      ];

      var executedJobs = [];

      queue.once('empty', function() {
        executedJobs.should.have.lengthOf(jobs.length);
        executedJobs.should.eql(expectedJobs);
        done();
      });

      queue.setWorker(function(job, cb) {
        executedJobs.push(job.data.identifier);
        return cb();
      });

      queue.once('error', done);

      async.waterfall([
        function addJobs(cb) {
          async.each(jobs, function addJob(job, cb) {
            queue
              .createJob({identifier: job.identifier})
              .setPriority(job.priority)
              .save(cb);
          }, cb);
        },
        function startQueue(cb) {
          queue.start(cb);
        }
      ], function(err) {
        if(err) {
          return done(err);
        }
      });
    });
  });

  describe('TTL', function() {
    it('should execute all jobs unexpired', function(done) {
      var executedJobs = [];

      queue.once('empty', function() {
        executedJobs.should.have.lengthOf(3);
        executedJobs.should.eql(['test-1', 'test-2', 'test-3']);
        done();
      });

      queue.setWorker(function(job, cb) {
        executedJobs.push(job.data.identifier);
        return cb();
      });

      queue.once('error', done);

      async.waterfall([
        function addJobs(cb) {
          async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
            queue.createJob({identifier: identifier}).save(cb);
          }, cb);
        },
        function addTTLJobs(cb) {
          queue.createJob({identifier: 'test-4'}, {ttl: 1}).save(cb);
        },
        function startQueue(cb) {
          queue.should.have.property('state', Queue.STOPPED);
          queue.start(cb);
        }
      ], function(err) {
        if(err) {
          return done(err);
        }

        queue.should.have.property('state', Queue.STARTED);
      });
    });
  });
});

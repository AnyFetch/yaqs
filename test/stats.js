'use strict';

require('should');

var async = require('async');
var rarity = require('rarity');

var client = require('../lib')();

describe('Stats', function() {
  describe('Queue.stats()', function() {
    var queue;
    beforeEach(function createQueue() {
      queue = client.createQueue('test');

      queue.setWorker(function(job, cb) {
        return cb();
      });
    });

    afterEach(function removeQueue(done) {
      queue.removeAllListeners();
      queue.remove(done);
    });

    it('should return stats without processing jobs', function(done) {
      async.waterfall([
        function addJobs(cb) {
          async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
            queue.createJob({identifier: identifier}).save(cb);
          }, cb);
        },
        function getStats(cb) {
          queue.stats(cb);
        },
        function checkStats(stats, cb) {
          stats.should.have.property('pending', 3);
          stats.should.have.property('processing', 0);
          stats.should.have.property('total', 3);

          cb();
        }
      ], done);
    });

    it('should return stats with processing jobs', function(done) {
      async.waterfall([
        function addJobs(cb) {
          async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
            queue.createJob({identifier: identifier}).save(cb);
          }, cb);
        },
        function startQueue(cb) {
          queue.on('error', function(err) {
            cb(err);
          });

          var originalCb = cb;
          queue.setWorker(function(job, cb) {
            if(originalCb) {
              originalCb();
              originalCb = null;
              return;
            }

            cb();
          });

          queue.start();
        },
        function getStats(cb) {
          queue.stats(cb);
        },
        function checkStats(stats, cb) {
          stats.should.have.property('pending', 2);
          stats.should.have.property('processing', 1);
          stats.should.have.property('total', 3);

          cb();
        },
        function stopQueue(cb) {
          queue.opts.timeoutOnStop = 1;
          queue.stop(cb);
        }
      ], done);
    });
  });

  describe('Client.stats()', function() {
    var queues = {};
    beforeEach(function createQueues() {
      ['test1', 'test2', 'test3'].forEach(function(name) {
        queues[name] = (client.createQueue(name));

        queues[name].setWorker(function(job, cb) {
          return cb();
        });

        delete client.queues[name];
      });
    });

    afterEach(function removeQueues(done) {
      async.eachSeries(Object.keys(queues), function(name, cb) {
        queues[name].removeAllListeners();
        queues[name].remove(cb);
      }, done);
    });

    it('should return stats without processing jobs', function(done) {
      var expectedStats = {};

      async.waterfall([
        function addJobs(cb) {
          async.eachSeries(Object.keys(queues), function(name, cb) {
            async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
              queues[name].createJob({identifier: identifier}).save(cb);
            }, cb);
          }, cb);
        },
        function getStats(cb) {
          client.stats(cb);
        },
        function generateExpectedStats(stats, cb) {
          async.eachSeries(Object.keys(queues), function(name, cb) {
            queues[name].stats(function(err, stats) {
              expectedStats[name] = stats;
              cb(err);
            });
          }, rarity.carry([stats], cb));
        },
        function checkStats(stats, cb) {
          stats.should.eql(expectedStats);
          cb();
        }
      ], done);
    });

    it('should return stats with processing jobs', function(done) {
      var expectedStats = {};

      async.waterfall([
        function addJobs(cb) {
          async.eachSeries(Object.keys(queues), function(name, cb) {
            async.eachSeries(['test-1', 'test-2', 'test-3'], function(identifier, cb) {
              queues[name].createJob({identifier: identifier}).save(cb);
            }, cb);
          }, cb);
        },
        function startQueues(cb) {
          async.each(Object.keys(queues), function(name, cb) {
            queues[name].on('error', function(err) {
              cb(err);
            });

            var originalCb = cb;
            queues[name].setWorker(function(job, cb) {
              if(originalCb) {
                originalCb();
                originalCb = null;
                return;
              }

              cb();
            });

            queues[name].start();
          }, cb);
        },
        function getStats(cb) {
          client.stats(cb);
        },
        function generateExpectedStats(stats, cb) {
          async.eachSeries(Object.keys(queues), function(name, cb) {
            queues[name].stats(function(err, stats) {
              expectedStats[name] = stats;
              cb(err);
            });
          }, rarity.carry([stats], cb));
        },
        function checkStats(stats, cb) {
          stats.should.eql(expectedStats);
          cb();
        },
        function stopQueues(cb) {
          async.each(Object.keys(queues), function(name, cb) {
            queues[name].opts.timeoutOnStop = 1;
            queues[name].stop(cb);
          }, cb);
        }
      ], done);
    });
  });
});

'use strict';

require('should');

var async = require('async');

var client = require('../lib')();
var Job = require('../lib/job');

describe('Clean', function() {
  var queue;
  beforeEach(function createQueue() {
    queue = client.createQueue('test');
  });

  afterEach(function removeQueue(done) {
    queue.remove(done);
  });

  describe('cleanProcessingStuckJobs()', function() {
    var stuckIds = [10, 58, 95];
    var normalIds = [11, 59, 96];

    beforeEach(function createStuckJobs(done) {
      async.eachSeries(stuckIds, function(id, cb) {
        var job = new Job(queue, {}, {});

        job.id = id;
        job.lastUpdate = Date.now() - 24 * 3600 * 1000;

        client.getConn('conn').multi()
          .hmset(queue.getPrefix('jobs:' + job.id), job.saveToData())
          .zadd(queue.getPrefix('processing'), -job.opts.priority, Job.padId(job.id))
          .exec(cb);
      }, done);
    });

    beforeEach(function createNormalJobs(done) {
      async.eachSeries(normalIds, function(id, cb) {
        var job = new Job(queue, {}, {});

        job.id = id;

        client.getConn('conn').multi()
          .hmset(queue.getPrefix('jobs:' + job.id), job.saveToData())
          .zadd(queue.getPrefix('processing'), -job.opts.priority, Job.padId(job.id))
          .exec(cb);
      }, done);
    });

    beforeEach(function removeDataFromAStuckJob(done) {
      client.getConn('conn').del(queue.getPrefix('jobs:' + stuckIds[0]), done);
    });

    it('should clean processing jobs which are stucked', function(done) {
      async.waterfall([
        function callClean(cb) {
          queue.cleanProcessingStuckJobs(cb);
        },
        function getStats(cb) {
          queue.stats(cb);
        },
        function checkStats(stats, cb) {
          stats.should.have.property('pending', 0);
          stats.should.have.property('processing', 3);

          cb();
        }
      ], done);
    });
  });
});

#!/usr/bin/env node

'use strict';

var url = require('url');
var redis = require('redis');
var program = require('commander');
var async = require('async');
var rarity = require('rarity');

program
  .version('0.0.1')
  .option('-r, --redis <url>', 'Redis URL');

var client;

// Monkey patch to automatically connect to Redis
var originalAction = program.Command.prototype.action;
program.Command.prototype.action = function(fn) {
  function newFunction() {
    var components = url.parse(program.redis || process.env.REDIS_URL || 'redis://localhost:6379');

    client = redis.createClient(components.port || 6379, components.hostname || "localhost", {
      auth_pass: (components.auth) ? ((components.auth.split(':').length > 1) ? components.auth.split(':')[1] : components.auth) : undefined
    });

    client.on('error', function(err) {
      console.error(err.stack);
    });

    var args = Array.prototype.slice.call(arguments, 0, -1);
    args.push(function endAction(err) {
      if(err) {
        console.error(err.stack);
      }

      client.quit();
    });

    fn.apply(null, args);
  }

  originalAction.apply(this, [newFunction]);
};

function getQueues(prefix, cb) {
  client.keys(prefix ? prefix + ':*' : '*:*', function(err, keys) {
    if(err) {
      return cb(err);
    }

    keys = keys.filter(function(key) {
      return key.split(':').length === 2;
    });

    cb(null, keys.map(function(key) {
      return {prefix: key.split(':')[0], name: key.split(':')[1]};
    }));
  });
}

function emitRemoveEvent(prefix, names, event, cb) {
  async.waterfall([
    function retrieveQueues(cb) {
      if(names.length > 0) {
        return cb(null, names.map(function(name) {
          return {prefix: prefix, name: name};
        }));
      }

      getQueues(prefix, cb);
    },
    function retrieveStats(queues, cb) {
      var conn = client.multi();

      queues.forEach(function(queue) {
        conn = conn.publish(queue.prefix + ':' + queue.name, event);
      });

      conn.exec(cb);
    },
  ], cb);
}

program
  .command('flush <prefix> [names...]')
  .description('Flush pending jobs from specified queues')
  .action(function flush(prefix, names, cb) {
    async.waterfall([
      function retrieveQueues(cb) {
        if(names.length > 0) {
          return cb(null, names.map(function(name) {
            return {prefix: prefix, name: name};
          }));
        }

        getQueues(prefix, cb);
      },
      function retrieveAndRemovePendingJobs(queues, cb) {
        var conn = client.multi();

        queues.forEach(function(queue) {
          var prefix = queue.prefix + ':' + queue.name;
          conn = conn
            .zrange(prefix + ':pending', 0, -1)
            .del(prefix + ':pending');
        });

        conn.exec(rarity.carry([queues], cb));
      },
      function removeJobDatas(queues, replies, cb) {
        var conn = client.multi();

        for(var i = 0; i < replies.length; i += 2) {
          replies[i].forEach(function(jobId) {
            conn = conn.del(queues[i / 2].prefix + ':' + queues[i / 2].name + ':jobs:' + parseInt(jobId));
          });
        }

        conn.exec(rarity.slice(1, cb));
      }
    ], cb);
  });

program
  .command('start <prefix> [names...]')
  .description('Start specified queues')
  .action(function start(prefix, names, cb) {
    emitRemoveEvent(prefix, names, 'start', cb);
  });

program
  .command('stop <prefix> [names...]')
  .description('Stop specified queues')
  .action(function stop(prefix, names, cb) {
    emitRemoveEvent(prefix, names, 'stop', cb);
  });


program
  .command('list [prefix]')
  .description('List queues')
  .action(function list(prefix, cb) {
    async.waterfall([
      function retrieveQueues(cb) {
        getQueues(prefix, cb);
      },
      function mergeQueuesByPrefix(queues, cb) {
        var prefix = {};

        queues.forEach(function(queue) {
          if(!prefix[queue.prefix]) {
            prefix[queue.prefix] = [];
          }

          prefix[queue.prefix].push(queue.name);
        });

        cb(null, prefix);
      },
      function displayList(prefix, cb) {
        Object.keys(prefix).forEach(function(name) {
          console.log(name);
          prefix[name].forEach(function(queue, index) {
            console.log("", (prefix[name].length - 1 === index ? '└' : '├') + "─", queue);
          });
        });

        cb();
      }
    ], cb);
  });

program
  .command('stats [prefix] [names...]')
  .description('Get stats of specified queues')
  .action(function stats(prefix, names, cb) {
    async.waterfall([
      function retrieveQueues(cb) {
        if(names.length > 0) {
          return cb(null, names.map(function(name) {
            return {prefix: prefix, name: name};
          }));
        }

        getQueues(prefix, cb);
      },
      function retrieveStats(queues, cb) {
        var conn = client.multi();

        queues.forEach(function(queue) {
          var prefix = queue.prefix + ':' + queue.name;
          conn = conn
            .zrange(prefix + ':pending', 0, -1)
            .zrange(prefix + ':processing', 0, -1)
            .hget(prefix, 'total');
        });

        conn.exec(rarity.carry([queues], cb));
      },
      function addStatsToQueues(queues, replies, cb) {
        for(var i = 0; i < replies.length; i += 3) {
          queues[i / 3].stats = {
            pending: replies[i].length,
            processing: replies[i + 1].length,
            total: replies[i + 2] ? parseInt(replies[i + 2]) : 0
          };
        }

        cb(null, queues);
      },
      function mergeQueuesByPrefix(queues, cb) {
        var prefix = {};

        queues.forEach(function(queue) {
          if(!prefix[queue.prefix]) {
            prefix[queue.prefix] = [];
          }

          prefix[queue.prefix].push({
            name: queue.name,
            stats: queue.stats
          });
        });

        cb(null, prefix);
      },
      function displayStats(prefix, cb) {
        Object.keys(prefix).forEach(function(name) {
          console.log(name);
          prefix[name].forEach(function(queue, index) {
            var isLast = prefix[name].length - 1 === index;
            console.log("", (isLast ? '└' : '├') + "─", queue.name);
            console.log((isLast ? "   " : " │ "), '├' + "─", 'pending', '->', queue.stats.pending);
            console.log((isLast ? "   " : " │ "), '├' + "─", 'processing', '->', queue.stats.processing);
            console.log((isLast ? "   " : " │ "), '└' + "─", 'total', '->', queue.stats.total);
          });
        });

        cb();
      }
    ], cb);
  });

program.parse(process.argv);

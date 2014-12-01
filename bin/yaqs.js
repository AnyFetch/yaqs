#!/usr/bin/env node

'use strict';

var url = require('url');
var redis = require('redis');
var program = require('commander');

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

program
  .command('flush <prefix> [names...]')
  .description('Flush specified queues')
  .action(function flush(prefix, names, cb) {
    console.log("TO-DO: Flush redis queues");
    cb();
  });

program
  .command('start <prefix> [names...]')
  .description('Start specified queues')
  .action(function start(prefix, names, cb) {
    console.log("TO-DO: Send start event to queues");
    cb();
  });

program
  .command('stop <prefix> [names...]')
  .description('Stop specified queues')
  .action(function stop(prefix, names, cb) {
    console.log("TO-DO: Send stop event to queues");
    cb();
  });


program
  .command('list [prefix]')
  .description('List queues')
  .action(function list(prefix, names, cb) {
    console.log("TO-DO: List queues");
    cb();
  });

program
  .command('stats [prefix] [name]')
  .description('Get stats of specified queues')
  .action(function stats(prefix, names, cb) {
    console.log("TO-DO: Display stats");
    cb();
  });

program.parse(process.argv);

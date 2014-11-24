'use strict';

require('should');

var client = require('../lib')();

before(function flushRedis(cb) {
  client.getConn('conn').flushdb(cb);
});

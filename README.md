## YAQS

YAQS (*Yet Another Queue System*) is a Node.js module to manage job queues using Redis.

YAQS manage any number of queues, any number of servers and any concurrency per server per queue.

### Installation

```
npm install yaqs
```

### Client

To use YAQS you need to create a client:

```js
var client = require('yaqs')(opts)
```

You can use this client to create, use and remove queues.

#### Options

* `prefix` (default=`'yaqs'`): A prefix for Redis keys
* `redis` (default=`{}`): Configuration to connect to Redis
    - `port` (default=`6379`): Redis port
    - `host` (default=`"localhost"`): Redis host
    - `pass` (default=`undefined`): Redis password
* `defaultConcurrency` (default=`1`): Default concurrency
* `defaultTimeoutOnStop` (default=`2000`): Default timeoutOnStop (timeout before force-killing a queue)
* `defaultPriority` (default=`client.PRIORITY.NORMAL`): Default priority when we create job without `priority` option
* `defaultRetry` (default=`0`): Default number of retry before giving up on a job
* `defaultRetryDelay` (default=`-1`): Default retry delay; values lower than 0 means direct retry.
* `defaultTimeout` (default=`-1`): Default retry

#### Usage

```js
// An object of constant priorities
client.PRIORITY;

// Create queue with a name and some options
client.createQueue(name, opts);

// Call queue.stop() on all queues
client.stopAllQueues(function(err) {}) 
```

### Queue

```js
var queue = client.createQueue(name, opts)
```

#### Options

* `concurrency` (default=`client.defaultConcurrency`): Concurrency for this queue in this server
* `timeoutOnStop` (default=`client.defaultTimeoutOnStop`): When stoppping the queue, maximum amount of time to wait before force-stopping workers. If the timeout is less than or equal to 0, no limit is enforced.
* `defaultPriority` (default=`client.PRIORITY.NORMAL`): Default priority for new jobs
* `defaultRetry` (default=0): Default retry for new jobs
* `defaultRetryDelay` (default=-2): Default retry delay for new jobs
* `defaultTimeout` (default=-1): Default timeout for new jobs

#### Usage

```js
// Start processing jobs,
// Callback can get an error if unable to retrieve jobs from Redis.
queue.start(function(err) {});

// Set the function used to process jobs.
// Can be hot-swapped while the queue is working
queue.setWorker(function (job, cb) {});

// Create a job with data and somes options (optional)
// This job needs to be saved to be processed
var job = queue.createJob(data, opts);

// Stop processing jobs
queue.stop(function(err) {});

// Stop processing jobs and remove all jobs
queue.remove(function(err) {});
```

#### Events

```js
queue.on('error', function(err) {})                // When an error occured
queue.on('start', function(queue) {})              // When we start the queue
queue.on('stop', function(queue) {})               // When we stop the queue
queue.on('remove', function(queue) {})             // When we remove the queue
queue.on('empty', function(queue) {})              // When a queue has no new jobs to process

queue.on('job.complete', function(job, data) {})   // When a job is completed
queue.on('job.failed', function(job, err) {})      // When a job fail
queue.on('job.timeout', function(job) {})          // When a job timeout
```

#### Example of worker function

```js
function workerFunction(job, cb) {
    // Use job.data to do something
    console.log(job.data.foo);

    // Return an error for event 'job.failed'
    cb(new Error("An error"));

    // Return some data for event 'job.complete'
    cb(null, {foo: 'bar'});
}

queue.setWorker(workerFunction);
```

### Job

```js
var job = queue.createJob(data, opts)
```

#### Options

* `priority` (default:`queue.defaultPriority`): Priority for the job, greater is better. Use `client.PRIORITY` values (VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW)
* `timeout` (default:`queue.defaultTimeout`): Timeout of the job. If the job is not finished after the timeout, it is considered finished (although the job is not killed, it is your responsability to ensure you're not leaking resources). If the timeout is less than or equal to 0, it is not taken into consideration. A `job.timeout` event is sent on the queue.
* `retry` (default:`queue.defaultRetry`): number of retry to use if the worker returns an error.
* `retryDelay` (default:`queue.defaultRetryDelay`): delay in ms between two retry

#### Usage

```js
// Set job's priority
job.setPriority(priority);

// Save job in redis for processing
job.save(function(err) {});

// Remove job from pending list
job.remove(function(err) {});
```

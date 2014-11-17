## YAQS

YAQS is a Node.JS module of job queues which use Redis. It can manage any number of queues, any number of servers and concurrency per server per queue.

### Installation

```
npm install yaqs
```

### Client

To use YAQS you need to create a client which can be used to create, use and remove queues.

```js
var client = require('yaqs')(opts)
```

#### Options

* prefix (default='yaqs') : A prefix for Redis keys
* redis (default=`{}`) : Configuration to connect to Redis
    - port (default=6379) : Redis port
    - host (default="localhost") : Redis host
    - pass (default=`undefined`) : Redis password
* defaultConcurrency (default=1) : Default concurrency when we create queue without `concurrency` option
* defaultTimeoutOnStop (default=2000) : Default timeoutOnStop when we create queue without `timeoutOnStop` option
* defaultPriority (default=`client.PRIORITY.NORMAL`) : Default priority when we create job without `priority` option
* defaultRetry (default=0) : Default retry when we create job without `retry` option
* defaultRetryDelay (default=-2) : Default retry delay when we create job without `retryDelay` option
* defaultTimeout (default=-1) : Default retry when we create job without `timeout` option

#### Usage

```js
client.PRIORITY;                    // An object of constant priorities
client.createQueue(name, opts)      // Create queue with a name and some options
client.stopAllQueues(function(err)) // Call queue.stop() on all queues
```

### Queue

```js
var queue = client.createQueue(name, opts)
```

#### Options

* concurrency (default=`client.defaultConcurrency`) : Concurrency for this queue in this server
* timeoutOnStop (default=`client.defaultTimeoutOnStop`) : When we stop the queue, we wait all workers. If they have not completed after the timeout, it stops force. If the timeout is less than or equal to 0, it is not taken into consideration.
* defaultPriority (default=`client.PRIORITY.NORMAL`) : Default priority when we create job without `priority` option
* defaultRetry (default=0) : Default retry when we create job without `retry` option
* defaultRetryDelay (default=-2) : Default retry delay when we create job without `retryDelay` option
* defaultTimeout (default=-1) : Default retry when we create job without `timeout` option

#### Usage

```js
queue.start(function(err))          // Start processing jobs
queue.setWorker(function (job, cb)) // Set the function which process jobs
queue.createJob(data, opts)         // Create a job with data and somes options
queue.stop(function(err))           // Stop processing jobs
queue.remove(function(err))         // Stop processing jobs and remove all queue's keys
```

#### Events

```js
queue.on('error', function(err))                // When an error occured
queue.on('start', function(queue))              // When we start the queue
queue.on('stop', function(queue))               // When we stop the queue
queue.on('remove', function(queue))             // When we remove the queue
queue.on('empty', function(queue))              // When a queue haven't new jobs to process

queue.on('job.complete', function(job, data))   // When a job is complete
queue.on('job.failed', function(job, err))      // When a job fail
queue.on('job.timeout', function(job))          // When a job timeout
```

#### Example of worker function

```js
function workerFunction(job, cb) {
    // Use job.data to do somethings
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

* priority (default:`queue.defaultPriority`) : Priority of the job, greater is better
* timeout (default:`queue.defaultTimeout`) : Timeout of the job. If the job haven't finished after the timeout, it is considered finished. If the timeout is less than or equal to 0, it is not taken into consideration.
* retry (default:`queue.defaultRetry`) : Number of retry for the job if it call the callback with an error
* retryDelay (default:`queue.defaultRetryDelay`) : Delay in ms between two retry

#### Usage

```js
job.setPriority(priority)   // Set priority of the job
job.save(function(err))     // Save job in redis, it is ready to be processed
job.remove(function(err))   // Remove job from pending list
```

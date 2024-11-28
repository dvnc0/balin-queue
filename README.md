# Balin Queue
A simple database queue for PHP.

Supports:
- MySQL/MariaDB
- SQLite3

## Installation
```bash
composer require danc0/balin-queue
```

## Config
```php
$Balin = Balin::load([
	'path' => __DIR__,
	'database' => [
		'driver' => 'sqlite',
		'name' => 'balin_queue.sqlite',
		'dsn' => 'sqlite:' . __DIR__ . '/balin_queue.sqlite'
	]
]);
```

Driver Options:
- `sqlite`
- `mysql`

A flag file is created in `path` to indicate that the queue has been created, make sure the path is writable. This is also where the sqlite database will be created if using sqlite.

## Usage
You would first create an instance of Balin somewhere with the config example above. To access the instance of Balin you would use the following code:

```php
$Balin = Balin::queue();
```

At this point you have a number of public methods you can use.

### Push to queue
`push(string $task_name, array $payload, int $priority = 99, int $max_attempts = 3, string $scheduled_at = null)`

This will push a task to the queue. Use lower numbers for higher priority. The default priority is 99. The default max attempts is 3. The default scheduled at is null. Using scheduled at will delay the task until the time specified.

### Getting a job
`pop()` 

This will return the next job in the queue. If there are no jobs in the queue it will return `null`. This will pull based on priority, scheduled at time, and created time.

`popTask(string $task_name)`

This will return the next job in the queue for the specified task name. If there are no jobs in the queue it will return `null`. This will pull based on priority, scheduled at time, and created time, but only for the specified task name.

### Update a job
`success(int $id)`

This will mark the job as successful and remove it from the queue.

`failure(int $id, string|null $scheduled_at = null)`

This will mark the job as failed and put it back in the queue. If the job has reached the max attempts it will be removed from the queue. If you pass a scheduled at time it will delay the job until that time.

`error(int $id, string $error_message)`

This will mark the job as having an error and remove it from the queue.

## Releasing orphaned jobs
`releaseLockedJobs(int $locked_max_time = 3600)`

This will release any jobs that have been locked for more than the specified time. The default is 3600 seconds (1 hour).
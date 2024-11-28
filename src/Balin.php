<?php
declare(strict_types=1);

namespace Balin;

use Balin\Database\Database_Interface;
use Balin\Exceptions\Balin_Exception;
use Balin\Database\Pdo_Database;
use Balin\Utilities\File;
use Balin\Workers\Mysql_Worker;
use Balin\Workers\Sqlite_Worker;
use Balin\Workers\Worker_Interface;

class Balin {
	/**
	 * The instance of Balin
	 *
	 * @var Balin
	 */
	protected static $instance;

	/**
	 * The configuration of Balin
	 *
	 * @var array
	 */
	protected array $config = [
		'path' => __DIR__ . '/data/balin',
		'flag_file' => 'balin.flag',
		'database' => [
			'driver' => 'sqlite',
			'name' => 'balin_queue.sqlite'
		]
	];

	/**
	 * The database instance
	 *
	 * @var Database_Interface
	 */
	protected Database_Interface $database;

	/**
	 * The worker instance
	 *
	 * @var Worker_Interface
	 */
	protected Worker_Interface $worker;

	/**
	 * Prevent the instance from being cloned
	 * 
	 * @return void
	 */
	protected function __clone(): void { }

	/**
	 * Prevent from being unserialized
	 * 
	 * @return void
	 */
	public function __wakeup(): void {
		throw new Balin_Exception('Cannot unserialize a Balin instance.');
	}

	/**
	 * Prevent from being constructed
	 * 
	 * @param array $config The configuration of Balin
	 */
	protected function __construct(array $config = []) {
		$this->initializeBalin($config);
		return;
	}

	/**
	 * Get the instance of Balin
	 * 
	 * @param array $settings The settings of Balin
	 * @return Balin
	 */
	public static function load(array $config = []): Balin {
		if (null === static::$instance) {
			static::$instance = new self($config);
		}
		return static::$instance;
	}

	/**
	 * Get the instance of Balin
	 * 
	 * @return Balin
	 */
	public static function queue(): Balin {
		if (null === static::$instance) {
			throw new Balin_Exception('Balin is not initialized');
		}
		return static::$instance;
	}

	/**
	 * Initialize Balin
	 * 
	 * @param array $config The configuration of Balin
	 * 
	 * @return void
	 */
	protected function initializeBalin(array $config): void {
		$this->config = array_merge($this->config, $config);
		$this->config['path'] = rtrim($this->config['path'], '/');
		$flag_path = $this->config['path'] . '/' . $this->config['flag_file'];

		$File = new File;
		$this->database = $this->getDatabaseInstance();
		$this->worker = $this->getWorkerInstance();

		if ($File->exists($flag_path) === false) {
			$this->worker->createDatabase();
			$File->putContents($flag_path, date('Y-m-d::H:i:s'));
		}
		return;
	}

	/**
	 * Get the database instance
	 * 
	 * @return Database_Interface
	 */
	protected function getDatabaseInstance(): Database_Interface {
		return match($this->config['database']['driver']) {
			'sqlite' => new Pdo_Database($this->config['database']['dsn']),
			'mysql' => new Pdo_Database($this->config['database']['dsn']),
			default => throw new Balin_Exception('Invalid database driver')
		};
	}

	/**
	 * Get the worker instance
	 * 
	 * @return Worker_Interface
	 */
	protected function getWorkerInstance(): Worker_Interface {
		return match($this->config['database']['driver']) {
			'sqlite' => new Sqlite_Worker($this->database),
			'mysql' => new Mysql_Worker($this->database),
			default => throw new Balin_Exception('Invalid database driver')
		};
	}

	/**
	 * Insert a task into the queue
	 * 
	 * @param string $task_name The name of the task
	 * @param array $payload The payload of the task
	 * @param int $priority The priority of the task
	 * @param int $max_attempts The maximum attempts of the task
	 * @param string $scheduled_at The scheduled time of the task
	 * 
	 * @return void
	 */
	public function push(string $task_name, array $payload, int $priority = 99, int $max_attempts = 3, string $scheduled_at = null): void {
		$this->worker->insertJob($task_name, $payload, $priority, $max_attempts, $scheduled_at);
	}

	/**
	 * Get the next record
	 * 
	 * @return array|null
	 */
	public function pop(): array|null {
		$job = $this->worker->getNextJob();
		return $job;
	}

	/**
	 * Get the next task
	 * 
	 * @param string $task_name The name of the task
	 * 
	 * @return array|null
	 */
	public function popTask(string $task_name): array|null {
		$job = $this->worker->getNextTask($task_name);
		return $job;
	}

	/**
	 * Job success
	 * 
	 * @param int $id The id of the job
	 * 
	 * @return void
	 */
	public function success(int $id): void {
		$this->worker->jobSuccess($id);
		return;
	}

	/**
	 * Job failure
	 * 
	 * @param int $id The id of the job
	 * @param string|null $scheduled_at The scheduled time of the job
	 * 
	 * @return void
	 */
	public function failure(int $id, string|null $scheduled_at = null): void {
		$this->worker->jobFailure($id, $scheduled_at);
		return;
	}

	/**
	 * Job error
	 * 
	 * @param int $id The id of the job
	 * @param string $error_message The error message of the job
	 * 
	 * @return void
	 */
	public function error(int $id, string $error_message): void {
		$this->worker->jobError($id, $error_message);
		return;
	}

	/**
	 * Release locked jobs
	 * 
	 * @param int $locked_max_time The maximum time of the locked job
	 * 
	 * @return void
	 */
	public function releaseLockedJobs(int $locked_max_time = 3600): void {
		$this->worker->releaseLockedJobs($locked_max_time);
		return;
	}
}
<?php
declare(strict_types=1);

namespace Balin;

use Balin\Database\Database_Interface;
use Balin\Exceptions\Balin_Exception;
use Balin\Database\Sqlite_Database;

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
	protected array $config = [];

	/**
	 * The database instance
	 *
	 * @var Database_Interface
	 */
	protected Database_Interface $database;

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
	 */
	protected function __construct(array $config = []) {
		$this->initializeBalin($config);
		return;
	}

	/**
	 * Get the instance of Buoy
	 * 
	 * @param array $settings The settings of Buoy
	 * @return Balin
	 */
	public static function load(array $config = []): Balin {
		if (NULL === static::$instance) {
			static::$instance = new self($config);
		}
		return static::$instance;
	}

	/**
	 * Initialize Balin
	 * 
	 * @return void
	 */
	protected function initializeBalin(array $config): void {
		$default_config = [
			'path' => __DIR__ . '/data/balin',
			'flag_file' => 'balin.flag',
			'database' => [
				'driver' => 'sqlite',
				'name' => 'balin_queue.sqlite'
			]
		];

		$this->config = array_merge($default_config, $config);
		$this->config['path'] = rtrim($this->config['path'], '/');
		$flag_path = $this->config['path'] . '/' . $this->config['flag_file'];

		$this->database = new Sqlite_Database($this->config['path'], $this->config['database']['name']);

		if (file_exists($flag_path) === false) {
			$sql = <<<SQL
			CREATE TABLE balin_queue (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				task_name TEXT NOT NULL,
				payload TEXT NOT NULL,
				status TEXT NOT NULL DEFAULT 'pending',
				priority INTEGER DEFAULT 0,
				attempts INTEGER DEFAULT 0,
				max_attempts INTEGER DEFAULT 3,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
				updated_at DATETIME,
				scheduled_at DATETIME,
				worker_id TEXT,
				error_message TEXT
			);
			SQL;

			$this->database->query($sql);

			file_put_contents($flag_path, date('Y-m-d::H:i:s'));
		}
		return;
	}

}
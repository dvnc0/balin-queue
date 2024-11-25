<?php
declare(strict_types=1);

namespace Balin;

use Balin\Database\Database_Interface;
use Balin\Exceptions\Balin_Exception;
use Balin\Database\Sqlite_Database;
use Balin\Utilities\File;

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
		if (NULL === static::$instance) {
			static::$instance = new self($config);
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

		if ($File->exists($flag_path) === false) {
			$sql = <<<SQL
			CREATE TABLE IF NOT EXISTS balin_queue (
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
			'sqlite' => new Sqlite_Database($this->config['path'], $this->config['database']['name']),
			default => throw new Balin_Exception('Invalid database driver')
		};
	}

}
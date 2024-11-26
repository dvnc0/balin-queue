<?php
declare(strict_types=1);

namespace Balin\Workers;

use Balin\Database\Pdo_Database;
use Exception;
use PDO;

class Sqlite_Worker implements Worker_Interface {
	/**
	 * The database instance
	 *
	 * @var Pdo_Database
	 */
	protected Pdo_Database $database;

	/**
	 * Construct the worker
	 * 
	 * @param Pdo_Database $database The database instance
	 */
	public function __construct(Pdo_Database $database) {
		$this->database = $database;
	}

	public function createDatabase(): void {
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
			error_message TEXT,
			locked INT DEFAULT 0,
			is_active INT DEFAULT 1
		);

		CREATE INDEX idx_status_locked_active ON balin_queue (status, locked, is_active);
		CREATE INDEX idx_priority_schedule_created ON balin_queue (priority DESC, scheduled_at ASC, created_at ASC);
		
		SQL;

		$this->database->query($sql);

		return;
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
	public function insertJob(string $task_name, array $payload, int $priority = 0, int $max_attempts = 3, string $scheduled_at = NULL): void {
		$sql = <<<SQL
		INSERT INTO balin_queue (task_name, payload, priority, max_attempts, scheduled_at)
		VALUES (:task_name, :payload, :priority, :max_attempts, :scheduled_at);
		SQL;

		$this->database->query($sql, [
			':task_name' => $task_name,
			':payload' => serialize(json_encode($payload)),
			':priority' => $priority,
			':max_attempts' => $max_attempts,
			':scheduled_at' => $scheduled_at
		]);
		return;
	}

	/**
	 * Get the next record
	 * 
	 * @return array|null
	 */
	public function getNextJob(): array|null {
		$pdo = $this->database->getConnection();
		$worker_id = getmypid() . '_' . uniqid();
		try {
			$pdo->beginTransaction();
			
			$sql = <<<SQL
			SELECT id, task_name, payload, status, priority, attempts, max_attempts, created_at, updated_at, scheduled_at
			FROM balin_queue
			WHERE
				status = 'pending'
				AND locked = 0
				AND is_active = 1
				AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
				AND (attempts < max_attempts OR max_attempts = 0)
			ORDER BY priority DESC, scheduled_at ASC, created_at ASC
			LIMIT 1
			SQL;

			$select_stmt = $pdo->prepare($sql);
			$select_stmt->execute();
			$task = $select_stmt->fetch(PDO::FETCH_ASSOC);

			// no task found
			if ($task === false) {
				$pdo->rollBack();
				return null;
			}

			// lock task
			$update_sql = <<<SQL
			UPDATE balin_queue
			SET
				status = 'processing',
				locked = 1,
				worker_id = :worker_id,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = :task_id
			SQL;

			$updateStmt = $pdo->prepare($update_sql);
			$updateStmt->execute([
				'worker_id' => $worker_id,
				'task_id'   => $task['id'],
			]);

			// commit
			$pdo->commit();

			$task['payload'] = json_decode(unserialize($task['payload']), true);
			
			return $task;
		} catch (Exception $e) {
			$pdo->rollBack();
			throw $e;
		}
	}

	public function jobSuccess(int $id): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = 'success',
			locked = 0,
			worker_id = NULL,
			updated_at = CURRENT_TIMESTAMP,
			is_active = 0
		WHERE id = :id
		SQL;

		$this->database->query($sql, [':id' => $id]);
		return;
	}

	public function jobFailure(int $id): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = CASE
				WHEN (attempts + 1) >= max_attempts THEN 'failed'
				ELSE 'pending'
			END,
			locked = 0,
			worker_id = NULL,
			updated_at = CURRENT_TIMESTAMP,
			attempts = (attempts + 1)
		WHERE id = :id;
		SQL;

		$this->database->query($sql, [':id' => $id]);
		return;
	}

	public function jobError(int $id, string $error_message): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = 'error',
			locked = 0,
			worker_id = NULL,
			updated_at = CURRENT_TIMESTAMP,
			error_message = :error_message
		WHERE id = :id
		SQL;

		$this->database->query($sql, [':id' => $id, ':error_message' => $error_message]);
		return;
	}

	public function releaseLockedJobs(int $locked_max_time = 3600): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = 'pending',
			locked = 0,
			worker_id = NULL,
			updated_at = CURRENT_TIMESTAMP
		WHERE
			locked = 1
			AND updated_at < (CURRENT_TIMESTAMP - :locked_max_time)
		SQL;

		$this->database->query($sql, [':locked_max_time' => $locked_max_time]);
		return;
	}
}
<?php
declare(strict_types=1);

namespace Balin\Workers;

use Balin\Database\Pdo_Database;
use Exception;
use PDO;

class Mysql_Worker implements Worker_Interface {
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

	/**
	 * Create the database
	 * 
	 * @return void
	 */
	public function createDatabase(): void {
		$sql = <<<SQL
		CREATE TABLE IF NOT EXISTS balin_queue (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			task_name VARCHAR(255) NOT NULL,
			payload TEXT NOT NULL,
			status ENUM('pending', 'processing', 'failed', 'success', 'error') NOT NULL DEFAULT 'pending',
			priority INT DEFAULT 99,
			attempts INT DEFAULT 0,
			max_attempts INT DEFAULT 3,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
			scheduled_at DATETIME DEFAULT NULL,
			worker_id VARCHAR(255) DEFAULT NULL,
			error_message TEXT DEFAULT NULL,
			locked TINYINT(1) DEFAULT 0,
			is_active TINYINT(1) DEFAULT 1
		);

		CREATE INDEX idx_task_name ON balin_queue (task_name);
		CREATE INDEX idx_status_locked_active ON balin_queue (status, locked, is_active);
		CREATE INDEX idx_priority_schedule_created ON balin_queue (priority ASC, scheduled_at ASC, created_at ASC);

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
	public function insertJob(string $task_name, array $payload, int $priority = 99, int $max_attempts = 3, string $scheduled_at = NULL): void {
		$sql = <<<SQL
		INSERT INTO balin_queue (task_name, payload, priority, max_attempts, scheduled_at)
		VALUES (:task_name, :payload, :priority, :max_attempts, :scheduled_at);
		SQL;

		$this->database->query($sql, [
			':task_name' => $task_name,
			':payload' => serialize(json_encode($payload)),
			':priority' => $priority,
			':max_attempts' => $max_attempts,
			':scheduled_at' => $scheduled_at ?? date('Y-m-d H:i:s'),
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
			ORDER BY priority ASC, scheduled_at ASC, created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
			SQL;

			$select_stmt = $pdo->prepare($sql);
			$select_stmt->execute();
			$task = $select_stmt->fetch(PDO::FETCH_ASSOC);

			if ($task === false) {
				$pdo->rollBack();
				return null;
			}

			$update_sql = <<<SQL
			UPDATE balin_queue
			SET
				status = 'processing',
				locked = 1,
				worker_id = :worker_id,
			WHERE id = :task_id
			SQL;

			$update_stmt = $pdo->prepare($update_sql);
			$update_stmt->execute([
				'worker_id' => $worker_id,
				'task_id'   => $task['id'],
			]);

			$pdo->commit();
			$task['payload'] = json_decode(unserialize($task['payload']), true);
			return $task;

		} catch (Exception $e) {
			$pdo->rollBack();
			throw $e;
		}

	}

	/**
	 * Get the job by task name
	 * 
	 * @param string $task_name The name of the task
	 * 
	 * @return array|null
	 */
	public function getNextTask(string $task_name): array|null {
		$pdo = $this->database->getConnection();
		$worker_id = getmypid() . '_' . uniqid();
		try {
			$pdo->beginTransaction();
			
			$sql = <<<SQL
			SELECT id, task_name, payload, status, priority, attempts, max_attempts, created_at, updated_at, scheduled_at
			FROM balin_queue
			WHERE
				task_name = :task_name
				status = 'pending'
				AND locked = 0
				AND is_active = 1
				AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
				AND (attempts < max_attempts OR max_attempts = 0)
			ORDER BY priority ASC, scheduled_at ASC, created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
			SQL;

			$select_stmt = $pdo->prepare($sql);
			$select_stmt->execute([':task_name' => $task_name]);
			$task = $select_stmt->fetch(PDO::FETCH_ASSOC);

			if ($task === false) {
				$pdo->rollBack();
				return null;
			}

			$update_sql = <<<SQL
			UPDATE balin_queue
			SET
				status = 'processing',
				locked = 1,
				worker_id = :worker_id,
			WHERE id = :task_id
			SQL;

			$update_stmt = $pdo->prepare($update_sql);
			$update_stmt->execute([
				'worker_id' => $worker_id,
				'task_id'   => $task['id'],
			]);

			$pdo->commit();
			$task['payload'] = json_decode(unserialize($task['payload']), true);
			return $task;

		} catch (Exception $e) {
			$pdo->rollBack();
			throw $e;
		}
	}

	/**
	 * Mark the job as success
	 *
	 * @param  integer $id
	 * @return void
	 */
	public function jobSuccess(int $id): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = 'success',
			locked = 0,
			worker_id = NULL,
			is_active = 0
		WHERE id = :id
		SQL;

		$this->database->query($sql, [':id' => $id]);
		return;
	}

	/**
	 * Mark the job as failure
	 *
	 * @param  integer $
	 * @param  string|null $scheduled_at
	 * @return void
	 */
	public function jobFailure(int $id, string|null $scheduled_at): void {
		$retry_at = $scheduled_at ?? date('Y-m-d H:i:s');
		$scheduled_for = "scheduled_at = '$retry_at'";
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = CASE
				WHEN max_attempts = 0 THEN 'pending'
				WHEN (attempts + 1) >= max_attempts THEN 'failed'
				ELSE 'pending'
			END,
			locked = 0,
			worker_id = NULL,
			attempts = (attempts + 1),
			$scheduled_for
		WHERE id = :id;
		SQL;

		$this->database->query($sql, [':id' => $id]);
		return;
	}

	/**
	 * Mark the job as error
	 *
	 * @param  integer $id
	 * @param  string $error_message
	 * @return void
	 */
	public function jobError(int $id, string $error_message): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = 'error',
			locked = 0,
			worker_id = NULL,
			error_message = :error_message
		WHERE id = :id
		SQL;

		$this->database->query($sql, [':id' => $id, ':error_message' => $error_message]);
		return;
	}

	/**
	 * Release the locked jobs
	 *
	 * @param  integer $locked_max_time
	 * @return void
	 */
	public function releaseLockedJobs(int $locked_max_time = 3600): void {
		$sql = <<<SQL
		UPDATE balin_queue
		SET
			status = 'pending',
			locked = 0,
			worker_id = NULL,
		WHERE locked = 1
			AND updated_at < (CURRENT_TIMESTAMP - :locked_max_time)
		SQL;

		$this->database->query($sql, [':locked_max_time' => $locked_max_time]);
		return;
	}
}
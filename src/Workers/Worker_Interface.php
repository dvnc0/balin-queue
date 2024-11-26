<?php
declare(strict_types=1);

namespace Balin\Workers;

interface Worker_Interface {

	/**
	 * Create the database
	 * 
	 * @return void
	 */
	public function createDatabase(): void;

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
	public function insertJob(string $task_name, array $payload, int $priority = 0, int $max_attempts = 3, string $scheduled_at = NULL): void;

	/**
	 * Get the next record
	 * 
	 * @return array|null
	 */
	public function getNextJob(): array|null;

	/**
	 * Mark a job as successful
	 *
	 * @param  integer $id
	 * @return void
	 */
	public function jobSuccess(int $id): void;

	/**
	 * Mark a job as failed
	 *
	 * @param  integer $id
	 * @return void
	 */
	public function jobFailure(int $id): void;

	/**
	 * Mark a job as errored
	 *
	 * @param  integer $id
	 * @param  string $error_message
	 * @return void
	 */
	public function jobError(int $id, string $error_message): void;

	/**
	 * Release locked jobs
	 *
	 * @param  integer $locked_max_time
	 * @return void
	 */
	public function releaseLockedJobs(int $locked_max_time = 3600): void;
}
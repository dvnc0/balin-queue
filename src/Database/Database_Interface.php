<?php
declare(strict_types=1);

namespace Balin\Database;

use PDO;
use PDOStatement;

interface Database_Interface {
	/**
	 * Connect to the database
	 * 
	 * @return void
	 */
	public function connect(string $dsn): void;

	/**
	 * Execute a query
	 * 
	 * @param string $query The query to execute
	 * @param array $params The parameters to bind
	 * @return PDOStatement
	 */
	public function query(string $query, array $params = []): PDOStatement;

	/**
	 * Fetch a single row
	 * 
	 * @param string $query The query to execute
	 * @param array $params The parameters to bind
	 * @return array
	 */
	public function fetch(string $query, array $params = []): array;

	/**
	 * Fetch all rows
	 * 
	 * @param string $query The query to execute
	 * @param array $params The parameters to bind
	 * @return array
	 */
	public function fetchAll(string $query, array $params = []): array;

	/**
	 * get the last insert id
	 *
	 * @return string|bool
	 */
	public function lastInsertId(): string|bool;

	/**
	 * Get the connection
	 * 
	 * @return PDO
	 */
	public function getConnection(): PDO;

	/**
	 * Close the connection
	 * 
	 * @return void
	 */
	public function close(): void;
}
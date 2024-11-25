<?php
declare(strict_types=1);

namespace Balin\Database;

use Balin\Exceptions\Balin_Exception;
use Balin\Database\Database_Interface;
use PDO;
use PDOException;
use PDOStatement;

/**
 * The database class
 */
class Sqlite_Database implements Database_Interface {
	protected $connection;
	public function __construct(string $database_path, string $database_name) {
		$this->connect($database_path, $database_name);
	}

	/**
	 * Connect to the database
	 * 
	 * @return void
	 */
	public function connect($database_path, $database_name): void {
		try {
			$pdo = new PDO("sqlite:" . $database_path . '/' . $database_name);

			$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
			$this->connection = $pdo;
		} catch (PDOException $e) {
			throw new Balin_Exception($e->getMessage());
		}
	}

	/**
	 * Execute a query
	 * 
	 * @param string $query The query to execute
	 * @param array $params The parameters to bind
	 * @return PDOStatement
	 */
	public function query(string $query, array $params = []): PDOStatement {
		$statement = $this->connection->prepare($query);
		$statement->execute($params);
		return $statement;
	}

	/**
	 * Fetch a single row
	 * 
	 * @param string $query The query to execute
	 * @param array $params The parameters to bind
	 * @return array
	 */
	public function fetch(string $query, array $params = []): array {
		$statement = $this->query($query, $params);
		return $statement->fetch(PDO::FETCH_ASSOC);
	}

	/**
	 * Fetch all rows
	 * 
	 * @param string $query The query to execute
	 * @param array $params The parameters to bind
	 * @return array
	 */
	public function fetchAll(string $query, array $params = []): array {
		$statement = $this->query($query, $params);
		return $statement->fetchAll(PDO::FETCH_ASSOC);
	}

	/**
	 * Get the last inserted id
	 * 
	 * @return int
	 */
	public function lastInsertId(): int {
		return $this->connection->lastInsertId();
	}

	/**
	 * Get the connection
	 * 
	 * @return PDO
	 */
	public function getConnection(): PDO {
		return $this->connection;
	}

	/**
	 * Close the connection
	 * 
	 * @return void
	 */
	public function close(): void {
		$this->connection = NULL;
	}

	/**
	 * Prevent from being serialized
	 * 
	 * @return array
	 */
	public function __sleep(): array {
		throw new Balin_Exception('Cannot serialize a Database instance.');
	}

	/**
	 * Prevent from being unserialized
	 * 
	 * @return void
	 */
	public function __wakeup(): void {
		throw new Balin_Exception('Cannot unserialize a Database instance.');
	}

	/**
	 * Prevent from being cloned
	 * 
	 * @return void
	 */
	public function __clone(): void {
		throw new Balin_Exception('Cannot clone a Database instance.');
	}
}
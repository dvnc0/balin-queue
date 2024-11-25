<?php
declare(strict_types=1);

namespace Balin\Utilities;

use Balin\Exceptions\Balin_Exception;

class File {
	public function putContents(string $filename, string $data): void {
		if (file_put_contents($filename, $data) === false) {
			throw new Balin_Exception('Failed to write to file: ' . $filename);
		}
	}

	public function exists(string $filename): bool {
		return file_exists($filename);
	}
}
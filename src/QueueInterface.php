<?php

namespace Zoon\ReQueue;

interface QueueInterface {

	public function update(string $id, callable $updateCallback, int $retryLimit = 1000): void;
	public function push(MessageInterface $message): void;
	public function pop(?TimestampRangeInterface $timestampRange = null, int $retryLimit = 1000): ?MessageInterface;
	public function clear(?TimestampRangeInterface $timestampRange = null): void;
	public function count(?TimestampRangeInterface $timestampRange = null): int;

}
<?php

namespace Zoon\ReQueue;

interface RedisAdapterInterface {

	public function multi(): void;
	public function pipeline(): void;
	public function set(string $key, string $value): void;
	public function zAdd(string $key, string $score, string $value): void;
	public function exec(): bool;
	public function watch(string $key): void;
	public function zRangeByScope(string $key, ?TimestampRangeInterface $timestampRange, int $limit = 1): array;
	public function zRemRangeByScope(string $key, ?TimestampRangeInterface $timestampRange): void;
	public function unwatch(): void;
	public function del(string $key): void;
	public function zRem(string $key, string $member): void;
	public function zScore(string $key, string $member): ?float;
	public function get(string $key): ?string;
	public function zCount(string $key, ?TimestampRangeInterface $timestampRange): int;

}
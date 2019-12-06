<?php

namespace Zoon\ReQueue;

use Zoon\ReQueue\Exception\NotAtomicRedisModeException;

class RedisAdapter implements RedisAdapterInterface {

	private $redis;

	/**
	 * RedisAdapter constructor.
	 * @param \Redis $redis
	 */
	public function __construct(\Redis $redis) {
		$this->redis = $redis;
	}

	public function multi(): void {
		$this->redis->multi(\Redis::MULTI);
	}

	public function pipeline(): void {
		$this->redis->multi(\Redis::PIPELINE);
	}

	/**
	 * @param string $key
	 * @param string $value
	 */
	public function set(string $key, string $value): void {
		$this->redis->set($key, $value);
	}

	/**
	 * @param string $key
	 * @param string $score
	 * @param string $value
	 */
	public function zAdd(string $key, string $score, string $value): void {
		$this->redis->zAdd($key, [], $score, $value);
	}

	/**
	 * @return bool
	 */
	public function exec(): bool {
		return $this->redis->exec() !== false;
	}

	/**
	 * @param string $key
	 */
	public function watch(string $key): void {
		$this->redis->watch($key);
	}

	/**
	 * @param string $key
	 * @param TimestampRangeInterface|null $timestampRange
	 * @param int $limit
	 * @return array
	 * @throws NotAtomicRedisModeException
	 */
	public function zRangeByScope(string $key, ?TimestampRangeInterface $timestampRange, int $limit = 1): array {
		$this->validateAtomicRedisMode();
		return $this->redis->zRangeByScore(
			$key,
			self::getMinForRedis(($timestampRange !== null ? $timestampRange->getMin() : null)),
			self::getMaxForRedis(($timestampRange !== null ? $timestampRange->getMax() : null)),
			['limit' => [0, $limit]]
		);
	}

	/**
	 * @param string $key
	 * @param TimestampRangeInterface|null $timestampRange
	 */
	public function zRemRangeByScope(string $key, ?TimestampRangeInterface $timestampRange): void {
		$this->redis->zRemRangeByScore(
			$key,
			self::getMinForRedis(($timestampRange !== null ? $timestampRange->getMin() : null)),
			self::getMaxForRedis(($timestampRange !== null ? $timestampRange->getMax() : null))
		);
	}

	public function unwatch(): void {
		$this->redis->unwatch();
	}

	/**
	 * @param string $key
	 */
	public function del(string $key): void {
		$this->redis->del($key);
	}

	/**
	 * @param string $key
	 * @param string $member
	 */
	public function zRem(string $key, string $member): void {
		$this->redis->zRem($key, $member);
	}

	/**
	 * @param string $key
	 * @param string $member
	 * @return float|null
	 * @throws NotAtomicRedisModeException
	 */
	public function zScore(string $key, string $member): ?float {
		$this->validateAtomicRedisMode();
		$score = $this->redis->zScore($key, $member);
		if ($score === false) {
			return null;
		}
		return $score;
	}

	/**
	 * @param string $key
	 * @return string|null
	 * @throws NotAtomicRedisModeException
	 */
	public function get(string $key): ?string {
		$this->validateAtomicRedisMode();
		$value = $this->redis->get($key);
		if ($value === false) {
			return null;
		}
		return $value;
	}

	/**
	 * @param string $key
	 * @param TimestampRangeInterface|null $timestampRange
	 * @return int
	 * @throws NotAtomicRedisModeException
	 */
	public function zCount(string $key, ?TimestampRangeInterface $timestampRange): int {
		$this->validateAtomicRedisMode();
		return $this->redis->zCount(
			$key,
			self::getMinForRedis(($timestampRange !== null ? $timestampRange->getMin() : null)),
			self::getMaxForRedis(($timestampRange !== null ? $timestampRange->getMax() : null))
		);
	}

	/**
	 * @param int|null $min
	 * @return string
	 */
	private static function getMinForRedis(?int $min): string {
		return $min ?? '-inf';
	}

	/**
	 * @param int|null $max
	 * @return string
	 */
	private static function getMaxForRedis(?int $max): string {
		return $max ?? '+inf';
	}

	/**
	 * @throws NotAtomicRedisModeException
	 */
	private function validateAtomicRedisMode(): void {
		if ($this->redis->getMode() !== \Redis::ATOMIC) {
			throw new NotAtomicRedisModeException();
		}
	}

}
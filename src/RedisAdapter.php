<?php

namespace Zoon\ReQueue;

use Redis;
use Zoon\ReQueue\Exception\NotAtomicRedisModeException;

class RedisAdapter implements RedisAdapterInterface {

	private Redis $redis;

	/**
	 * RedisAdapter constructor.
	 * @param \Redis $redis
	 */
	public function __construct(Redis $redis) {
		$this->redis = $redis;
	}

	public function multi(): void {
		$this->redis->multi(Redis::MULTI);
	}

	public function pipeline(): void {
		$this->redis->multi(Redis::PIPELINE);
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
	public function zRangeByScore(string $key, ?TimestampRangeInterface $timestampRange, int $limit = 1): array {
		$this->validateAtomicRedisMode();
		return $this->redis->zRangeByScore(
			$key,
			self::getMinForRedis($timestampRange?->getMin()),
			self::getMaxForRedis($timestampRange?->getMax()),
			['limit' => [0, $limit]],
		);
	}

	/**
	 * @param string $key
	 * @param TimestampRangeInterface|null $timestampRange
	 */
	public function zRemRangeByScore(string $key, ?TimestampRangeInterface $timestampRange): void {
		$this->redis->zRemRangeByScore(
			$key,
			self::getMinForRedis($timestampRange?->getMin()),
			self::getMaxForRedis($timestampRange?->getMax()),
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
		$res = $this->redis->zRem($key, $member);
		if ($res === 0) {
			$was = $this->compressionFix();
			$this->redis->zRem($key, $member);
			$this->compressionFix($was);
		}
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
			$was = $this->compressionFix();
			$score = $this->redis->zScore($key, $member);
			$this->compressionFix($was);
		}
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
		if (!$value || unserialize($value, ['allowed_classes' => true]) === false) {
			$was = $this->compressionFix();
			$value = $this->redis->get($key);
			$this->compressionFix($was);
		}
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
			self::getMinForRedis($timestampRange?->getMin()),
			self::getMaxForRedis($timestampRange?->getMax()),
		);
	}

	/**
	 * @param int|null $min
	 * @return string
	 */
	private static function getMinForRedis(?int $min): int|string {
		return $min ?? '-inf';
	}

	/**
	 * @param int|null $max
	 * @return string
	 */
	private static function getMaxForRedis(?int $max): int|string {
		return $max ?? '+inf';
	}

	/**
	 * @throws NotAtomicRedisModeException
	 */
	private function validateAtomicRedisMode(): void {
		if ($this->redis->getMode() !== Redis::ATOMIC) {
			throw new NotAtomicRedisModeException();
		}
	}

	private function compressionFix(?int $mode = null): int {
		$was = $this->redis->getOption(Redis::OPT_COMPRESSION);
		if ($mode !== null) {
			$this->redis->setOption(Redis::OPT_COMPRESSION, $mode);
		} else {
			$this->redis->setOption(Redis::OPT_COMPRESSION, match ($was) {
				Redis::COMPRESSION_ZSTD => Redis::COMPRESSION_NONE,
				Redis::COMPRESSION_NONE => Redis::COMPRESSION_ZSTD,
			});
		}
		return $was;
	}
}

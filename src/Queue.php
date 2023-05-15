<?php

namespace Zoon\ReQueue;

use Zoon\ReQueue\Exception\InvalidRetryLimitException;
use Zoon\ReQueue\Exception\InvalidUpdateCallbackException;
use Zoon\ReQueue\Exception\PushException;
use Zoon\ReQueue\Exception\RetryLimitException;

final class Queue {

	private const DEFAULT_DATA_KEY_PREFIX = 'dmq:data:';
	private const DEFAULT_TIMESTAMP_INDEX_KEY = 'dmq:tsIndex';
	private const CLEAR_BUFFER_SIZE = 1000;

	public function __construct(
		private RedisAdapter $client,
		private string $dataKeyPrefix = self::DEFAULT_DATA_KEY_PREFIX,
		private string $timestampIndexKey = self::DEFAULT_TIMESTAMP_INDEX_KEY
	) {
	}

	/**
	 * @param Message $message
	 * @throws PushException
	 */
	public function push(Message $message): void {
		if ($this->tryPush($message) === false) {
			throw new PushException();
		}
	}

	private function tryPush(Message $message): bool|array {
		$this->client->multi();
		$this->client->set($this->getDataKey($message->getId()), $message->getData());
		$this->client->zAdd($this->timestampIndexKey, $message->getTimestamp(), $message->getId());
		return $this->client->exec();
	}

	/**
	 * @param string $id
	 * @param callable $updateCallback
	 * @param int $retryLimit
	 * @throws InvalidRetryLimitException
	 * @throws InvalidUpdateCallbackException
	 * @throws RetryLimitException
	 */
	public function update(string $id, callable $updateCallback, int $retryLimit = 1000): void {
		self::validateRetryLimit($retryLimit);
		$retries = 0;
		while ($retries++ <= $retryLimit) {
			$this->client->watch($this->getDataKey($id));
			/** @var Message $updatedMessage */
			$updatedMessage = $updateCallback($this->getMessage($id));
			if (!($updatedMessage instanceof Message)) {
				throw new InvalidUpdateCallbackException();
			}
			if ($this->tryPush(new Message($id, $updatedMessage->getTimestamp(), $updatedMessage->getData())) === false) {
				continue;
			}
			return;
		}
		throw new RetryLimitException();
	}

	/**
	 * @throws InvalidRetryLimitException
	 */
	public function pop(TimestampRange $timestampRange = new TimestampRange(), int $retryLimit = 1000): ?Message {
		self::validateRetryLimit($retryLimit);
		$retries = 0;
		while ($retries++ <= $retryLimit) {
			$idInArray = $this->client->zRangeByScore(
				$this->timestampIndexKey,
				$timestampRange
			);
			if (count($idInArray) === 0) {
				return null;
			}
			$id = $idInArray[0];
			$dataKey = $this->getDataKey($id);
			$this->client->watch($dataKey);
			$message = $this->getMessage($id);
			if ($message === null) {
				$this->client->unwatch();
				continue;
			}

			$this->client->multi();
			$this->client->del($dataKey);
			$this->client->zRem($this->timestampIndexKey, $id);
			if ($this->client->exec() === false) {
				continue;
			}
			return new Message($id, $message->getTimestamp(), $message->getData());
		}
		return null;
	}

	private function getMessage(string $id): ?Message {
		$timestamp = $this->client->zScore($this->timestampIndexKey, $id);
		if ($timestamp === null) {
			return null;
		}
		$data = $this->client->get($this->getDataKey($id));
		if ($data === null) {
			return null;
		}
		return new Message($id, $timestamp, $data);
	}

	public function count(TimestampRange $timestampRange = new TimestampRange()): int {
		return $this->client->zCount(
			$this->timestampIndexKey,
			$timestampRange
		);
	}

	public function clear(TimestampRange $timestampRange = new TimestampRange()): void {
		while (true) {
			$list = $this->client->zRangeByScore($this->timestampIndexKey, $timestampRange, self::CLEAR_BUFFER_SIZE);
			if (count($list) === 0) {
				return;
			}

			$this->client->pipeline();
			foreach ($list as $id) {
				$this->client->watch($this->getDataKey($id));
			}
			$this->client->multi();
			$this->client->zRemRangeByScore($this->timestampIndexKey, $timestampRange);
			foreach ($list as $id) {
				$this->client->del($this->getDataKey($id));
			}
			$this->client->exec();
			$this->client->exec();
		}
	}

	/**
	 * @param $id
	 * @return string
	 */
	private function getDataKey($id): string {
		return $this->dataKeyPrefix . $id;
	}

	/**
	 * @param int $retryLimit
	 * @throws InvalidRetryLimitException
	 */
	private static function validateRetryLimit(int $retryLimit): void {
		if ($retryLimit < 0) {
			throw new InvalidRetryLimitException();
		}
	}

}

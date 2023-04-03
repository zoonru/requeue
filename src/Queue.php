<?php

namespace Zoon\ReQueue;

use Zoon\ReQueue\Exception\InvalidRetryLimitException;
use Zoon\ReQueue\Exception\InvalidUpdateCallbackException;
use Zoon\ReQueue\Exception\PushException;
use Zoon\ReQueue\Exception\RetryLimitException;

final class Queue implements QueueInterface {

	private const DEFAULT_DATA_KEY_PREFIX = 'dmq:data:';
	private const DEFAULT_TIMESTAMP_INDEX_KEY = 'dmq:tsIndex';
	private const CLEAR_BUFFER_SIZE = 1000;

	private $client;
	private $dataKeyPrefix;
	private $timestampIndexKey;

	/**
	 * Queue constructor.
	 * @param RedisAdapterInterface $redis
	 * @param string $dataKeyPrefix
	 * @param string $timestampIndexKey
	 */
	public function __construct(
		RedisAdapterInterface $redis,
		string $dataKeyPrefix = self::DEFAULT_DATA_KEY_PREFIX,
		string $timestampIndexKey = self::DEFAULT_TIMESTAMP_INDEX_KEY
	) {
		$this->client = $redis;
		$this->dataKeyPrefix = $dataKeyPrefix;
		$this->timestampIndexKey = $timestampIndexKey;
	}

	/**
	 * @param MessageInterface $message
	 * @throws PushException
	 */
	public function push(MessageInterface $message): void {
		if ($this->tryPush($message) === false) {
			throw new PushException();
		}
	}

	/**
	 * @param MessageInterface $message
	 * @return bool
	 */
	private function tryPush(MessageInterface $message): bool {
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
			/** @var MessageDataInterface $updatedMessage */
			$updatedMessageData = $updateCallback($this->getMessageData($id));
			if (!($updatedMessageData instanceof MessageDataInterface)) {
				throw new InvalidUpdateCallbackException();
			}
			if ($this->tryPush(new Message($id, $updatedMessageData->getTimestamp(), $updatedMessageData->getData())) === false) {
				continue;
			}
			return;
		}
		throw new RetryLimitException();
	}

	/**
	 * @param TimestampRangeInterface|null $timestampRange
	 * @param int $retryLimit
	 * @return MessageInterface|null
	 * @throws InvalidRetryLimitException
	 */
	public function pop(?TimestampRangeInterface $timestampRange = null, int $retryLimit = 1000): ?MessageInterface {
		self::validateRetryLimit($retryLimit);
		$timestampRange = $timestampRange ?? new TimestampRange();
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
			$messageData = $this->getMessageData($id);
			if ($messageData === null) {
				$this->client->unwatch();
				continue;
			}
			//if (
			//	($timestampRange->getMin() !== null && $messageData->getTimestamp() < $timestampRange->getMin()) ||
			//	($timestampRange->getMax() !== null && $messageData->getTimestamp() > $timestampRange->getMax())
			//) {
			//	$this->client->unwatch();
			//	continue;
			//}
			$this->client->multi();
			$this->client->del($dataKey);
			$this->client->zRem($this->timestampIndexKey, $id);
			if ($this->client->exec() === false) {
				continue;
			}
			return new Message($id, $messageData->getTimestamp(), $messageData->getData());
		}
		return null;
	}

	/**
	 * @param string $id
	 * @return MessageDataInterface|null
	 */
	private function getMessageData(string $id): ?MessageDataInterface {
		$timestamp = $this->client->zScore($this->timestampIndexKey, $id);
		if ($timestamp === null) {
			return null;
		}
		$data = $this->client->get($this->getDataKey($id));
		if ($data === null) {
			return null;
		}
		return new MessageData($timestamp, $data);
	}

	/**
	 * @param TimestampRangeInterface|null $timestampRange
	 * @return int
	 */
	public function count(?TimestampRangeInterface $timestampRange = null): int {
		return $this->client->zCount(
			$this->timestampIndexKey,
			$timestampRange
		);
	}

	/**
	 * @param TimestampRangeInterface|null $timestampRange
	 * @throws RetryLimitException
	 */
	public function clear(?TimestampRangeInterface $timestampRange = null): void {
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

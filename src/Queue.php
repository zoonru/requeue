<?php

namespace Zoon\ReQueue;

use Zoon\ReQueue\Exception\InvalidRetryLimitException;
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
	 * @param list<Message> $messages
	 */
	public function putMessages(array $messages, MessageReducer $messageReducer, int $retryLimit = 3): void {
		self::validateRetryLimit($retryLimit);
		if (\count($messages) === 0) {
			return;
		}

		$keys = [];
		foreach ($messages as $message) {
			$keys[] = $this->getDataKey($message->getId());
		}

		for ($attemptNo = 0; $attemptNo < $retryLimit; ++$attemptNo) {
			$this->client->watch(...$keys);

			$oldMessages = $this->client->mGet($keys);
			$newMessages = [];
			foreach ($keys as $i => $key) {
				$oldMessage = $oldMessages[$i];
				if ($oldMessage === false) {
					$newMessages[] = $messages[$i];
				} else {
					// when the version of redis => 6.2.0 and ZMSCORE is available, it's possible to reconstruct the message without the ZSCORE and the second GET 
					$oldMessage = $this->getMessage($messages[$i]->getId());
					if ($oldMessage !== null) {
						$newMessages[] = $messageReducer->reduce($oldMessage, $messages[$i]);
					} else {
						$newMessages[] = $messages[$i];
					}
				}
			}

			$this->client->multi();

			$this->client->mSet($this->messagesToMSetArg($newMessages));
			$this->client->zAdd($this->timestampIndexKey, ...$this->messagesToZAddArgs($newMessages));

			if ($this->client->exec() !== false) {
				return;
			}
		}

		throw new RetryLimitException();
	}

	/**
	 * @param list<Message> $messages
	 * @return array<string, string>
	 */
	private function messagesToMSetArg(array $messages): array {
		$mSetArg = [];
		foreach ($messages as $message) {
			$mSetArg[$this->getDataKey($message->getId())] = $message->getData();
		}
		return $mSetArg;
	}

	/**
	 * @param list<Message> $messages
	 * @return list<string>
	 */
	private function messagesToZAddArgs(array $messages): array {
		return array_merge(
			...array_map(static fn (Message $message): array => [$message->getTimestamp(), $message->getId()], $messages),
		);
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
                trigger_error('Message not found', E_USER_WARNING);
				$this->client->unwatch();
				continue;
			}

			$this->client->multi();
			$this->client->del($dataKey);
			$this->client->zRem($this->timestampIndexKey, $id);
			if ($this->client->exec() === false) {
                trigger_error('Unable to exec', E_USER_WARNING);
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

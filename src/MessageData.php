<?php

namespace Zoon\ReQueue;

class MessageData implements MessageDataInterface {

	protected $timestamp;
	protected $data;

	/**
	 * MessageData constructor.
	 * @param int $timestamp
	 * @param string $data
	 */
	public function __construct(int $timestamp, string $data) {
		$this->timestamp = $timestamp;
		$this->data = $data;
	}

	/**
	 * @return int
	 */
	public function getTimestamp(): int {
		return $this->timestamp;
	}

	/**
	 * @return string
	 */
	public function getData(): string {
		return $this->data;
	}

}
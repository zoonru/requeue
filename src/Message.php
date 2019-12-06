<?php

namespace Zoon\ReQueue;

final class Message extends MessageData implements MessageInterface {

	private $id;

	/**
	 * Message constructor.
	 * @param string $id
	 * @param int $timestamp
	 * @param string $data
	 */
	public function __construct(string $id, int $timestamp, string $data) {
		parent::__construct($timestamp, $data);
		$this->id = $id;
	}

	/**
	 * @return string
	 */
	public function getId(): string {
		return $this->id;
	}

}
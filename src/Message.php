<?php

namespace Zoon\ReQueue;

final class Message {

	private string $id;

    private int $timestamp;
    private string $data;

	/**
	 * Message constructor.
	 * @param string $id
	 * @param int $timestamp
	 * @param string $data
	 */
	public function __construct(string $id, int $timestamp, string $data) {
        $this->timestamp = $timestamp;
        $this->data = $data;
		$this->id = $id;
	}

	/**
	 * @return string
	 */
	public function getId(): string {
		return $this->id;
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
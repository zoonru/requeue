<?php

namespace Zoon\ReQueue;

final class TimestampRange implements TimestampRangeInterface {

	private $min;
	private $max;

	/**
	 * TimestampRange constructor.
	 * @param int|null $min
	 * @param int|null $max
	 */
	public function __construct(?int $min = null, ?int $max = null) {
		if ($min !== null && $max !== null && $max < $min) {
			throw new \InvalidArgumentException('max < min');
		}
		$this->min = $min;
		$this->max = $max;
	}

	/**
	 * @return int|null
	 */
	public function getMin(): ?int {
		return $this->min;
	}

	/**
	 * @return int|null
	 */
	public function getMax(): ?int {
		return $this->max;
	}

}
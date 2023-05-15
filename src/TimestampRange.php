<?php

namespace Zoon\ReQueue;

final class TimestampRange {

	/**
	 * TimestampRange constructor.
	 * @param int|null $min
	 * @param int|null $max
	 */
	public function __construct(
        private ?int $min = null,
        private ?int $max = null
    ) {
		if ($this->min !== null && $this->max !== null && $this->max < $this->min) {
			throw new \InvalidArgumentException('max < min');
		}
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
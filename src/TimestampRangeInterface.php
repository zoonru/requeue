<?php

namespace Zoon\ReQueue;

interface TimestampRangeInterface {

	public function getMin(): ?int;
	public function getMax(): ?int;

}
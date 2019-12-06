<?php

namespace Zoon\ReQueue\Exception;

final class NotAtomicRedisModeException extends \LogicException {

	public function __construct() {
		parent::__construct('redis mode !== atomic');
	}

}
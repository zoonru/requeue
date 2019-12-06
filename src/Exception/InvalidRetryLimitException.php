<?php

namespace Zoon\ReQueue\Exception;

final class InvalidRetryLimitException extends \LogicException {

	public function __construct() {
		parent::__construct('retryLimit < 0');
	}

}
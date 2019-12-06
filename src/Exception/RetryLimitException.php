<?php

namespace Zoon\ReQueue\Exception;

final class RetryLimitException extends \RuntimeException {

	public function __construct() {
		parent::__construct('retries > retryLimit');
	}

}
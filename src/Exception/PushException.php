<?php

namespace Zoon\ReQueue\Exception;

final class PushException extends \LogicException {

	public function __construct() {
		parent::__construct('tryPush === false');
	}

}
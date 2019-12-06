<?php

namespace Zoon\ReQueue\Exception;

final class InvalidUpdateCallbackException extends \LogicException {

	public function __construct() {
		parent::__construct('return updateCallback !== MessageDataInterface');
	}

}
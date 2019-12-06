<?php

namespace Zoon\ReQueue;

interface MessageDataInterface {

	public function getTimestamp(): int;
	public function getData(): string;

}
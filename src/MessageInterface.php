<?php

namespace Zoon\ReQueue;

interface MessageInterface extends MessageDataInterface {

	public function getId(): string;

}
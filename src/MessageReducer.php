<?php

namespace Zoon\ReQueue;

interface MessageReducer {

	public function reduce(Message $oldMessage, Message $newMessage): Message;

}

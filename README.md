# ReQueue

Redis delayed message queue without locks.

# Quick start

## Install

```bash
composer require zoon/requeue
```
Note: Requires Redis >= 2.2.0

## Example

**Initialize**
```php
$redis = new \Redis();
if (!$redis->connect('127.0.0.1')) {
    exit('no connection');
}
$queue = createQueue($redis);

function createQueue(\Redis $connection): \Zoon\ReQueue\QueueInterface {
	$redisAdapter = new \Zoon\ReQueue\RedisAdapter($connection);
	return new \Zoon\ReQueue\Queue($redisAdapter);
}
```
**Push**
```php
$queue->push(new \Zoon\ReQueue\Message('id', time() + 3600, 'data'));
```
**Update**
```php
$queue->update('id', function (?\Zoon\ReQueue\MessageDataInterface $old) {
	$time = ($old === null ? time() + 3600 : $old->getTimestamp() + 600);
	$data = ($old === null ? 'data' : $old->getData() . '+new');
	return new \Zoon\ReQueue\MessageData($time, $data);
});
```
**Count**
```php
printf("count: %d\n", $queue->count());
```
```php
// count: 1
```
**Pop**
```php
$timestampRange = new \Zoon\ReQueue\TimestampRange(null, time() + 3600 + 600);
$message = $queue->pop($timestampRange);
if ($message !== null) {
	printf("id: %s\n", $message->getId());
	printf("timestamp: %d\n", $message->getTimestamp());
	printf("data: %s\n", $message->getData());
}
```
```php
// id: id
// timestamp: 1575040030
// data: data+new
```
**Clear**
```php
$queue->clear();
```
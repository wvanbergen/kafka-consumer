# Kafka::Consumer

High-level Kafka consumer for Ruby. Uses Zookeeper to manage load balancing, failover, and offset management.

A consumer group consists of multiple instances of the same consumer. Every instance registers itself in
Zookeeper. Based on the number of instances that are registered, an instance will start consuming some or
all of the partitions of the topics it wants to consume.

The distribution algorithm will make sure that every partition is only consumed by one consumer
instance at a time. It uses Zookeeper watches to be notified of new consumer instances coming
online or going offline, which will trigger a redistribition of all the partitions that are consumed.

Periodically, it will commit the last processed offset of every partition to Zookeeper. Whenever a
new consumer starts, it will resume consumingevery partition at the last committed offset. This implements
an **at least once guarantee**, so it is possible that you end up consuming the same message more than once.
It's your responsibility to deal with this if that is a problem for you, e.g. by using idempotent operations.

## Usage

First, add `kafka-consumer` to your **Gemfile**, and run `bundle install.
If your messages are snappy-compressed, add the `snappy` gem as well.

``` ruby
zookeeper = "zk1:2181,zk2:2181,zk3:2181"
name      = "access-log-processor"
topics    = ["access_log"]

consumer = Kafka::Consumer.new(name, topics, zookeeper: zookeeper)

Signal.trap("INT") { consumer.interrupt }

consumer.each do |message|
  # process message
end
```

## Contributing

1. Fork it ( https://github.com/wvanbergen/kafka-consumer/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

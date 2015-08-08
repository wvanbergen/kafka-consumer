require "kazoo"
require "poseidon"
require "thread"
require "logger"

require "kafka/consumer/version"

module Kafka
  class Consumer

    class Message
      attr_reader :topic, :partition, :offset, :key, :value

      def initialize(topic, partition, fetched_message)
        @topic, @partition = topic, partition
        @key, @value, @offset = fetched_message.key, fetched_message.value, fetched_message.offset
      end
    end

    include Enumerable

    attr_reader :name, :subscription, :queue, :cluster, :max_wait_ms, :logger

    def initialize(name, subscription, zookeeper: [], chroot: '', max_wait_ms: 500, logger: nil)
      @name, @subscription, @max_wait_ms = name, subscription, max_wait_ms
      @cluster = Kazoo::Cluster.new(zookeeper, chroot: chroot)

      @logger = logger || Logger.new($stdout)

      @queue, @dead = Queue.new, false
      start_consuming
    end

    def topics
      @topics ||= Array(@cluster.topics[subscription]) # todo: multiple topics
    end

    def partitions
      topics.map(&:partitions).flatten
    end

    def interrupt
      Thread.new do
        logger.info "Interrupting partition consumers..."
        @partition_consumers.each(&:interrupt)
        @partition_consumers.each(&:wait)
        @dead = true
        logger.info "All partition consumers were terminated"
      end
    end

    def stop
      interrupt.join
    end

    def dead?
      @dead
    end

    def each(&block)
      until dead? && queue.empty?
        yield queue.pop(true)
        @partition_consumers.each(&:continue) if queue.length < 50
      end
      logger.debug "All events where consumed"

    rescue ThreadError
      retry
    end

    private

    def start_consuming
      @partition_consumers = partitions.map do |partition|
        PartitionConsumer.new(self, partition, max_wait_ms: max_wait_ms)
      end
    end

    class PartitionConsumer
      attr_reader :consumer, :partition

      def wait
        consumer.logger.info "Waiting for #{partition.topic.name}/#{partition.id} to terminate..."
        @thread.join if @thread.alive?
        consumer.logger.info "#{partition.topic.name}/#{partition.id} was terminated!"
      end

      def interrupt
        consumer.logger.info "Interrupting consumer for #{partition.topic.name}/#{partition.id}..."
        @thread[:dying] = true
      end

      def continue
        @thread.run if @thread.status == 'sleep'
      end

      def dying?
        @thread[:dying]
      end

      def initialize(consumer, partition, max_wait_ms: 100)
        consumer.logger.info "Starting consumer for #{partition.topic.name}/#{partition.id}..."
        @consumer, @partition = consumer, partition
        @pc = Poseidon::PartitionConsumer.consumer_for_partition(
                  @name,
                  consumer.cluster.brokers.values.map(&:addr),
                  partition.topic.name,
                  partition.id,
                  -1)

        @thread = Thread.new do
          until dying?
            if consumer.queue.length > 100
              consumer.logger.debug "Too much backlog in event queue, pausing #{partition.topic.name}/#{partition.id} for a bit"
              Thread.stop
            end

            messages = @pc.fetch(max_wait_ms: max_wait_ms)
            consumer.logger.debug "Fetched #{messages.length} messages for #{partition.topic.name}/#{partition.id}"
            messages.each do |message|
              consumer.queue << Message.new(partition.topic.name, partition.id, message)
            end
          end

          consumer.logger.info "Terminating consumer for #{partition.topic.name}/#{partition.id}..."
          @pc.close
        end
      end
    end
  end
end

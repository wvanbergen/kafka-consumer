require "kazoo"
require "poseidon"
require "thread"
require "logger"

require "kafka/consumer/version"
require "kafka/consumer/message"

module Kafka
  class Consumer

    BACKPRESSURE_MESSAGE_LIMIT = 1000
    RETRY_CLAIM_PARTITION_INTERVAL = 1.0

    include Enumerable

    attr_reader :name, :subscription, :queue, :cluster, :max_wait_ms, :logger, :instance

    def initialize(name, subscription, zookeeper: [], chroot: '', max_wait_ms: 500, logger: nil)
      @name, @subscription, @max_wait_ms = name, subscription, max_wait_ms
      @logger = logger || Logger.new($stdout)

      @cluster = Kazoo::Cluster.new(zookeeper, chroot: chroot)
      @consumergroup = Kazoo::Consumergroup.new(@cluster, name)
      @consumergroup.create unless @consumergroup.exists?

      @instance = @consumergroup.instantiate
      @instance.register(topics)

      @queue = Queue.new
      @consumer_manager = mananage_partition_consumers
      @consumer_manager.abort_on_exception = true
    end

    def topics
      @topics ||= begin
        topic_names = Array(subscription)
        topic_names.map { |topic_name| cluster.topics.fetch(topic_name) }
      end
    end

    def partitions
      topics.flat_map(&:partitions).sort_by { |partition| [partition.leader.id, partition.topic.name, partition.id] }
    end

    def interrupt
      Thread.new do
        logger.info "Interrupting partition consumers..."
        @consumer_manager[:interrupted] = true

        # Make sure to wake up the manager thread, and join it
        continue
        @consumer_manager.join

        # Deregister the instance. This should trigger
        # a rebalance in all the remaining instances.
        @instance.deregister
        logger.info "Consumer group instance was deregistered"

        cluster.close
      end
    end

    def interrupted?
      @consumer_manager[:interrupted]
    end

    def stop
      interrupt.join
    end

    def dead?
      @consumer_manager.status == false
    end

    def each(&block)
      until dead? && queue.empty?
        yield queue.pop(true)

        if queue.length < BACKPRESSURE_MESSAGE_LIMIT / 2
          # Resume any partition consumers that have been suspended due to backpressure
          @partition_consumers.each(&:continue)
        end
      end
      logger.debug "All events where consumed"

    rescue ThreadError
      retry
    end

    def self.distribute_partitions(instances, partitions)
      return {} if instances.empty?
      partitions_per_instance = partitions.length.to_f / instances.length.to_f

      partitions.group_by.with_index do |partition, index|
        instance_index = index.fdiv(partitions_per_instance).floor
        instances[instance_index]
      end
    end

    private

    def continue
      @consumer_manager.run if @consumer_manager.status == 'sleep'
    end

    def mananage_partition_consumers
      Thread.new do
        until interrupted?
          running_instances = @consumergroup.instances.sort_by(&:id)
          logger.info "#{running_instances.length} instances have been registered: #{running_instances.map(&:id).join(', ')}."

          # Distribute the partitions over the running instances. Afterwards, we can see
          # what partitions are assigned to this particular instance. Because all instances
          # run the same algorithm on the same sorted lists of instances and partitions,
          # all instances should be in agreement of the distribtion.
          distributed_partitions = self.class.distribute_partitions(running_instances, partitions)
          my_partitions = distributed_partitions[@instance]

          logger.info "Claiming #{my_partitions.length} out of #{partitions.length} partitions."

          @partition_consumers = my_partitions.map do |partition|
            PartitionConsumer.new(self, partition, max_wait_ms: max_wait_ms)
          end

          @consumergroup.watch_instances { continue }

          logger.info "Suspended consumer manager thread."
          Thread.stop
          logger.info "Consumer manager thread is resuming..."

          logger.info "Stopping partition consumers..."
          @partition_consumers.each(&:interrupt)
          @partition_consumers.each(&:wait)
        end

        logger.debug "Consumer interrupted, manager will shut down."
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
        @thread[:interrupted] = true
        continue
      end

      def continue
        @thread.run if @thread.status == 'sleep'
      end

      def interrupted?
        @thread[:interrupted]
      end

      def initialize(consumer, partition, max_wait_ms: 100)
        @consumer, @partition = consumer, partition
        @thread = Thread.new do

          # First, we will try to claim the partition in Zookeeper to ensure there's
          # only one consumer for it simultaneously.
          consumer.logger.info "Claiming partition #{partition.topic.name}/#{partition.id}..."
          begin
            consumer.instance.claim_partition(partition)
          rescue Kazoo::PartitionAlreadyClaimed
            consumer.logger.warn "Partition #{partition.topic.name}/#{partition.id} is still claimed by another instance. Trying again in 1 second..."
            sleep(RETRY_CLAIM_PARTITION_INTERVAL)
            return if interrupted?
            retry # abort eventually?
          end

          # Now we have successfully claimed the partition, we can start a consumer for it.
          consumer.logger.info "Starting consumer for #{partition.topic.name}/#{partition.id}..."
          pc = Poseidon::PartitionConsumer.consumer_for_partition(
                @name,
                consumer.cluster.brokers.values.map(&:addr),
                partition.topic.name,
                partition.id,
                -1 # TODO: resume.
              )

          until interrupted?
            if consumer.queue.length > BACKPRESSURE_MESSAGE_LIMIT
              consumer.logger.debug "Too much backlog in event queue, pausing #{partition.topic.name}/#{partition.id} for a bit"
              Thread.stop
              consumer.logger.debug "Resuming #{partition.topic.name}/#{partition.id} now backlog has cleared..."
            end

            messages = pc.fetch(max_wait_ms: max_wait_ms)
            messages.each do |message|
              consumer.queue << Message.new(partition.topic.name, partition.id, message)
            end
          end

          consumer.logger.info "Terminating consumer for #{partition.topic.name}/#{partition.id}..."
          pc.close

          consumer.instance.release_partition(partition)
          consumer.logger.info "Released claim for partition #{partition.topic.name}/#{partition.id}!"
        end
        @thread.abort_on_exception = true
      end
    end
  end
end

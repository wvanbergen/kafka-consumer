require "kazoo"
require "poseidon"
require "thread"
require "logger"

require "kafka/consumer/version"
require "kafka/consumer/message"

module Kafka
  class Consumer
    BACKPRESSURE_MESSAGE_LIMIT = 1000

    include Enumerable

    attr_reader :name, :subscription, :queue, :cluster, :max_wait_ms, :logger, :instance, :group

    def initialize(name, subscription, zookeeper: [], chroot: '', max_wait_ms: 500, logger: nil)
      @name, @subscription, @max_wait_ms = name, subscription, max_wait_ms
      @logger = logger || Logger.new($stdout)

      @cluster = Kazoo::Cluster.new(zookeeper, chroot: chroot)
      @group = Kazoo::Consumergroup.new(@cluster, name)
      @group.create unless @group.exists?

      @instance = @group.instantiate
      @instance.register(topics)

      @queue = Queue.new
      @consumer_manager = mananage_partition_consumers
      @consumer_manager[:offsets] = {}
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
        Thread.current.abort_on_exception = true

        logger.info "Stopping partition consumers..."
        @consumer_manager[:interrupted] = true

        # Make sure to wake up the manager thread, so it can shut down
        continue
      end
    end

    def interrupted?
      @consumer_manager[:interrupted]
    end

    def stop
      interrupt
      wait
    end

    def wait
      @consumer_manager.join
    end

    def dead?
      @consumer_manager.status == false
    end

    def each(&block)
      until dead? && queue.empty?
        message = queue.pop(true)
        block.call(message)

        @consumer_manager[:offsets][message.topic]
      end
      logger.debug "All events in the queue were consumed."

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
        Thread.current.abort_on_exception = true

        logger.info "Registered for #{group.name} as #{instance.id}"

        @partition_consumers = {}

        until interrupted?
          running_instances, change = group.watch_instances { continue }
          logger.info "#{running_instances.length} instances have been registered: #{running_instances.map(&:id).join(', ')}."

          # Distribute the partitions over the running instances. Afterwards, we can see
          # what partitions are assigned to this particular instance. Because all instances
          # run the same algorithm on the same sorted lists of instances and partitions,
          # all instances should be in agreement of the distribtion.
          distributed_partitions = self.class.distribute_partitions(running_instances, partitions)
          my_partitions = distributed_partitions[@instance]

          logger.info "Claiming #{my_partitions.length} out of #{partitions.length} partitions."

          partition_to_stop  = @partition_consumers.keys - my_partitions
          partition_to_start = my_partitions - @partition_consumers.keys

          if partition_to_stop.length > 0
            logger.info "Stopping #{partition_to_stop.length} out of #{@partition_consumers.length} partition consumers."

            partition_to_stop.each do |partition|
              partition_consumer = @partition_consumers.delete(partition)
              partition_consumer.stop
            end
          end

          if partition_to_start.length > 0
            logger.info "Starting #{partition_to_start.length} new partition consumers."

            partition_to_start.each do |partition|
              @partition_consumers[partition] = PartitionConsumer.new(self, partition, max_wait_ms: max_wait_ms)
            end
          end

          unless change.completed?
            logger.debug "Suspended consumer manager thread."
            Thread.stop
            logger.debug "Consumer manager thread woke up..."
          end
        end

        logger.debug "Consumer interrupted."

        @partition_consumers.each_value(&:stop)

        # Deregister the instance. This should trigger
        # a rebalance in all the remaining instances.
        @instance.deregister
        logger.debug "Consumer group instance #{instance.id} was deregistered"

        cluster.close
      end
    end

    class PartitionConsumer
      attr_reader :consumer, :partition

      def wait
        @thread.join if @thread.alive?
      end

      def interrupt
        @thread[:interrupted] = true
        continue
      end

      def stop
        interrupt
        wait
        consumer.logger.info "Consumer for #{partition.topic.name}/#{partition.id} stopped."
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
          Thread.current.abort_on_exception = true

          # First, we will try to claim the partition in Zookeeper to ensure there's
          # only one consumer for it simultaneously.
          consumer.logger.info "Claiming partition #{partition.topic.name}/#{partition.id}..."
          begin
            other_instance, change = consumer.group.watch_partition_claim(partition) { continue }
            if other_instance.nil?
              consumer.instance.claim_partition(partition)
            elsif other_instance == consumer.instance
              raise "Already claimed this partition myself. That should not happen"
            else
              consumer.logger.warn "Partition #{partition.topic.name}/#{partition.id} is still claimed by instance #{other_instance.id}. Waiting for the claim to be released..."
              Thread.stop unless change.completed?
              raise Kazoo::PartitionAlreadyClaimed
            end
          rescue Kazoo::PartitionAlreadyClaimed
            retry unless interrupted?
          end

          unless interrupted?
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
                sleep(1.0)
              end

              messages = pc.fetch(max_wait_ms: max_wait_ms)
              messages.each do |message|
                consumer.queue << Message.new(partition.topic.name, partition.id, message)
              end
            end

            consumer.logger.debug "Stopping consumer for #{partition.topic.name}/#{partition.id}..."
            pc.close

            consumer.instance.release_partition(partition)
            consumer.logger.debug "Released claim for partition #{partition.topic.name}/#{partition.id}!"
          end
        end
      end
    end
  end
end

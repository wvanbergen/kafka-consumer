require "kazoo"
require "poseidon"
require "thread"
require "logger"

require "kafka/consumer/message"
require "kafka/consumer/partition_consumer"
require "kafka/consumer/version"

module Kafka
  class Consumer
    include Enumerable

    attr_reader :subscription,
        :cluster, :group, :instance,
        :max_wait_ms, :initial_offset,
        :logger

    def initialize(name, subscription, zookeeper: nil, max_wait_ms: 200, initial_offset: :latest_offset, logger: nil)
      raise ArgumentError, "The consumer's name cannot be empty" if name.nil? || name.empty?
      raise ArgumentError, "You have to specify a zookeeper connection string" if zookeeper.nil? || zookeeper.empty?

      @name = name
      @max_wait_ms, @initial_offset = max_wait_ms, initial_offset
      @logger = logger || Logger.new($stdout)

      @cluster = Kazoo::Cluster.new(zookeeper)
      @group = Kazoo::Consumergroup.new(@cluster, name)

      @group.create unless @group.exists?
      @instance = @group.instantiate(subscription: Kazoo::Subscription.build(subscription)).register
    end

    def name
      group.name
    end

    def id
      instance.id
    end

    def subscription
      instance.subscription
    end

    def partitions
      subscription.partitions(@cluster).sort_by { |partition| [partition.preferred_leader.id, partition.topic.name, partition.id] }
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
      @consumer_manager.join if @consumer_manager.alive?
    end

    def dead?
      @consumer_manager.status == false
    end

    def each(&block)
      mutex = Mutex.new

      handler = lambda do |message|
        mutex.synchronize { block.call(message) }
      end

      @consumer_manager = Thread.new do
        Thread.current.abort_on_exception = true
        manage_partition_consumers(handler)
      end

      wait
    end

    def self.distribute_partitions(instances, partitions)
      return {} if instances.empty?
      # sort the list of instance to make sure all instances
      # get the same order when running distribution algorithm.
      # Otherwise, distribution will probably conflict.
      instances = instances.sort_by { |i| i.id }
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

    def manage_partition_consumers(handler)
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
        my_partitions = distributed_partitions.fetch(@instance, [])

        logger.info "Claiming #{my_partitions.length} out of #{partitions.length} partitions."

        # based onw hat partitions we should be consuming and the partitions
        # that we already are consuming, figure out what partition consumers
        # to stop and start
        partitions_to_stop  = @partition_consumers.keys - my_partitions
        partitions_to_start = my_partitions - @partition_consumers.keys

        # Stop the partition consumers we should no longer be running in parallel
        if partitions_to_stop.length > 0
          logger.info "Stopping #{partitions_to_stop.length} out of #{@partition_consumers.length} partition consumers."

          threads = []
          partitions_to_stop.each do |partition|
            partition_consumer = @partition_consumers.delete(partition)
            threads << Thread.new { partition_consumer.stop }
          end
          threads.each(&:join)
        end

        # Start all the partition consumers we are missing.
        if partitions_to_start.length > 0
          logger.info "Starting #{partitions_to_start.length} new partition consumers."

          partitions_to_start.each do |partition|
            @partition_consumers[partition] = PartitionConsumer.new(self, partition,
                max_wait_ms: max_wait_ms, initial_offset: initial_offset, handler: handler)
          end
        end

        unless change.completed?
          logger.debug "Suspended consumer manager thread."
          Thread.stop
          logger.debug "Consumer manager thread woke up..."
        end
      end

      logger.debug "Consumer interrupted."

      # Stop all running partition consumers
      threads = []
      @partition_consumers.each_value do |partition_consumer|
        threads << Thread.new { partition_consumer.stop }
      end
      threads.each(&:join)

      # Deregister the instance. This should trigger a rebalance in all the remaining instances.
      @instance.deregister
      logger.debug "Consumer group instance #{instance.id} was deregistered"

      cluster.close
    end
  end
end

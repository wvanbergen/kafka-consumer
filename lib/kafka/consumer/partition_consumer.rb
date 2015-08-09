module Kafka
  class Consumer
    class PartitionConsumer

      attr_reader :consumer, :partition, :handler, :max_wait_ms, :initial_offset

      def initialize(consumer, partition, handler: nil, max_wait_ms: 100, initial_offset: :latest_offset)
        @consumer, @partition, @handler = consumer, partition, handler
        @initial_offset, @max_wait_ms = initial_offset, max_wait_ms
        @consumer_thread = partition_consumer_thread
      end

      def wait
        @consumer_thread.join if @consumer_thread.alive?
      end

      def interrupt
        @consumer_thread[:interrupted] = true
        continue
      end

      def interrupted?
        @consumer_thread[:interrupted]
      end

      def stop
        interrupt
        wait
        consumer.logger.info "Consumer for #{partition.topic.name}/#{partition.id} stopped."
      end

      def continue
        @consumer_thread.run if @consumer_thread.status == 'sleep'
      end

      def partition_consumer_thread
        Thread.new do
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
                  consumer.group.name,
                  consumer.cluster.brokers.values.map(&:addr),
                  partition.topic.name,
                  partition.id,
                  initial_offset # TODO: resume.
                )

            until interrupted?
              pc.fetch(max_wait_ms: max_wait_ms).each do |message|
                handler.call(Message.new(partition.topic.name, partition.id, message))
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

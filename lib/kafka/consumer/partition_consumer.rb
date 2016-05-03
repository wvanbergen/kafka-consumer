module Kafka
  class Consumer
    class PartitionConsumer

      attr_reader :consumer, :partition, :handler, :max_wait_ms, :initial_offset,
                  :commit_interval, :last_processed_offset, :last_committed_offset

      def initialize(consumer, partition, handler: nil, max_wait_ms: 100, initial_offset: :latest_offset, commit_interval: 5.0)
        @consumer, @partition, @handler = consumer, partition, handler
        @initial_offset, @max_wait_ms, @commit_interval = initial_offset, max_wait_ms, commit_interval

        @commit_mutex = Mutex.new

        @consumer_thread = Thread.new do
          Thread.current.abort_on_exception = true
          manage_partition_consumer
        end

        Thread.new do
          Thread.current.abort_on_exception = true
          background_committer
        end
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

      def claim_partition
        consumer.logger.info "Claiming partition #{partition.topic.name}/#{partition.id}..."
        begin
          other_instance, change = consumer.group.watch_partition_claim(partition) { continue }
          if other_instance.nil?
            consumer.instance.claim_partition(partition)
          elsif other_instance == consumer.instance
            raise Kazoo::Error, "Already claimed this partition myself. That should not happen"
          else
            consumer.logger.warn "Partition #{partition.topic.name}/#{partition.id} is still claimed by instance #{other_instance.id}. Waiting for the claim to be released..."
            Thread.stop unless change.completed?

            return false if interrupted?
            raise Kazoo::PartitionAlreadyClaimed
          end
        rescue Kazoo::PartitionAlreadyClaimed
          retry unless interrupted?
        end

        true
      end

      def commit_last_offset
        @commit_mutex.synchronize do
          if last_processed_offset && (last_committed_offset.nil? || last_committed_offset < last_processed_offset)
            consumer.group.commit_offset(partition, last_processed_offset)
            @last_committed_offset = last_processed_offset + 1
          end
        end
      end

      def background_committer
        until interrupted?
          commit_last_offset
          sleep(commit_interval)
        end
      end

      def manage_partition_consumer
        # First, we will try to claim the partition in Zookeeper to ensure there's
        # only one consumer for it simultaneously.
        if claim_partition
          @last_committed_offset = consumer.group.retrieve_offset(partition)
          case start_offset = last_committed_offset || initial_offset
          when :earliest_offset, -2
            consumer.logger.info "Starting consumer for #{partition.topic.name}/#{partition.id} at the earliest available offset..."
          when :latest_offset, -1
            consumer.logger.info "Starting consumer for #{partition.topic.name}/#{partition.id} for new messages..."
          else
            consumer.logger.info "Starting consumer for #{partition.topic.name}/#{partition.id} at offset #{start_offset}..."
          end

          begin
            pc = Poseidon::PartitionConsumer.consumer_for_partition(
                    consumer.group.name,
                    consumer.cluster.brokers.values.map(&:addr),
                    partition.topic.name,
                    partition.id,
                    start_offset
            )

            until interrupted?
              pc.fetch(max_wait_ms: max_wait_ms).each do |message|
                message = Message.new(partition.topic.name, partition.id, message)
                handler.call(message)
                @last_processed_offset = message.offset
              end
            end

          rescue Poseidon::Errors::OffsetOutOfRange
            pc.close

            consumer.logger.warn "Offset #{start_offset} is no longer available for #{partition.topic.name}/#{partition.id}!"
            case initial_offset
            when :earliest_offset, -2
              consumer.logger.warn "Instead, start consuming #{partition.topic.name}/#{partition.id} at the earliest available offset."
            when :latest_offset, -1
              consumer.logger.warn "Instead, start consuming #{partition.topic.name}/#{partition.id} for new messages only."
            end

            start_offset = initial_offset
            retry

          ensure
            consumer.logger.debug "Stopping consumer for #{partition.topic.name}/#{partition.id}..."
            pc.close if pc
          end


          commit_last_offset
          consumer.logger.info "Committed offset #{last_committed_offset - 1} for #{partition.topic.name}/#{partition.id}..." if last_committed_offset

          consumer.instance.release_partition(partition)
          consumer.logger.debug "Released claim for partition #{partition.topic.name}/#{partition.id}."
        end
      end
    end
  end
end

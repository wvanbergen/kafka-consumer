module Kafka
  class Consumer
    class Message
      attr_reader :topic, :partition, :offset, :key, :value

      def initialize(topic, partition, fetched_message)
        @topic, @partition = topic, partition
        @key, @value, @offset = fetched_message.key, fetched_message.value, fetched_message.offset
      end
    end
  end
end

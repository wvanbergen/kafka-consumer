module Kazoo
  class Topic

    attr_reader :cluster, :name
    attr_accessor :partitions

    def initialize(cluster, name, partitions: nil)
      @cluster, @name, @partitions = cluster, name, partitions
    end

    def self.from_json(cluster, name, json)
      topic = new(cluster, name)
      topic.partitions = json.fetch('partitions').map do |(id, replicas)|
        topic.partition(id.to_i, replicas: replicas.map { |b| cluster.brokers[b] })
      end.sort_by(&:id)

      return topic
    end

    def partition(*args)
      Kazoo::Partition.new(self, *args)
    end

    def replication_factor
      partitions.map(&:replication_factor).min
    end

    def under_replicated?
      partitions.any?(:under_replicated?)
    end

    def inspect
      "#<Kazoo::Topic #{name}>"
    end

    def eql?(other)
      other.kind_of?(Kazoo::Topic) && cluster == other.cluster && name == other.name
    end

    alias_method :==, :eql?

    def hash
      [cluster, name].hash
    end
  end
end

module Kazoo
  class Cluster

    attr_reader :zookeeper, :chroot

    def initialize(zookeeper, chroot: "")
      @zookeeper, @chroot = zookeeper, chroot
      @zk_mutex, @brokers_mutex, @topics_mutex, @consumergroups_mutex = Mutex.new, Mutex.new, Mutex.new, Mutex.new
    end

    def zk
      @zk_mutex.synchronize do
        @zk ||= Zookeeper.new(zookeeper)
      end
    end

    def brokers
      @brokers_mutex.synchronize do
        @brokers ||= begin
          brokers = zk.get_children(path: node_with_chroot("/brokers/ids"))
          result, threads, mutex = {}, ThreadGroup.new, Mutex.new
          brokers.fetch(:children).map do |id|
            t = Thread.new do
              broker_info = zk.get(path: node_with_chroot("/brokers/ids/#{id}"))
              broker = Kazoo::Broker.from_json(self, id, JSON.parse(broker_info.fetch(:data)))
              mutex.synchronize { result[id.to_i] = broker }
            end
            threads.add(t)
          end
          threads.list.each(&:join)
          result
        end
      end
    end

    def consumergroups
      @consumergroups ||= begin
        consumers = zk.get_children(path: node_with_chroot("/consumers"))
        consumers.fetch(:children).map { |name| Kazoo::Consumergroup.new(self, name) }
      end
    end

    def topics
      @topics_mutex.synchronize do
        @topics ||= begin
          topics = zk.get_children(path: node_with_chroot("/brokers/topics"))
          result, threads, mutex = {}, ThreadGroup.new, Mutex.new
          topics.fetch(:children).each do |name|
            t = Thread.new do
              topic_info = zk.get(path: node_with_chroot("/brokers/topics/#{name}"))
              topic = Kazoo::Topic.from_json(self, name, JSON.parse(topic_info.fetch(:data)))
              mutex.synchronize { result[name] = topic }
            end
            threads.add(t)
          end
          threads.list.each(&:join)
          result
        end
      end
    end

    def partitions
      topics.values.flat_map(&:partitions)
    end

    def reset_metadata
      @topics, @brokers = nil, nil
    end

    def under_replicated?
      partitions.any?(&:under_replicated?)
    end

    def node_with_chroot(path)
      "#{@chroot}#{path}"
    end

    def close
      zk.close
    end
  end
end

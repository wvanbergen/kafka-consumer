module Kazoo
  class Consumergroup
    attr_reader :cluster, :name

    def initialize(cluster, name)
      @cluster, @name = cluster, name
    end

    def create
      cluster.zk.create(path: cluster.node_with_chroot("/consumers/#{name}"))
      cluster.zk.create(path: cluster.node_with_chroot("/consumers/#{name}/ids"))
      cluster.zk.create(path: cluster.node_with_chroot("/consumers/#{name}/owners"))
      cluster.zk.create(path: cluster.node_with_chroot("/consumers/#{name}/offsets"))
    end

    def exists?
      stat = cluster.zk.stat(path: cluster.node_with_chroot("/consumers/#{name}"))
      stat.fetch(:stat).exists?
    end


    def instantiate(id: nil)
      Instance.new(self, id: id)
    end

    def instances
      instances = cluster.zk.get_children(path: cluster.node_with_chroot("/consumers/#{name}/ids"))
      instances.fetch(:children).map { |id| Instance.new(self, id: id) }
    end

    def watch_instances(&block)
      cb = Zookeeper::Callbacks::WatcherCallback.create(&block)
      result = cluster.zk.get_children(
        path: cluster.node_with_chroot("/consumers/#{name}/ids"),
        watcher: cb,
      )

      instances = result.fetch(:children).map { |id| Instance.new(self, id: id) }

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to watch instances. Error code result[:rc]"
      end

      [instances, cb]
    end


    def watch_partition_claim(partition, &block)
      cb = Zookeeper::Callbacks::WatcherCallback.create(&block)

      result = cluster.zk.get(
        path:    cluster.node_with_chroot("/consumers/#{name}/owners/#{partition.topic.name}/#{partition.id}"),
        watcher: cb
      )

      case result.fetch(:rc)
      when Zookeeper::Constants::ZNONODE # Nobody is claiming this partition yet
        [nil, nil]
      when Zookeeper::Constants::ZOK
        [Kazoo::Consumergroup::Instance.new(self, id: result.fetch(:data)), cb]
      else
        raise Kazoo::Error, "Failed set watch for partition claim of #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def retrieve_offset(partition)
      result = cluster.zk.get(path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{partition.topic.name}/#{partition.id}"))
      case result.fetch(:rc)
        when Zookeeper::Constants::ZOK;
          result.fetch(:data).to_i
        when Zookeeper::Constants::ZNONODE;
          nil
        else
          raise Kazoo::Error, "Failed to retrieve offset for partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def commit_offset(partition, offset)
      result = cluster.zk.set(
        path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{partition.topic.name}/#{partition.id}"),
        data: (offset + 1).to_s
      )

      if result.fetch(:rc) == Zookeeper::Constants::ZNONODE
        result = cluster.zk.create(path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{partition.topic.name}"))
        case result.fetch(:rc)
          when Zookeeper::Constants::ZOK, Zookeeper::Constants::ZNODEEXISTS
        else
          raise Kazoo::Error, "Failed to commit offset #{offset} for partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
        end

        result = cluster.zk.create(
          path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{partition.topic.name}/#{partition.id}"),
          data: (offset + 1).to_s
        )
      end

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to commit offset #{offset} for partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def reset_offsets
      result = cluster.zk.get_children(path: cluster.node_with_chroot("/consumers/#{name}/offsets"))
      raise Kazoo::Error unless result.fetch(:rc) == Zookeeper::Constants::ZOK

      result.fetch(:children).each do |topic|
        result = cluster.zk.get_children(path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{topic}"))
        raise Kazoo::Error unless result.fetch(:rc) == Zookeeper::Constants::ZOK

        result.fetch(:children).each do |partition|
          cluster.zk.delete(path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{topic}/#{partition}"))
          raise Kazoo::Error unless result.fetch(:rc) == Zookeeper::Constants::ZOK
        end

        cluster.zk.delete(path: cluster.node_with_chroot("/consumers/#{name}/offsets/#{topic}"))
        raise Kazoo::Error unless result.fetch(:rc) == Zookeeper::Constants::ZOK
      end
    end

    def inspect
      "#<Kazoo::Consumergroup name=#{name}>"
    end

    def eql?(other)
      other.kind_of?(Kazoo::Consumergroup) && cluster == other.cluster && name == other.name
    end

    alias_method :==, :eql?

    def hash
      [cluster, name].hash
    end

    class Instance

      def self.generate_id
        "#{Socket.gethostname}:#{SecureRandom.uuid}"
      end

      attr_reader :group, :id

      def initialize(group, id: nil)
        @group = group
        @id = id || self.class.generate_id
      end

      def registered?
        stat = cluster.zk.stat(path: cluster.node_with_chroot("/consumers/#{group.name}/ids/#{id}"))
        stat.fetch(:stat).exists?
      end

      def register(subscription)
        result = cluster.zk.create(
          path: cluster.node_with_chroot("/consumers/#{group.name}/ids/#{id}"),
          ephemeral: true,
          data: JSON.generate({
            version: 1,
            timestamp: Time.now.to_i,
            pattern: "static",
            subscription: Hash[*subscription.flat_map { |topic| [topic.name, 1] } ]
          })
        )

        if result.fetch(:rc) != Zookeeper::Constants::ZOK
          raise Kazoo::ConsumerInstanceRegistrationFailed, "Failed to register instance #{id} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
        end

        subscription.each do |topic|
          stat = cluster.zk.stat(path: cluster.node_with_chroot("/consumers/#{group.name}/owners/#{topic.name}"))
          unless stat.fetch(:stat).exists?
            result = cluster.zk.create(path: cluster.node_with_chroot("/consumers/#{group.name}/owners/#{topic.name}"))
            if result.fetch(:rc) != Zookeeper::Constants::ZOK
              raise Kazoo::ConsumerInstanceRegistrationFailed, "Failed to register subscription of #{topic.name} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
            end
          end
        end
      end

      def deregister
        cluster.zk.delete(path: cluster.node_with_chroot("/consumers/#{group.name}/ids/#{id}"))
      end

      def claim_partition(partition)
        result = cluster.zk.create(
          path: cluster.node_with_chroot("/consumers/#{group.name}/owners/#{partition.topic.name}/#{partition.id}"),
          ephemeral: true,
          data: id,
        )

        case result.fetch(:rc)
        when Zookeeper::Constants::ZOK
          return true
        when Zookeeper::Constants::ZNODEEXISTS
          raise Kazoo::PartitionAlreadyClaimed, "Partition #{partition.topic.name}/#{partition.id} is already claimed!"
        else
          raise Kazoo::Error, "Failed to claim partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
        end
      end

      def release_partition(partition)
        result = cluster.zk.delete(path: cluster.node_with_chroot("/consumers/#{group.name}/owners/#{partition.topic.name}/#{partition.id}"))
        if result.fetch(:rc) != Zookeeper::Constants::ZOK
          raise Kazoo::Error, "Failed to release partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
        end
      end

      def inspect
        "#<Kazoo::Consumergroup::Instance group=#{group.name} id=#{id}>"
      end

      def hash
        [group, id].hash
      end

      def eql?(other)
        other.kind_of?(Kazoo::Consumergroup::Instance) && group == other.group && id == other.id
      end

      alias_method :==, :eql?

      private

      def cluster
        group.cluster
      end
    end
  end
end

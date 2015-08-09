require 'kazoo'
require 'thor'

module Kazoo
  class CLI < Thor
    class_option :zookeeper, :type => :string, :default => ENV['ZOOKEEPER_PEERS']
    class_option :chroot, :type => :string, :default => ""

    desc "cluster", "Describes the Kafka cluster as registered in Zookeeper"
    def cluster
      validate_class_options!

      kafka_cluster.brokers.values.sort_by(&:id).each do |broker|
        $stdout.puts "#{broker.id}:\t#{broker.addr}\t(hosts #{broker.replicated_partitions.length} partitions, leads #{broker.led_partitions.length})"
      end
    end

    desc "topics", "Lists all topics in the cluster"
    def topics
      validate_class_options!

      kafka_cluster.topics.values.sort_by(&:name).each do |topic|
        $stdout.puts topic.name
      end
    end

    option :topic, :type => :string
    desc "partitions", "Lists partitions"
    def partitions
      validate_class_options!

      topics = kafka_cluster.topics.values
      topics.select! { |t| t.name == options[:topic] } if options[:topic]
      topics.sort_by!(&:name)

      topics.each do |topic|
        topic.partitions.each do |partition|
          $stdout.puts "#{partition.topic.name}/#{partition.id}\tReplicas: #{partition.replicas.map(&:id).join(",")}"
        end
      end
    end

    option :replicas, :type => :numeric, :default => 1
    desc "critical <broker>", "Determine whether a broker is critical"
    def critical(broker_name)
      validate_class_options!

      if broker(broker_name).critical?(replicas: options[:replicas])
        raise Thor::Error, "WARNING: broker #{broker_name} is critical and cannot be stopped safely!"
      else
        $stdout.puts "Broker #{broker_name} is non-critical and can be stopped safely."
      end
    end


    private

    def validate_class_options!
      if options[:zookeeper].nil? || options[:zookeeper] == ''
        raise Thor::InvocationError, "Please supply --zookeeper argument, or set the ZOOKEEPER_PEERS environment variable"
      end
    end

    def broker(name_or_id)
      broker = if name_or_id =~ /\A\d+\z/
        kafka_cluster.brokers[name_or_id.to_i]
      else
        kafka_cluster.brokers.values.detect { |b| b.addr == name_or_id } || cluster.brokers.values.detect { |b| b.host == name_or_id }
      end

      raise Thor::InvocationError, "Broker #{name_or_id.inspect} not found!" if broker.nil?
      broker
    end

    def kafka_cluster
      @kafka_cluster ||= Kazoo::Cluster.new(options[:zookeeper], chroot: options[:chroot])
    end
  end
end

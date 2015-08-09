require 'minitest/autorun'
require 'kafka/consumer'
require 'kazoo'
require 'mocha/mini_test'
require 'pp'

module MockCluster
  def mock_cluster
    cluster = Kazoo::Cluster.new('example.com:2181')
    cluster.stubs(:zk).returns(mock)

    cluster.stubs(:brokers).returns(
      1 => Kazoo::Broker.new(cluster, 1, 'example.com', 9091),
      2 => Kazoo::Broker.new(cluster, 2, 'example.com', 9092),
      3 => Kazoo::Broker.new(cluster, 3, 'example.com', 9093),
    )

    cluster.stubs(:topics).returns(
      'test.1' => topic_1 = Kazoo::Topic.new(cluster, 'test.1'),
      'test.4' => topic_4 = Kazoo::Topic.new(cluster, 'test.4')
    )

    topic_1.stubs(:partitions).returns([
      Kazoo::Partition.new(topic_1, 0, replicas: [cluster.brokers[1], cluster.brokers[2]]),
    ])

    topic_4.stubs(:partitions).returns([
      Kazoo::Partition.new(topic_4, 0, replicas: [cluster.brokers[2], cluster.brokers[1]]),
      Kazoo::Partition.new(topic_4, 1, replicas: [cluster.brokers[2], cluster.brokers[3]]),
      Kazoo::Partition.new(topic_4, 2, replicas: [cluster.brokers[1], cluster.brokers[3]]),
      Kazoo::Partition.new(topic_4, 3, replicas: [cluster.brokers[3], cluster.brokers[2]]),
    ])

    topic_1.partitions[0].stubs(:isr).returns([cluster.brokers[1], cluster.brokers[2]])
    topic_4.partitions[0].stubs(:isr).returns([cluster.brokers[2], cluster.brokers[1]])
    topic_4.partitions[1].stubs(:isr).returns([cluster.brokers[2], cluster.brokers[3]])
    topic_4.partitions[2].stubs(:isr).returns([cluster.brokers[1], cluster.brokers[3]])
    topic_4.partitions[3].stubs(:isr).returns([cluster.brokers[3], cluster.brokers[2]])

    topic_1.partitions[0].stubs(:leader).returns(cluster.brokers[1])
    topic_4.partitions[0].stubs(:leader).returns(cluster.brokers[2])
    topic_4.partitions[1].stubs(:leader).returns(cluster.brokers[2])
    topic_4.partitions[2].stubs(:leader).returns(cluster.brokers[1])
    topic_4.partitions[3].stubs(:leader).returns(cluster.brokers[3])

    return cluster
  end
end

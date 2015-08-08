require 'test_helper'

class PartitionDistributionTest < Minitest::Test

  def setup
    @cluster = Object.new
    @group = Kazoo::Consumergroup.new(@cluster, 'test.ruby')
    @topic = Kazoo::Topic.new(@cluster, 'test.4')
    @partitions = [
      Kazoo::Partition.new(@topic, 0),
      Kazoo::Partition.new(@topic, 1),
      Kazoo::Partition.new(@topic, 2),
      Kazoo::Partition.new(@topic, 3),
    ]
  end

  def test_single_consumer_gets_everything
    instances = [Kazoo::Consumergroup::Instance.new(@group, id: '1')]
    instance_copy = Kazoo::Consumergroup::Instance.new(@group, id: '1')

    distribution = Kafka::Consumer.distribute_partitions(instances, @partitions)
    assert_equal Set.new(instances), Set.new(distribution.keys)
    assert_equal Set.new(@partitions), Set.new(distribution.values.flatten)

    assert_equal @partitions, distribution[instance_copy]
  end

  def test_two_consumers_split_the_load
    instances = [
      Kazoo::Consumergroup::Instance.new(@group, id: '1'),
      Kazoo::Consumergroup::Instance.new(@group, id: '2'),
    ]

    distribution = Kafka::Consumer.distribute_partitions(instances, @partitions)
    assert_equal Set.new(instances), Set.new(distribution.keys)
    assert_equal Set.new(@partitions), Set.new(distribution.values.flatten)

    assert_equal 2, distribution[instances[0]].length
    assert_equal 2, distribution[instances[1]].length
  end


  def test_three_consumers_split_the_load
    instances = [
      Kazoo::Consumergroup::Instance.new(@group, id: '1'),
      Kazoo::Consumergroup::Instance.new(@group, id: '2'),
      Kazoo::Consumergroup::Instance.new(@group, id: '3'),
    ]

    distribution = Kafka::Consumer.distribute_partitions(instances, @partitions)
    assert_equal Set.new(instances), Set.new(distribution.keys)
    assert_equal Set.new(@partitions), Set.new(distribution.values.flatten)

    assert_equal 2, distribution[instances[0]].length
    assert_equal 1, distribution[instances[1]].length
    assert_equal 1, distribution[instances[2]].length
  end

  def test_four_consumers_split_the_load
    instances = [
      Kazoo::Consumergroup::Instance.new(@group, id: '1'),
      Kazoo::Consumergroup::Instance.new(@group, id: '2'),
      Kazoo::Consumergroup::Instance.new(@group, id: '3'),
      Kazoo::Consumergroup::Instance.new(@group, id: '4'),
    ]

    distribution = Kafka::Consumer.distribute_partitions(instances, @partitions)
    assert_equal Set.new(instances), Set.new(distribution.keys)
    assert_equal Set.new(@partitions), Set.new(distribution.values.flatten)

    assert_equal 1, distribution[instances[0]].length
    assert_equal 1, distribution[instances[1]].length
    assert_equal 1, distribution[instances[2]].length
    assert_equal 1, distribution[instances[3]].length
  end

  def test_four_consumers_split_the_load_and_one_is_standby
    instances = [
      Kazoo::Consumergroup::Instance.new(@group, id: '1'),
      Kazoo::Consumergroup::Instance.new(@group, id: '2'),
      Kazoo::Consumergroup::Instance.new(@group, id: '3'),
      Kazoo::Consumergroup::Instance.new(@group, id: '4'),
      Kazoo::Consumergroup::Instance.new(@group, id: '5'),
    ]

    distribution = Kafka::Consumer.distribute_partitions(instances, @partitions)
    assert_equal Set.new(instances[0..3]), Set.new(distribution.keys)
    assert_equal Set.new(@partitions), Set.new(distribution.values.flatten)

    assert_equal 1, distribution[instances[0]].length
    assert_equal 1, distribution[instances[1]].length
    assert_equal 1, distribution[instances[2]].length
    assert_equal 1, distribution[instances[3]].length

    assert_nil distribution[instances[4]]
  end

  def test_assign_everything_with_random_number_of_partitions_and_instances
    partitions = (0 .. rand(500)).map { |i| Kazoo::Partition.new(@topic, i) }
    instances = (0 .. rand(100)).map { |i| Kazoo::Consumergroup::Instance.new(@group, id: i.to_s) }

    distribution = Kafka::Consumer.distribute_partitions(instances, partitions)
    assert_equal [partitions.length, instances.length].min, distribution.keys.length
    assert_equal Set.new(partitions), Set.new(distribution.values.flatten)
  end

  def test_assign_zero_partitions
    instances = [Kazoo::Consumergroup::Instance.new(@group, id: '1')]
    distribution = Kafka::Consumer.distribute_partitions(instances, [])
    assert distribution.empty?
  end

  def test_assign_to_zero_instances
    distribution = Kafka::Consumer.distribute_partitions([], @partitions)
    assert distribution.empty?
  end
end

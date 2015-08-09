require 'test_helper'

class PartitionTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_replication_factor
    assert_equal 2, @cluster.topics['test.1'].partitions[0].replication_factor
  end

  def test_state
    partition = @cluster.topics['test.1'].partitions[0]
    partition.unstub(:leader)
    partition.unstub(:isr)

    json_payload = '{"controller_epoch":157,"leader":1,"version":1,"leader_epoch":8,"isr":[3,2,1]}'
    @cluster.zk.expects(:get).with(path: "/brokers/topics/test.1/partitions/0/state").returns(data: json_payload)

    assert_equal 1, partition.leader.id
    assert_equal [3,2,1], partition.isr.map(&:id)
  end
end

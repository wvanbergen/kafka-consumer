require 'test_helper'

class ClusterTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_cluster_under_replicated?
    refute @cluster.under_replicated?

    @cluster.topics['test.4'].partitions[2].expects(:isr).returns([@cluster.brokers[1]])
    assert @cluster.under_replicated?
  end
end

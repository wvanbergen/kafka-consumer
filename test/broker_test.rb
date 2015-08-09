require 'test_helper'

class BrokerTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_broker_critical?
    refute @cluster.brokers[1].critical?(replicas: 1), "We have 2 in-sync replicas for everything so we can lose 1."
    assert @cluster.brokers[2].critical?(replicas: 2), "We only have 2 replicas so we can never lose 2."

    # Simulate losing a broker from the ISR for a partition.
    # This partition lives on broker 1 and 3
    @cluster.topics['test.4'].partitions[2].expects(:isr).returns([@cluster.brokers[1]])

    assert @cluster.brokers[1].critical?(replicas: 1), "Final remaining broker for this partition's ISR set, cannot lose"
    refute @cluster.brokers[2].critical?(replicas: 1), "Not related to the under-replicated partitions"
    refute @cluster.brokers[3].critical?(replicas: 1), "Already down, so not critical"
  end

  def test_from_json
    json_payload = '{"jmx_port":9999,"timestamp":"1431719964125","host":"kafka03.example.com","version":1,"port":9092}'
    broker = Kazoo::Broker.from_json(mock('cluster'), 3, JSON.parse(json_payload))

    assert_equal 3, broker.id
    assert_equal 'kafka03.example.com', broker.host
    assert_equal 9092, broker.port
    assert_equal 9999, broker.jmx_port
    assert_equal "kafka03.example.com:9092", broker.addr
  end

  def test_replicated_partitions
    assert_equal 3, @cluster.brokers[1].replicated_partitions.length
    assert_equal 4, @cluster.brokers[2].replicated_partitions.length
    assert_equal 3, @cluster.brokers[3].replicated_partitions.length
  end

  def test_led_partitions
    assert_equal 2, @cluster.brokers[1].led_partitions.length
    assert_equal 2, @cluster.brokers[2].led_partitions.length
    assert_equal 1, @cluster.brokers[3].led_partitions.length
  end
end

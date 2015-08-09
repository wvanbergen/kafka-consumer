require 'zookeeper'
require 'json'
require 'thread'
require 'socket'
require 'securerandom'

module Kazoo
  Error = Class.new(StandardError)
  ConsumerInstanceRegistrationFailed = Class.new(Kazoo::Error)
  PartitionAlreadyClaimed = Class.new(Kazoo::Error)
  ReleasePartitionFailure = Class.new(Kazoo::Error)

end

require 'kazoo/cluster'
require 'kazoo/broker'
require 'kazoo/topic'
require 'kazoo/partition'
require 'kazoo/consumergroup'
require 'kazoo/version'

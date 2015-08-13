require "bundler/gem_tasks"
require "kafka/consumer"
require "rake/testtask"
require "benchmark"

Rake::TestTask.new do |t|
  t.libs = ["lib", "test"]
  t.test_files = FileList['test/*_test.rb']
end

namespace :kafka do
  task :consumer do
    zookeeper = ENV["ZOOKEEPER"] or raise "Specify the ZOOKEEPER connection string."
    name      = ENV["NAME"]      or raise "Specify NAME to name the consumergroup."
    topics    = ENV["TOPICS"]    or raise "Specify the TOPICS you want to consume. Use comma as separator."

    consumer = Kafka::Consumer.new(name, topics.split(','), zookeeper: zookeeper, initial_offset: :earliest_offset)

    Signal.trap("TERM") { puts "TERM received"; consumer.interrupt }
    Signal.trap("INT")  { puts "INT received";  consumer.interrupt }

    counter = 0
    duration = Benchmark.realtime do
      consumer.each do |event|
        counter += 1
        print "Consumed #{counter} messages.\n" if counter % 1000 == 0
      end
    end

    puts
    puts "%d messages consumed in %0.3fs (%0.3f msg/s)" % [counter, duration, counter.to_f / duration]
  end

  namespace :consumer do
    task :reset do
      zookeeper = ENV["ZOOKEEPER"] or raise "Specify the ZOOKEEPER connection string."
      name      = ENV["NAME"]      or raise "Specify NAME to name the consumergroup."

      consumer = Kafka::Consumer.new(name, [], zookeeper: zookeeper)
      consumer.group.reset_offsets
    end
  end
end

task default: :test

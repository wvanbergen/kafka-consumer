# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kafka/consumer/version'

Gem::Specification.new do |spec|
  spec.name          = "kafka-consumer"
  spec.version       = Kafka::Consumer::VERSION
  spec.authors       = ["Willem van Bergen"]
  spec.email         = ["willem@vanbergen.org"]
  spec.summary       = %q{High-level consumer for Kafka}
  spec.description   = %q{High-level consumer for Kafka. Implements the Zookeeper-backed consumer implementation that offers offset management, load balancing and automatic failovers.}
  spec.homepage      = "https://github.com/wvanbergen/kafka-consumer"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "mocha", "~> 1.0"

  spec.add_runtime_dependency "poseidon", "~> 0.0.5"
  spec.add_runtime_dependency "kazoo-ruby", "~> 0.1"
end

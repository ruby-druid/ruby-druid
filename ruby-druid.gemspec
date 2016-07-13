lib = File.expand_path("../lib", __FILE__)
$:.unshift(lib) unless $:.include?(lib)

require "druid/version"

Gem::Specification.new do |spec|
  spec.name          = "ruby-druid"
  spec.version       = Druid::VERSION
  spec.authors       = ["Ruby Druid Community"]
  spec.summary       = %q{Ruby client for Druid}
  spec.description   = %q{Ruby client for Druid}
  spec.homepage      = "https://github.com/ruby-druid/ruby-druid"
  spec.license       = "MIT"

  spec.files = Dir["lib/**/*"] + %w{LICENSE README.md ruby-druid.gemspec}
  spec.test_files = Dir["spec/**/*"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activesupport", "~> 4.2"
  spec.add_dependency "activemodel", "~> 4.2"
  spec.add_dependency "iso8601", "~> 0.9"
  spec.add_dependency "multi_json", "~> 1.12"
  spec.add_dependency "rest-client", "~> 2.0"
  spec.add_dependency "zk", "~> 1.9"
  spec.add_development_dependency "rake", "~> 11.2"
  spec.add_development_dependency "rspec", "~> 3.4"
  spec.add_development_dependency "webmock", "~> 2.1"
end

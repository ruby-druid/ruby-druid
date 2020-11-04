lib = File.expand_path("../lib", __FILE__)
$:.unshift(lib) unless $:.include?(lib)

require "druid/version"

Gem::Specification.new do |spec|
  spec.name          = "ruby-druid"
  spec.version       = Druid::VERSION
  spec.authors       = ["Ruby Druid Community"]
  spec.summary       = %q{A Ruby client for Druid}
  spec.description   = <<-EOF
    ruby-druid is a Ruby client for Druid. It includes a Squeel-like query DSL
    and generates a JSON query that can be sent to Druid directly.
  EOF
  spec.homepage      = "https://github.com/ruby-druid/ruby-druid"
  spec.license       = "MIT"

  spec.files = Dir["lib/**/*"] + %w{LICENSE README.md ruby-druid.gemspec}
  spec.test_files = Dir["spec/**/*"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activesupport", ">= 3.0.0"
  spec.add_dependency "activemodel", ">= 3.0.0"
  spec.add_dependency "iso8601", "~> 0.8"
  spec.add_dependency "multi_json", "~> 1.0"
  spec.add_dependency "rest-client", ">= 1.8", "< 3.0"
  spec.add_dependency "zk", "~> 1.9"
  spec.add_development_dependency "bundler", ">= 1.3.0", "< 2.2"
  spec.add_development_dependency "rake", "~> 11.2"
  spec.add_development_dependency "rspec", "~> 3.4"
  spec.add_development_dependency "webmock", "~> 2.1"
end

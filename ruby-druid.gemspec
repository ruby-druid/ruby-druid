# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "ruby-druid"
  spec.version       = "0.2.0.rc2"
  spec.authors       = `git log --format='%aN' | sort -u`.split($/)
  spec.summary       = %q{Ruby client for Druid}
  spec.description   = %q{Ruby client for Druid}
  spec.homepage      = "https://github.com/ruby-druid/ruby-druid"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "activesupport"
  spec.add_dependency "activemodel"
  spec.add_dependency "iso8601"
  spec.add_dependency "multi_json"
  spec.add_dependency "rest-client"
  spec.add_dependency "zk"
end

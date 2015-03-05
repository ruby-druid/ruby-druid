require 'bundler/setup'
require 'liquid/boot'
require 'liquid/tasks'

Dir[ File.join(File.dirname(__FILE__), 'tasks', '*.rake') ].sort.each do |f|
  load f
end

task default: :spec

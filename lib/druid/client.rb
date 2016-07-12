require 'druid/zk'
require 'druid/data_source'

module Druid
  class Client

    attr_reader :zk

    def initialize(zookeeper, opts = {})
      @zk = ZK.new(zookeeper, opts)
    end

    def data_source(source)
      uri = @zk.data_sources[source]
      Druid::DataSource.new(source, uri)
    end

    def data_sources
      @zk.data_sources
    end

  end
end

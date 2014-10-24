require 'druid/zk'
require 'druid/data_source'

module Druid
  class Client

    def initialize(zookeeper, opts = {})
      @zk = ZK.new(zookeeper, opts)
    end

    def data_source(source)
      uri = @zk.data_sources[source]
      return nil unless uri
      Druid::DataSource.new(source, uri)
    end

  end
end

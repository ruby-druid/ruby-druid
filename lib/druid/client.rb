require 'timeout'
require 'socket'

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
      return nil unless self.class.reachable?(uri)
      Druid::DataSource.new(source, uri)
    end

    def self.reachable?(uri)
      uri = URI(uri)
      Timeout::timeout(1) do
        TCPSocket.new(uri.host, uri.port).close
      end
      return true
    rescue => e
      return false
    end
  end
end

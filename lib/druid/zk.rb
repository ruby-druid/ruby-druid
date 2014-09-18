require 'zk'
require 'json'
require 'rest_client'

module Druid
  class ZK
    def initialize(uri, opts = {})
      @zk = ::ZK.new(uri, chroot: :check)
      @registry = Hash.new { |hash, key| hash[key] = Array.new }
      @discovery_path = opts[:discovery_path] || '/discovery'
      @watched_services = Hash.new
      register
    end

    def register
      @zk.on_expired_session { register }
      @zk.register(@discovery_path, only: :child) do |event|
        check_services
      end
      check_services
    end

    def close!
      @zk.close!
    end

    def register_service(service, brokers)
      # poor mans load balancing
      @registry[service] = brokers.shuffle
    end

    def unregister_service(service)
      @registry.delete(service)
      unwatch_service(service)
    end

    def watch_service(service)
      return if @watched_services.include?(service)
      watch = @zk.register(watch_path(service), only: :child) do |event|
        unwatch_service(service)
        check_service(service)
      end
      @watched_services[service] = watch
    end

    def unwatch_service(service)
      return unless @watched_services.include?(service)
      @watched_services.delete(service).unregister
    end

    def check_services
      zk_services = @zk.children(@discovery_path, watch: true)

      (services - zk_services).each do |service|
        unregister_service(service)
      end

      zk_services.each do |service|
        check_service(service)
      end
    end

    def verify_broker(service, name)
      info = @zk.get("#{watch_path(service)}/#{name}")
      node = JSON.parse(info[0])
      uri = "http://#{node['address']}:#{node['port']}/druid/v2/"
      check = RestClient::Request.execute({
        method: :get, url: "#{uri}datasources/",
        timeout: 5, open_timeout: 5
      })
      return [uri, JSON.parse(check.to_str)] if check.code == 200
    rescue RestClient::ResourceNotFound
      return false
    end

    def watch_path(service)
      "#{@discovery_path}/#{service}"
    end

    def check_service(service)
      return if @watched_services.include?(service)

      watch_service(service)

      known = @registry[service].map { |node| node[:name] }
      live = @zk.children(watch_path(service), watch: true)
      new_list = @registry[service].select { |node| live.include?(node[:name]) }

      # verify the new entries to be living brokers
      (live - known).each do |name|
        uri, sources = verify_broker(service, name)
        new_list.push({ name: name, uri: uri, data_sources: sources }) if uri
      end

      if new_list.empty?
        # don't show services w/o active brokers
        unregister_service(service)
      else
        register_service(service, new_list)
      end
    end

    def services
      @registry.keys
    end

    def data_sources
      result = Hash.new { |hash, key| hash[key] = [] }

      @registry.each do |service, brokers|
        brokers.each do |broker|
          broker[:data_sources].each do |data_source|
            result["#{service}/#{data_source}"] << broker[:uri]
          end
        end
      end

      result.each do |source, uris|
        result[source] = uris.sample if uris.respond_to?(:sample)
      end

      result
    end

    def to_s
      @registry.to_s
    end
  end
end

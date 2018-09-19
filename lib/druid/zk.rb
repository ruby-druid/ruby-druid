require 'zk'
require 'multi_json'
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
      $log.debug("druid.zk register discovery path") if $log
      @zk.on_expired_session { register }
      @zk.register(@discovery_path, only: :child) do |event|
        $log.debug("druid.zk got event on discovery path") if $log
        check_services
      end
      check_services
    end

    def close!
      $log.debug("druid.zk shutting down") if $log
      @zk.close!
    end

    def register_service(service, brokers)
      $log.debug("druid.zk register", service: service, brokers: brokers) if $log
      # poor mans load balancing
      @registry[service] = brokers.shuffle
    end

    def unregister_service(service)
      $log.debug("druid.zk unregister", service: service) if $log
      @registry.delete(service)
      unwatch_service(service)
    end

    def watch_service(service)
      return if @watched_services.include?(service)
      $log.debug("druid.zk watch", service: service) if $log
      watch = @zk.register(watch_path(service), only: :child) do |event|
        $log.debug("druid.zk got event on watch path for", service: service, event: event) if $log
        unwatch_service(service)
        check_service(service)
      end
      @watched_services[service] = watch
    end

    def unwatch_service(service)
      return unless @watched_services.include?(service)
      $log.debug("druid.zk unwatch", service: service) if $log
      @watched_services.delete(service).unregister
    end

    def check_services
      $log.debug("druid.zk checking services") if $log
      zk_services = @zk.children(@discovery_path, watch: true)

      (services - zk_services).each do |service|
        unregister_service(service)
      end

      zk_services.each do |service|
        check_service(service)
      end
    end

    def verify_broker(service, name)
      $log.debug("druid.zk verify", broker: name, service: service) if $log
      info = @zk.get("#{watch_path(service)}/#{name}")
      node = MultiJson.load(info[0])
      uri = "http://#{node['address']}:#{node['port']}/druid/v2/"
      check = RestClient::Request.execute({
        method: :get, url: "#{uri}datasources/",
        timeout: 5, open_timeout: 5
      })
      $log.debug("druid.zk verified", uri: uri, sources: check) if $log
      return [uri, MultiJson.load(check.to_str)] if check.code == 200
    rescue
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
      $log.debug("druid.zk checking", service: service, known: known, live: live, new_list: new_list) if $log

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

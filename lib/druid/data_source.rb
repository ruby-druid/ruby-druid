require 'multi_json'
require 'iso8601'

module Druid
  class DataSource

    attr_reader :name, :uri, :metrics, :dimensions

    def initialize(name, uri, http_params: {})
      @name = name.split('/').last
      uri = uri.sample if uri.is_a?(Array)
      if uri.is_a?(String)
        @uri = URI(uri)
      else
        @uri = uri
      end
      @http_params = http_params
    end

    def metadata
      @metadata ||= metadata!
    end

    def metadata!(opts = {})
      meta_path = "#{@uri.path}datasources/#{name}"

      if opts[:interval]
        from, to = opts[:interval]
        from = from.respond_to?(:iso8601) ? from.iso8601 : ISO8601::DateTime.new(from).to_s
        to = to.respond_to?(:iso8601) ? to.iso8601 : ISO8601::DateTime.new(to).to_s

        meta_path += "?interval=#{from}/#{to}"
      end

      req = Net::HTTP::Get.new(meta_path)
      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.open_timeout = 10 # if druid is down fail fast
        http.read_timeout = nil # we wait until druid is finished
        if @http_params
          for param, value in @http_params
            http.send("#{param}=", value)
          end
        end
        http.request(req)
      end

      if response.code != '200'
        raise "Request failed: #{response.code}: #{response.body}"
      end

      MultiJson.load(response.body)
    end

    def metrics
      @metrics ||= metadata['metrics']
    end

    def dimensions
      @dimensions ||= metadata['dimensions']
    end

    def post(query)
      query = query.query if query.is_a?(Druid::Query::Builder)
      query = Query.new(MultiJson.load(query)) if query.is_a?(String)
      query.dataSource = name

      req = Net::HTTP::Post.new(uri.path, { 'Content-Type' => 'application/json' })
      query_as_json = query.as_json
      req.body = MultiJson.dump(query_as_json)


      response = ActiveSupport::Notifications.instrument('post.druid', data_source: name, query: query_as_json) do
        Net::HTTP.new(uri.host, uri.port).start do |http|
          http.open_timeout = 10 # if druid is down fail fast
          http.read_timeout = nil # we wait until druid is finished
          if @http_params
            for param, value in @http_params
              http.send("#{param}=", value)
            end
          end
          http.request(req)
        end
      end

      if response.code != '200'
        # ignore GroupBy cache issues and try again without cached results
        if query.context.useCache != false && response.code == "500" && response.body =~ /Cannot have a null result!/
          query.context.useCache = false
          return self.post(query)
        end

        raise Error.new(response)
      end

      MultiJson.load(response.body)
    end

    class Error < StandardError
      attr_reader :error, :error_message, :error_class, :host, :response

      def initialize(response)
        @response = response
        parsed_body = MultiJson.load(response.body)
        @error, @error_message, @error_class, @host = parsed_body.values_at(*%w(
          error
          errorMessage
          errorClass
          host
        ))
      end

      def message
        error
      end

      def query_timeout?
        error == 'Query timeout'.freeze
      end

      def query_interrupted?
        error == 'Query interrupted'.freeze
      end

      def query_cancelled?
        error == 'Query cancelled'.freeze
      end

      def resource_limit_exceeded?
        error == 'Resource limit exceeded'.freeze
      end

      def unknown_exception?
        error == 'Unknown exception'.freeze
      end
    end
  end
end

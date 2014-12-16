require 'multi_json'

module Druid
  class DataSource

    attr_reader :name, :uri, :metrics, :dimensions

    def initialize(name, uri)
      @name = name
      @uri = uri
    end

    def uri
      @uri = URI(@uri) if @uri.is_a?(String)
      @uri
    end

    def metadata
      return @metadata unless @metadata.nil?

      meta_path = "#{uri.path}datasources/#{name.split('/').last}"
      req = Net::HTTP::Get.new(meta_path)
      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = 60_000 # ms
        http.request(req)
      end

      if response.code != "200"
        raise "Request failed: #{response.code}: #{response.body}"
      end

      @metadata = MultiJson.load(response.body)
    end

    def metrics
      @metrics ||= metadata['metrics']
    end

    def dimensions
      @dimensions ||= metadata['dimensions']
    end

    def send(query)
      query.data_source(name)

      req = Net::HTTP::Post.new(uri.path, {'Content-Type' => 'application/json'})
      req.body = query.to_json

      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = 60_000 # ms
        http.request(req)
      end

      if response.code != "200"
        raise "Request failed: #{response.code}: #{response.body}"
      end

      MultiJson.load(response.body).map do |row|
        ResponseRow.new(row)
      end
    end

    def query
      Query.new(self)
    end

  end
end

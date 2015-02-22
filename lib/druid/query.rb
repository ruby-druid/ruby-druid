require 'time'
require 'iso8601'

require 'active_support/all'

require 'druid/filter'
require 'druid/having'
require 'druid/post_aggregation'

module Druid
  class Query < ::Hash
    include Serializable

    attr_reader :source # needed for console magic

    def self.new(source = nil)
      query = self.allocate
      query.send(:initialize, source)
      query
    end

    def initialize(source = nil)
      @source = source
      granularity(:all)
      interval(Time.now.utc.beginning_of_day)
    end

    def interval(from, to = Time.now)
      intervals([[from, to]])
    end

    def intervals(is)
      self[:intervals] = is.map do |from, to|
        from = from.respond_to?(:iso8601) ? from.iso8601 : ISO8601::DateTime.new(from).to_s
        to = to.respond_to?(:iso8601) ? to.iso8601 : ISO8601::DateTime.new(to).to_s
        "#{from}/#{to}"
      end
      self
    end

    def granularity(gran, time_zone = "UTC")
      gran = gran.to_s
      if %w(all none minute fifteen_minute thirty_minute hour day).include?(gran)
        self[:granularity] = gran
      else
        self[:granularity] = {
          type: 'period',
          period: gran,
          timeZone: time_zone
        }
      end
      self
    end

    ## query types
    def metadata
      query_type(:segmentMetadata)
      self.delete :granularity
      self
    end


    def search(what = "",dimensions = [], limit = nil)
      query_type(:search)
      self[:searchDimensions] = dimensions unless dimensions.empty?
      self[:limit] = limit if  limit
      # for now we always sort lexicographic
      self[:sort] = {
        type: "lexicographic"
      }
      self[:query] = {
        type: "insensitive_contains",
        value: what
      }
      self
    end

    def search?
      self[:queryType] == :search
    end

    def query_type(type)
      self[:queryType] = type
      self
    end

    def data_source(source)
      self[:dataSource] = source.split('/').last
      self
    end

    def group_by(*dimensions)
      query_type(:groupBy)
      self[:dimensions] = dimensions.flatten
      self
    end

    def topn(dimension, metric, threshold)
      query_type(:topN)
      self[:dimension] = dimension
      self[:metric] = metric
      self[:threshold] = threshold
      self
    end

    def time_series
      query_type(:timeseries)
      self
    end

    [:long_sum, :double_sum, :count, :min, :max, :hyper_unique].each do |method_name|
      aggregation_type = method_name.to_s.camelize(:lower)
      define_method method_name do |*metrics|
        metrics.flatten.each do |metric|
          aggregate(aggregation_type, metric)
        end
        self
      end
    end

    def cardinality(metric, dimensions, by_row = true)
      aggregate(:cardinality, metric,
        field_names: dimensions,
        by_row: by_row
      )
    end

    def js_aggregation(metric, columns, functions)
      aggregate(:javascript, metric,
        field_names: columns,
        fn_aggregate: functions[:aggregate],
        fn_combine: functions[:combine],
        fn_reset: functions[:reset]
      )
    end

    def aggregate(agg_type, metric, options = {})
      @properties[:aggregations] ||= []

      unless contains_aggregation?(metric)
        @properties[:aggregations] << build_aggregation(agg_type, metric, options)
      end

      self
    end

    def build_aggregation(agg_type, metric, options = {})
      options = {
        type: to_druid_notation(agg_type),
        name: metric.to_s
      }.merge(
        Hash[options.map { |k, v| [to_druid_notation(k).to_sym, v] }]
      )

      options[:fieldName] ||= metric.to_s if !options[:fieldNames] && agg_type != :filtered
      options
    end

    alias_method :sum, :long_sum

    def cardinality(metric, dimensions, by_row = false)
      aggregate(:cardinality, metric,
        fieldNames: dimensions,
        byRow: by_row
      )
    end

    def js_aggregation(metric, columns, functions)
      aggregate(:javascript, metric,
        fieldNames: columns,
        fnAggregate: functions[:aggregate],
        fnCombine: functions[:combine],
        fnReset: functions[:reset]
      )
    end

    def aggregate(type, metric, options = {})
      self[:aggregations] ||= []
      return self if self[:aggregations].any?{|agg| agg[:fieldName].to_s == metric.to_s}
      self[:aggregations] << build_aggregation(type.to_s, metric, options)
      self
    end

    def build_aggregation(type, metric, options = {})
      options[:fieldName] ||= metric.to_s unless options[:fieldNames] || %w(cardinality filtered javascript).include?(type.to_s)
      { type: type.to_s, name: metric.to_s }.merge(options)
    end

    ## post aggregations

    def postagg(type = :long_sum, &block)
      post_agg = PostAggregation.new.instance_exec(&block)
      self[:postAggregations] ||= []
      self[:postAggregations] << post_agg
      # make sure, the required fields are in the query
      self.method(type).call(post_agg.get_field_names)
      self
    end

    ## filters

    def filter(hash = nil, type = :in, &block)
      filter_from_hash(hash, type) if hash
      filter_from_block(&block) if block
      self
    end

    def filter_from_hash(hash, type = :in)
      last = nil
      hash.each do |k, values|
        filter = FilterDimension.new(k).__send__(type, values)
        last = last ? last.&(filter) : filter
      end
      self[:filter] = self[:filter] ? self[:filter].&(last) : last
    end

    def filter_from_block(&block)
      filter = Filter.new.instance_exec(&block)
      raise "Not a valid filter" unless filter.is_a? FilterParameter
      self[:filter] = self[:filter] ? self[:filter].&(filter) : filter
    end

    ## having
    def having(hash = nil, &block)
      having_from_hash(hash) if hash
      having_from_block(&block) if block
      self
    end

    def having_from_block(&block)
      chain_having Having.new.instance_exec(&block)
    end

    def having_from_hash(h)
      chain_having HavingClause.from_h(h)
    end

    def chain_having(having)
      having = self[:having].chain(having) if self[:having]
      self[:having] = having
      self
    end

    ## limit/sort

    def limit(limit, columns)
      self[:limitSpec] = {
        type: :default,
        limit: limit,
        columns: columns.map do |dimension, direction|
          { dimension: dimension, direction: direction }
        end
      }
      self
    end

    private

    def to_druid_notation(string)
      string.to_s.split('_').
        each_with_index.map { |v, i| i == 0 ? v : v.capitalize }.
        join
    end

    def order_by_column_spec(columns)
      columns.map do |dimension, direction|
        {
          :dimension => dimension,
          :direction => direction
        }
      end
    end

    def contains_aggregation?(metric)
      return false if @properties[:aggregations].nil?
      @properties[:aggregations].index { |aggregation| aggregation[:name] == metric.to_s }
    end

  end
end

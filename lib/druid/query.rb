require 'time'
require 'iso8601'

require 'active_support/all'
require 'active_model'

require 'druid/granularity'
require 'druid/dimension'
require 'druid/aggregation'
require 'druid/post_aggregation'
require 'druid/filter'
require 'druid/context'
require 'druid/having'

module Druid
  class Query
    include ActiveModel::Model

    attr_accessor :queryType
    validates :queryType, inclusion: { in: %w(timeseries search timeBoundary groupBy segmentMetadata select topN dataSourceMetadata) }

    attr_accessor :dataSource
    validates :dataSource, presence: true

    class IntervalsValidator < ActiveModel::EachValidator
      def validate_each(record, attribute, value)
        if !value.is_a?(Array) || value.blank?
          record.errors.add(attribute, 'must be a list with at least one interval')
          return
        end
        value.each do |interval|
          parts = interval.to_s.split('/')
          record.errors.add(attribute, 'must consist of two ISO8601 dates seperated by /') unless parts.length == 2
          parts = parts.map do |ts|
            ISO8601::DateTime.new(ts) rescue nil
          end
          record.errors.add(attribute, 'must consist of valid ISO8601 dates') unless parts.all?
          record.errors.add(attribute, 'first date needs to be < second date') unless parts.first.to_time < parts.last.to_time
        end
      end
    end

    attr_accessor :intervals
    validates :intervals, intervals: true

    class GranularityValidator < ActiveModel::EachValidator
      TYPES = %w(timeseries search groupBy select topN)
      SIMPLE = %w(all none minute fifteen_minute thirty_minute hour day)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.queryType)
          if value.is_a?(String)
            record.errors.add(attribute, "must be one of #{SIMPLE.inspect}") unless SIMPLE.include?(value)
          elsif value.is_a?(Granularity)
            value.valid? # trigger validation
            value.errors.messages.each do |k, v|
              record.errors.add(attribute, { k => v })
            end
          else
            record.errors.add(attribute, "invalid type or class: #{value.inspect}")
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.queryType}") if value
        end
      end
    end

    attr_accessor :granularity
    validates :granularity, granularity: true

    def granularity=(value)
      if value.is_a?(String)
        @granularity = value
      elsif value.is_a?(Hash)
        @granularity = Granularity.new(value)
      else
        @granularity = value
      end
    end

    class DimensionsValidator < ActiveModel::EachValidator
      TYPES = %w(groupBy select)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.queryType)
          if !value.is_a?(Array) || value.blank?
            record.errors.add(attribute, 'must be a list with at least one dimension')
          else
            value.each(&:valid?) # trigger validation
            value.each do |avalue|
              avalue.errors.messages.each do |k, v|
                record.errors.add(attribute, { k => v })
              end
            end
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.queryType}") if value
        end
      end
    end

    attr_accessor :dimensions
    validates :dimensions, dimensions: true

    def dimensions
      @dimensions ||= []
    end

    def dimensions=(value)
      if value.is_a?(Array)
        @dimensions = value.map do |x|
          x.is_a?(Dimension) ? x : Dimension.new(x)
        end
      else
        @dimensions = [
          value.is_a?(Dimension) ? value : Dimension.new(value)
        ]
      end
    end

    class AggregationsValidator < ActiveModel::EachValidator
      TYPES = %w(timeseries groupBy topN)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.queryType)
          if !value.is_a?(Array) || value.blank?
            record.errors.add(attribute, 'must be a list with at least one aggregator')
          else
            value.each(&:valid?) # trigger validation
            value.each do |avalue|
              avalue.errors.messages.each do |k, v|
                record.errors.add(attribute, { k => v })
              end
            end
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.queryType}") if value
        end
      end
    end

    attr_accessor :aggregations
    validates :aggregations, aggregations: true

    def aggregations
      @aggregations ||= []
    end

    def aggregations=(value)
      if value.is_a?(Array)
        @aggregations = value.map do |x|
          x.is_a?(Aggregation) ? x : Aggregation.new(x)
        end
      else
        @aggregations = [
          value.is_a?(Aggregation) ? value : Aggregation.new(value)
        ]
      end
    end

    def aggregation_types
      Set.new(@aggregations.map do |aggregation|
        aggregation.type
      end.flatten.compact)
    end

    def aggregation_names
      Set.new(@aggregations.map do |aggregation|
        [aggregation.fieldName] + [aggregation.fieldNames]
      end.flatten.compact)
    end

    class PostaggregationsValidator < ActiveModel::EachValidator
      TYPES = %w(timeseries groupBy topN)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.queryType)
          value.each(&:valid?) # trigger validation
          value.each do |avalue|
            avalue.errors.messages.each do |msg|
              record.errors.add(attribute, msg)
            end
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.queryType}") if value
        end
      end
    end

    attr_accessor :postAggregations
    validates :postAggregations, postaggregations: true

    def postAggregations
      @postAggregations ||= []
    end

    def postAggregations=(value)
      if value.is_a?(Array)
        @postAggregations = value.map do |x|
          PostAggregation.new(x)
        end
      else
        @postAggregations = [value]
      end
    end

    class FilterValidator < ActiveModel::EachValidator
      TYPES = %w(timeseries search groupBy select topN)
      def validate_each(record, attribute, value)
        if value && TYPES.include?(record.queryType)
          value.valid? # trigger validation
          value.errors.messages.each do |k, v|
            record.errors.add(attribute, { k => v })
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.queryType}") if value
        end
      end
    end

    attr_accessor :filter
    validates :filter, filter: true

    def filter=(value)
      if value.is_a?(Hash)
        @filter = Filter.new(value)
      else
        @filter = value
      end
    end

    # groupBy
    attr_accessor :having

    def having=(value)
      if value.is_a?(Hash)
        @having = Having.new(value)
      else
        @having = value
      end
    end

    # groupBy
    attr_accessor :limitSpec

    # search
    attr_accessor :limit

    # search
    attr_accessor :searchDimensions

    # search
    attr_accessor :query

    # search
    attr_accessor :sort

    # timeBoundary
    attr_accessor :bound

    # segementMetadata
    attr_accessor :toInclude

    # segementMetadata
    attr_accessor :merge

    # select
    attr_accessor :metrics

    # select
    attr_accessor :pagingSpec

    # topN
    attr_accessor :dimension

    # topN
    attr_accessor :metric

    # topN
    attr_accessor :threshold

    attr_accessor :context

    def context=(value)
      if value.is_a?(Hash)
        @context = Context.new(value)
      else
        @context = value
      end
    end

    def initialize(attributes = {})
      super
      @context ||= Context.new
    end

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end

    def contains_aggregation?(metric)
      aggregations.any? { |a| a.name.to_s == metric.to_s }
    end

    class Builder

      attr_reader :query

      def initialize
        @query = Query.new
        query_type(:timeseries)
        interval(Time.now.utc.beginning_of_day)
      end

      def query_type(type)
        @query.queryType = type.to_s
        self
      end

      def data_source(source)
        @query.dataSource = source.split('/').last
        self
      end

      def interval(from, to = Time.now)
        intervals([[from, to]])
      end

      def intervals(is)
        @query.intervals = is.map do |from, to|
          from = from.respond_to?(:iso8601) ? from.iso8601 : ISO8601::DateTime.new(from).to_s
          to = to.respond_to?(:iso8601) ? to.iso8601 : ISO8601::DateTime.new(to).to_s
          "#{from}/#{to}"
        end
        self
      end

      def last(duration)
        interval(Time.now - duration)
      end

      def granularity(gran, time_zone = "UTC")
        gran = gran.to_s
        if %w(all none minute fifteen_minute thirty_minute hour day).include?(gran)
          @query.granularity = gran
        else
          @query.granularity = Granularity.new({
            type: 'period',
            period: gran,
            timeZone: time_zone
          })
        end
        self
      end

      ## query types

      def metadata
        query_type(:segmentMetadata)
        @query.context.useCache = false
        @query.context.populateCache = false
        self
      end

      def timeseries
        query_type(:timeseries)
        self
      end

      def group_by(*dimensions)
        query_type(:groupBy)
        @query.dimensions = dimensions.flatten.map do |dimension|
          dimension.is_a?(Dimension) ? dimension : Dimension.new(dimension)
        end
        self
      end

      def topn(dimension, metric, threshold)
        query_type(:topN)
        @query.dimension = dimension
        @query.metric = metric
        @query.threshold = threshold
        self
      end

      def search(what = "", dimensions = [], limit = nil)
        query_type(:search)
        @query.searchDimensions = dimensions unless dimensions.empty?
        @query.limit = limit if limit
        # for now we always sort lexicographic
        @query.sort = { type: 'lexicographic' }
        @query.query = {
          type: "insensitive_contains",
          value: what
        }
        self
      end

      ### aggregations
      %i[count long_sum double_sum min max hyper_unique double_first double_last
         float_first float_last long_first long_last].each do |method_name|
        define_method method_name do |*metrics|
          metrics.flatten.compact.each do |metric|
            @query.aggregations << Aggregation.new({
              type: method_name.to_s.camelize(:lower),
              name: metric,
              fieldName: metric,
            }) unless @query.contains_aggregation?(metric)
          end
          self
        end
      end

      def histograms(metrics)
        metrics.each{|m| histogram(m) }
        self
      end

      def histogram(metric, type = "equalBuckets", args = {})
        @query.aggregations << Aggregation.new({
          type: "approxHistogramFold",
          name: "raw_#{metric}",
          fieldName: metric,
        })
        type = type.dup
        type[0] = type[0].upcase
        options = args.dup.merge({
          name: metric,
          fieldName: "raw_#{metric}"
        })
        @query.postAggregations << ::Druid.const_get("PostAggregationHistogram#{type}").new(options)
        self
      end

      alias_method :sum, :long_sum

      def cardinality(metric, dimensions, by_row = false)
        @query.aggregations << Aggregation.new({
          type: 'cardinality',
          name: metric,
          fieldNames: dimensions,
          byRow: by_row,
        }) unless @query.contains_aggregation?(metric)
        self
      end

      def js_aggregation(metric, columns, functions)
        @query.aggregations << Aggregation.new({
          type: 'javascript',
          name: metric,
          fieldNames: columns,
          fnAggregate: functions[:aggregate],
          fnCombine: functions[:combine],
          fnReset: functions[:reset],
        }) unless @query.contains_aggregation?(metric)
        self
      end

      def filtered_aggregation(metric, name, aggregation_type, &filter)
        @query.aggregations << Aggregation.new(
          type: 'filtered',
          filter: Filter.new.instance_exec(&filter),
          aggregator: Aggregation.new(
            type: aggregation_type.to_s.camelize(:lower),
            name: name,
            fieldName: metric
          )
        ) unless @query.contains_aggregation?(metric)
        self
      end

      ## post aggregations

      def postagg(type = :long_sum, &block)
        post_agg = PostAggregation.new.instance_exec(&block)
        @query.postAggregations << post_agg
        # make sure, the required fields are in the query
        self.method(type).call(post_agg.field_names)
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
          filter = DimensionFilter.new(dimension: k).__send__(type, values)
          last = last ? last.&(filter) : filter
        end
        @query.filter = @query.filter ? @query.filter.&(last) : last
      end

      def filter_from_block(&block)
        filter = Filter.new.instance_exec(&block)
        @query.filter = @query.filter ? @query.filter.&(filter) : filter
      end

      ## having

      def having(hash = nil, &block)
        having_from_hash(hash) if hash
        having_from_block(&block) if block
        self
      end

      def having_from_block(&block)
        chain_having(Having.new.instance_exec(&block))
      end

      def having_from_hash(h)
        chain_having(Having.new(h))
      end

      def chain_having(having)
        having = @query.having.chain(having) if @query.having
        @query.having = having
        self
      end

      ### limit/sort

      def limit(limit, columns)
        @query.limitSpec = {
          type: :default,
          limit: limit,
          columns: columns.map do |dimension, direction|
            { dimension: dimension, direction: direction }
          end
        }
        self
      end
    end

  end
end

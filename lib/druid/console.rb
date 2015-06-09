require 'active_support/time'
require 'ap'
require 'forwardable'
require 'irb'
require 'ripl'
require 'terminal-table'

require 'druid'

Ripl::Shell.class_eval do
  def _result_key(queryType)
    case queryType.to_sym
    when :timeseries
      "result"
    when :groupBy
      "event"
    end
  end

  def _format_query_result(result, query)
    ap(query.as_json)
    ap(result.as_json)
    return nil if result.empty?
    case query.queryType
    when 'timeseries', 'groupBy'
      _format_timeseries_result(result, query)
    when 'segmentMetadata'
      _format_segment_metadata_result(result, query)
    end
  end

  def _format_timeseries_result(result, query)
    result_key = _result_key(query.queryType)
    keys = result.last[result_key].keys
    grouped_result = result.group_by { |x| x['timestamp'] }
    Terminal::Table.new(:headings => keys) do
      grouped_result.each do |timestamp, rows|
        add_row(:separator) unless timestamp == result.first['timestamp']
        add_row([{ :value => timestamp, :colspan => keys.length }])
        add_row(:separator)
        rows.each { |row| add_row(keys.map { |key| row[result_key][key] }) }
      end
    end
  end

  def _format_segment_metadata_result(result, query)
    columns = result.map do |row|
      row['columns'].keys
    end.to_a.flatten.uniq.sort
    Terminal::Table.new(:headings => columns) do
    end
  end

  def format_result(result)
    if result.is_a?(Druid::Query::Builder)
      start = Time.now.to_f
      response = $source.post(result.query)
      rt = Time.now.to_f - start
      puts _format_query_result(response, result.query)
      puts "Response Time: #{rt.round(3)}s"
    else
      ap(result)
    end
  end
end

module Druid
  class Console
    extend Forwardable

    def initialize
      client # trigger connect
      Ripl.start(binding: binding)
    end

    def client
      @client ||= Druid::Client.new(opts[:zookeeper])
    end

    def source(name = nil)
      client.data_source(name || opts[:source])
    end

    def dimensions
      source.dimensions.sort
    end

    def metrics
      source.metrics.sort
    end

    def query(name = nil)
      $source = source(name)
      Query::Builder.new
    end

    def_delegators(*[
      :query,
      :query_type,
      :interval,
      :granularity,
      :metadata,
      :timeseries,
      :group_by,
      :topn,
      :search,
      :count,
      :sum,
      :long_sum,
      :double_sum,
      :min,
      :max,
      :hyper_unique,
      :cardinality,
      :js_aggregation,
      :postagg,
      :filter,
      :having,
      :limit,
    ])

  end
end

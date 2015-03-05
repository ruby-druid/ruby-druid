require 'active_support/time'
require 'ap'
require 'forwardable'
require 'irb'
require 'ripl'
require 'terminal-table'

require 'druid'

Ripl::Shell.class_eval do
  def _result_key(queryType)
    case queryType
    when :timeseries
      "result"
    when :groupBy
      "event"
    end
  end

  def _format_query_result(result, query)
    ap(query.as_json)
    ap(result)
    return nil if result.empty?
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

  def format_result(result)
    if result.is_a?(Druid::Query::Builder)
      start = Time.now.to_f
      puts _format_query_result($source.query(result.query), result.query)
      puts "Response Time: #{(Time.now.to_f - start).round(3)}s"
    else
      ap(result)
    end
  end
end

module Druid
  class Console

    extend Forwardable

    def initialize(uri, source, options)
      @uri, @source, @options = uri, source, options
      Ripl.start(binding: binding)
    end

    def client
      @client ||= Druid::Client.new(@uri, @options)
    end

    def source
      client.data_source(@source)
    end

    def dimensions
      source.dimensions.sort
    end

    def metrics
      source.metrics.sort
    end

    def query
      $source = source
      Query::Builder.new
    end

    def_delegators(*[
      :query,
      :group_by,
      :sum,
      :long_sum,
      :double_sum,
      :count,
      :postagg,
      :interval,
      :granularity,
      :filter,
      :time_series,
      :topn,
      :min,
      :max,
      :hyper_unique,
      :cardinality,
      :js_aggregation,
    ])

  end
end

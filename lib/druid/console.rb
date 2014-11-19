require 'active_support/time'
require 'ap'
require 'forwardable'
require 'irb'
require 'ripl'
require 'terminal-table'

require 'druid'

Ripl::Shell.class_eval do
  def format_query_result(result, query)

    if query.search?
      # the "magic" response row should be refactored ...
      group = {}
      result.each do |sub|
        sub.row.each do |entry|
          (group[entry["dimension"]] ||= []) << entry["value"]
        end
      end
      tt = Terminal::Table.new do
        group.each do |dimension, values|
          next if values.empty?
          add_row :separator
          add_row [{value:dimension, colspan:2}]
          add_row :separator
          values.each {|v| add_row ["", v] if !v.empty?}
        end
      end
      return tt
    end

    include_timestamp = query[:granularity] != 'all'
    keys = result.empty? ? [] : result.last.keys
    grouped_result = result.group_by(&:timestamp)
    Terminal::Table.new(:headings => keys) do
      grouped_result.each do |timestamp, rows|
        if include_timestamp
          add_row :separator unless timestamp == result.first.timestamp
          add_row [{ :value => timestamp, :colspan => keys.length }]
          add_row :separator
        end
        rows.each {|row| add_row keys.map {|key| row[key] } }
      end
    end
  end

  def format_result(result)
    if result.is_a?(Druid::Query)
      start = Time.now.to_f
      puts format_query_result(result.source.send(result), result)
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
      source.dimensions
    end

    def metrics
      source.metrics
    end

    def query
      source.query
    end

    def_delegators :query, :group_by, :sum, :long_sum, :double_sum, :count, :hyper_unique, :postagg, :interval, :granularity, :filter, :time_series, :topn
  end
end

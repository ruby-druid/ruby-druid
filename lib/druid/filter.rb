module Druid
  class Filter
    include ActiveModel::Model

    attr_accessor :type
    validates :type, inclusion: { in: %w(selector regex and or not javascript) }

    class DimensionValidator < ActiveModel::EachValidator
      TYPES = %w(selector regex javascript)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :dimension
    validates :dimension, dimension: true

    class ValueValidator < ActiveModel::EachValidator
      TYPES = %w(selector)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :value
    validates :value, value: true

    class PatternValidator < ActiveModel::EachValidator
      TYPES = %w(regex)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :pattern
    validates :pattern, pattern: true

    class FieldsValidator < ActiveModel::EachValidator
      TYPES = %w(and or)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          value.each(&:valid?) # trigger validation
          value.each do |fvalue|
            fvalue.errors.messages.each do |k, v|
              record.errors.add(attribute, { k => v })
            end
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") unless value.blank?
        end
      end
    end

    attr_accessor :fields
    validates :fields, fields: true

    def fields
      @fields ||= []
    end

    def fields=(value)
      if value.is_a?(Array)
        @fields = value.map do |x|
          Filter.new(x)
        end
      else
        @fields = [value]
      end
    end

    class FieldValidator < ActiveModel::EachValidator
      TYPES = %w(not)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          if value
            value.valid? # trigger validation
            value.errors.messages.each do |k, v|
              record.errors.add(attribute, { k => v })
            end
          else
            record.errors.add(attribute, "may not be blank")
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :field
    validates :field, field: true

    def field=(value)
      if value.is_a?(Hash)
        @field = Filter.new(value)
      else
        @field = value
      end
    end

    class FunctionValidator < ActiveModel::EachValidator
      TYPES = %w(javascript)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :function
    validates :function, function: true

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end

    def method_missing(name, *args)
      DimensionFilter.new(dimension: name)
    end
  end

  module BooleanOperators
    def &(other)
      BooleanFilter.new({
        type: 'and',
        fields: [self, other],
      })
    end

    def |(other)
      BooleanFilter.new({
        type: 'or',
        fields: [self, other],
      })
    end

    def !()
      BooleanFilter.new({
        type: 'not',
        field: self,
      })
    end
  end

  class DimensionFilter < Filter
    include BooleanOperators

    def in_rec(bounds)
      RecFilter.new(@dimension, bounds)
    end

    def in_circ(bounds)
      CircFilter.new(@dimension, bounds)
    end

    def eq(value)
      case value
      when ::Array
        self.in(value)
      when ::Regexp
        self.regexp(value)
      else
        @type = 'selector'
        @value = value
      end
      self
    end

    alias :'==' :eq

    def neq(value)
      return !self.eq(value)
    end

    alias :'!=' :neq

    def in(*args)
      filter_multiple(args.flatten, 'or', :eq)
    end

    def nin(*args)
      filter_multiple(args.flatten, 'and', :neq)
    end

    def filter_multiple(values, operator, method)
      ::Kernel.raise 'Values cannot be empty' if values.empty?
      return self.__send__(method, values[0]) if values.length == 1
      BooleanFilter.new({
        type: operator,
        fields: values.map do |value|
          DimensionFilter.new(dimension: @dimension).__send__(method, value)
        end
      })
    end

    alias_method :not_in, :nin

    def regexp(r)
      r = ::Regexp.new(r) unless r.is_a?(::Regexp)
      @pattern = r.inspect[1...-1] #to_s doesn't work
      @type = 'regex'
      self
    end

    def >(value)
      JavascriptFilter.new_comparison(@dimension, '>', value)
    end

    def <(value)
      JavascriptFilter.new_comparison(@dimension, '<', value)
    end

    def >=(value)
      JavascriptFilter.new_comparison(@dimension, '>=', value)
    end

    def <=(value)
      JavascriptFilter.new_comparison(@dimension, '<=', value)
    end

    def javascript(js)
      JavascriptFilter.new(@dimension, js)
    end
  end

  class BooleanFilter < Filter
    def &(other)
      if @type.to_s == 'and'
        self.fields << other
        self
      else
        BooleanFilter.new({
          type: 'and',
          fields: [self, other],
        })
      end
    end

    def |(other)
      if @type.to_s == 'or'
        self.fields << other
        self
      else
        BooleanFilter.new({
          type: 'or',
          fields: [self, other],
        })
      end
    end

    def !()
      if @type.to_s == 'not'
        self.field
        self
      else
        BooleanFilter.new({
          type: 'not',
          field: self,
        })
      end
    end
  end

  class RecFilter < Filter
    include BooleanOperators

    def initialize(dimension, bounds)
      super()
      @type = 'spatial'
      @dimension = dimension
      @bound = {
        type: 'rectangular',
        minCoords: bounds.first,
        maxCoords: bounds.last,
      }
    end
  end

  class CircFilter < Filter
    include BooleanOperators

    def initialize(dimension, bounds)
      super()
      @type = 'spatial'
      @dimension = dimension
      @bound = {
        type: 'radius',
        coords: bounds.first,
        radius: bounds.last,
      }
    end
  end

  class JavascriptFilter < Filter
    include BooleanOperators

    def initialize(dimension, function)
      super()
      @type = 'javascript'
      @dimension = dimension
      @function = function
    end

    def self.new_expression(dimension, expression)
      self.new(dimension, "function(#{dimension}) { return(#{expression}); }")
    end

    def self.new_comparison(dimension, operator, value)
      self.new_expression(dimension, "#{dimension} #{operator} #{value.to_json}")
    end
  end
end

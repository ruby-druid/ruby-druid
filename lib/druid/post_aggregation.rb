module Druid
  class PostAggregation
    include ActiveModel::Model

    attr_accessor :numBuckets

    attr_accessor :type
    validates :type, inclusion: { in: %w(arithmetic fieldAccess constant javascript hyperUniqueCardinality) }

    class NameValidator < ActiveModel::EachValidator
      TYPES = %w(arithmetic constant javascript)
      def validate_each(record, attribute, value)
        if !TYPES.include?(record.type)
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :name
    validates :name, name: true

    class FnValidator < ActiveModel::EachValidator
      TYPES = %w(arithmetic)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'must be a valid arithmetic operation') unless %w(+ - * /).include?(value)
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :fn
    validates :fn, fn: true

    class FieldsValidator < ActiveModel::EachValidator
      TYPES = %w(arithmetic)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          value.each(&:valid?) # trigger validation
          value.each do |fvalue|
            fvalue.errors.messages.each do |k, v|
              record.errors.add(attribute, { k => v })
            end
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :fields
    validates :fields, fields: true

    def fields=(value)
      @fields = [value].flatten.compact.map do |aggregation|
        PostAggregation.new(aggregation)
      end
    end

    class FieldnameValidator < ActiveModel::EachValidator
      TYPES = %w(fieldAccess hyperUniqueCardinality)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :fieldName
    validates :fieldName, fieldname: true

    class ValueValidator < ActiveModel::EachValidator
      TYPES = %w(constant)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'must be numeric') if !value.is_a?(Numeric)
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :value
    validates :value, value: true

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

    attr_reader :fieldNames

    def method_missing(name, *args)
      if args.empty?
        PostAggregationField.new(fieldName: name)
      end
    end

    def js(*args)
      if args.empty?
        PostAggregationField.new(fieldName: :js)
      else
        PostAggregationJavascript.new(args.first)
      end
    end
  end

  module PostAggregationOperators
    def +(value)
      PostAggregationOperation.new(self, :+, value)
    end

    def -(value)
      PostAggregationOperation.new(self, :-, value)
    end

    def *(value)
      PostAggregationOperation.new(self, :*, value)
    end

    def /(value)
      PostAggregationOperation.new(self, :/, value)
    end
  end

  class PostAggregationOperation < PostAggregation
    include PostAggregationOperators

    def initialize(left, operator, right)
      super()
      @type = 'arithmetic'
      @fn = operator
      @fields = [
        left.is_a?(Numeric) ? PostAggregationConstant.new(value: left) : left,
        right.is_a?(Numeric) ? PostAggregationConstant.new(value: right) : right,
      ]
    end

    def as(field)
      @name = field.fieldName.to_s
      self
    end

    def field_names
      fields.map(&:field_names).flatten.compact.uniq
    end
  end

  class PostAggregationField < PostAggregation
    include PostAggregationOperators

    def initialize(attributes = {})
      super
      @type ||= 'fieldAccess'
    end

    def field_names
      [@fieldName]
    end
  end

  class PostAggregationConstant < PostAggregation
    include PostAggregationOperators

    def initialize(attributes = {})
      super
      @type = 'constant'
    end

    def field_names
      []
    end
  end

  class PostAggregationJavascript < PostAggregation
    include PostAggregationOperators

    def initialize(function)
      super()
      @type = 'javascript'
      @fieldNames = extract_fields(function)
      @function = function
    end

    def field_names
      @fieldNames
    end

    def as(field)
      @name = field.fieldName.to_s
      self
    end

    private

    def extract_fields(function)
      match = function.match(/function\((.+)\)/)
      raise 'Invalid Javascript function' unless match && match.captures
      match.captures.first.split(',').map(&:strip)
    end
  end

  class PostAggregationHistogramEqualBuckets < PostAggregation
    attr_accessor :numBuckets
    def initialize(attributes = {})
      super(attributes)
      @type = "equalBuckets"
      @numBuckets ||= 10
    end
  end

  class PostAggregationHistogramBuckets < PostAggregation
    attr_accessor :bucketSize
    attr_accessor :offset
    def initialize(attributes = {})
      super
      @type = "buckets"
    end
  end

  class PostAggregationHistogramCustomBuckets < PostAggregation
    attr_accessor :breaks
    def initialize(attributes = {})
      super
      @type = "customBuckets"
    end
  end

  class PostAggregationHistogramMin < PostAggregation
    def initialize(attributes = {})
      super
      @type = "min"
    end
  end

  class PostAggregationHistogramMax < PostAggregation
    def initialize(attributes = {})
      super
      @type = "max"
    end
  end

  class PostAggregationHistogramQuantile < PostAggregation
    attr_accessor :probability
    def initialize(attributes = {})
      super
      @type = "quantile"
    end
  end

  class PostAggregationHistogramQuantiles < PostAggregation
    attr_accessor :probabilities
    def initialize(attributes = {})
      super
      @type = "quantiles"
    end
  end

end

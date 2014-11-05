module Druid
  class PostAggregation < BasicObject
    include Serializable

    def method_missing(name, *args)
      if args.empty?
        PostAggregationField.new(name)
      end
    end

    def js(*args)
      if args.empty?
        PostAggregationField.new(:js)
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

  class PostAggregationOperation
    include PostAggregationOperators
    include Serializable

    attr_reader :left, :operator, :right, :name

    def initialize(left, operator, right)
      @left = left.is_a?(Numeric) ? PostAggregationConstant.new(left) : left
      @operator = operator
      @right = right.is_a?(Numeric) ? PostAggregationConstant.new(right) : right
    end

    def as(field)
      @name = field.name.to_s
      self
    end

    def get_field_names
      field_names = left.get_field_names + right.get_field_names
      field_names.flatten.compact.uniq
    end

    def to_h
      hash = { type: "arithmetic", fn: @operator, fields: [@left.to_h, @right.to_h] }
      hash[:name] = @name if @name
      hash
    end
  end

  class PostAggregationField
    include PostAggregationOperators
    include Serializable

    attr_reader :name

    def initialize(name)
      @name = name
    end

    def get_field_names
      [@name]
    end

    def to_h
      { type: "fieldAccess", name: @name, fieldName: @name }
    end
  end

  class PostAggregationConstant
    include PostAggregationOperators
    include Serializable

    attr_reader :value

    def initialize(value)
      @value = value
    end

    def get_field_names
      []
    end

    def to_h
      { type: "constant", value: @value }
    end
  end

  class PostAggregationJavascript
    include PostAggregationOperators
    include Serializable

    def initialize(function)
      @field_names = extract_fields(function)
      @function = function
    end

    def get_field_names
      @field_names
    end

    def as(field)
      @name = field.name.to_s
      self
    end

    def to_h
      {
        type: "javascript",
        name: @name,
        fieldNames: @field_names,
        function: @function,
      }
    end

    private

    def extract_fields(function)
      match = function.match(/function\((.+)\)/)
      raise 'Invalid Javascript function' unless match && match.captures
      match.captures.first.split(',').map(&:strip)
    end
  end
end

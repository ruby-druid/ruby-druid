require 'druid/serializable'

module Druid
  class Having
    include Serializable

    def method_missing(name, *args)
      if args.empty?
        HavingClause.new(name)
      end
    end
  end

  class HavingFilter
    include Serializable

    def clause?
      is_a?(HavingClause)
    end

    def operator?
      is_a?(HavingOperator)
    end

    def chain(other)
      return unless other
      if self.operator? && self.and?
        having = self
      else
        having = HavingOperator.new('and')
        having.add(self)
      end
      having.add(other)
      having
    end
  end

  class HavingClause < HavingFilter
    include Serializable

    def initialize(metric)
      @metric = metric
    end

    def ==(value)
      @type = "equalTo"
      @value = value
      self
    end

    def <(value)
      @type = "lessThan"
      @value = value
      self
    end

    def >(value)
      @type = "greaterThan"
      @value = value
      self
    end

    def to_h
      {
        :type => @type,
        :aggregation => @metric,
        :value => @value
      }
    end
  end

  class HavingOperator < HavingFilter
    include Serializable

    def initialize(type)
      @type = type
      @elements = []
    end

    def and?
      @type == 'and'
    end

    def add(element)
      @elements << element
    end

    def to_h
      {
        :type => @type,
        :havingSpecs => @elements
      }
    end
  end
end

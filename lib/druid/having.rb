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
        having = HavingOperator.new('and', true)
        having.add(self)
      end
      having.add(other)
      having
    end
  end

  class HavingClause < HavingFilter
    include Serializable

    def self.from_h(h)
      self.for h[:metric], h[:operator], h[:value]
    end

    def self.for(metric, operator, value)
      h = new(metric)
      # TODO should check the operator
      h.__send__(operator, value)
      h
    end

    def initialize(metric)
      @metric = metric
    end

    def &(other)
      create_operator('and', other)
    end

    def |(other)
      create_operator('or', other)
    end

    def !
      create_operator('not')
    end

    def eq(value)
      set_clause('equalTo', value)
    end

    alias :'==' :eq

    def neq(value)
      !eq(value)
    end

    alias :'!=' :neq

    def <(value)
      set_clause('lessThan', value)
    end

    def >(value)
      set_clause('greaterThan', value)
    end

    def to_h
      {
        :type => @type,
        :aggregation => @metric,
        :value => @value
      }
    end

    private

    def create_operator(type, other = nil)
      operator = HavingOperator.new(type, !other.nil?)
      operator.add(self)
      operator.add(other) if other
      operator
    end

    def set_clause(type, value)
      @type = type
      @value = value
      self
    end
  end

  class HavingOperator < HavingFilter
    include Serializable

    def initialize(type, takes_many)
      @type = type
      @takes_many = takes_many
      @elements = []
    end

    def and?
      @type == 'and'
    end

    def add(element)
      @elements << element
    end

    def &(other)
      apply_operator('and', other)
    end

    def |(other)
      apply_operator('or', other)
    end

    def !
      if @type == 'not'
        @elements.first
      else
        operator = HavingOperator.new('not', false)
        operator.add(self)
        operator
      end
    end

    def to_h
      hash = {
        :type => @type,
      }

      if @takes_many
        hash[:havingSpecs] = @elements
      else
        hash[:havingSpec] = @elements.first
      end

      hash
    end

    private

    def apply_operator(type, other)
      if @type == type
        operator = self
      else
        operator = HavingOperator.new(type, true)
        operator.add(self)
      end
      operator.add(other)
      operator
    end
  end
end

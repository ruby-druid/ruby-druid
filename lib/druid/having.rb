module Druid
  class Having
    include ActiveModel::Model

    attr_accessor :type
    validates :type, inclusion: { in: %w(and or not greaterThan lessThan equalTo) }

    class HavingsValidator < ActiveModel::EachValidator
      TYPES = %w(and or)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.queryType)
          value.each(&:valid?) # trigger validation
          value.each do |avalue|
            avalue.errors.messages.each do |k, v|
              record.errors.add(attribute, { k => v })
            end
          end
        else
          record.errors.add(attribute, "is not supported by type=#{record.queryType}") if value
        end
      end
    end

    attr_accessor :havingSpecs
    validates :havingSpecs, havings: true

    def havingSpecs
      @havingSpecs ||= []
    end

    class HavingValidator < ActiveModel::EachValidator
      TYPES = %w(not)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :havingSpec
    validates :havingSpec, having: true

    class AggregationValidator < ActiveModel::EachValidator
      TYPES = %w(greaterThan lessThan equalTo)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :aggregation
    validates :aggregation, aggregation: true

    attr_accessor :value

    def method_missing(name, *args)
      if args.empty?
        HavingClause.new(aggregation: name)
      end
    end

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
        having = HavingOperator.new(type: 'and')
        having.havingSpecs << self
      end
      having.havingSpecs << other
      having
    end

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end
  end

  class HavingClause < Having
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

    private

    def create_operator(type, other = nil)
      operator = HavingOperator.new(type: type)
      if type.to_s == 'not'
        operator.havingSpec = self
      else
        operator.havingSpecs << self
        operator.havingSpecs << other if other
      end
      operator
    end

    def set_clause(type, value)
      @type = type
      @value = value
      self
    end
  end

  class HavingOperator < Having
    def and?
      @type == 'and'
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
        operator = HavingOperator.new(type: 'not')
        operator.havingSpec = self
        operator
      end
    end

    private

    def apply_operator(type, other)
      if @type == type
        operator = self
      else
        operator = HavingOperator.new(type: type)
        operator.havingSpecs << self
      end
      operator.havingSpecs << other
      operator
    end
  end
end

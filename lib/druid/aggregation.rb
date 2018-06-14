module Druid
  class Aggregation
    include ActiveModel::Model

    attr_accessor :type
    validates :type, inclusion: { in: %w[count longSum doubleSum min max
                                         javascript cardinality hyperUnique
                                         doubleFirst doubleLast longFirst
                                         longLast floatFirst floatLast] }

    attr_accessor :name
    validates :name, presence: true

    class FieldnameValidator < ActiveModel::EachValidator
      TYPES = %w[count longSum doubleSum min max hyperUnique doubleFirst
                 doubleLast longFirst longLast floatFirst floatLast].freeze
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

    class FieldnamesValidator < ActiveModel::EachValidator
      TYPES = %w(javascript cardinality)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'must be a list of field names') if !value.is_a?(Array) || value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :fieldNames
    validates :fieldNames, fieldnames: true

    class FnValidator < ActiveModel::EachValidator
      TYPES = %w(javascript)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :fnAggregate
    validates :fnAggregate, fn: true

    attr_accessor :fnCombine
    validates :fnCombine, fn: true

    attr_accessor :fnReset
    validates :fnReset, fn: true

    attr_accessor :byRow
    validates :byRow, allow_nil: true, inclusion: { in: [true, false] }

    class FilterValidator < ActiveModel::EachValidator
      TYPES = %w[filtered].freeze
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :filter
    validates :filter, filter: true

    class AggregatorValidator < ActiveModel::EachValidator
      TYPES = %w[filtered].freeze
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :aggregator
    validates :aggregator, aggregator: true

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end
  end
end

module Druid
  class Dimension
    include ActiveModel::Model

    attr_accessor :type
    validates :type, inclusion: { in: %w(default extraction) }

    attr_accessor :dimension
    validates :dimension, presence: true

    attr_accessor :outputName
    validates :outputName, presence: true

    class ExtractionFnValidator < ActiveModel::EachValidator
      TYPES = %w(extraction)
      def validate_each(record, attribute, value)
        if TYPES.include?(record.type)
          record.errors.add(attribute, 'may not be blank') if value.blank?
        else
          record.errors.add(attribute, "is not supported by type=#{record.type}") if value
        end
      end
    end

    attr_accessor :extractionFn
    validates :extractionFn, extraction_fn: true

    def initialize(params)
      if params.is_a?(Hash)
        super
      else
        super(type: 'default', dimension: params.to_s, outputName: params.to_s)
      end
    end

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end

    def self.lookup(dimension, namespace, retain: true, injective: false)
      new({
        type: 'extraction',
        dimension: dimension,
        outputName: dimension,
        extractionFn: {
          type: 'registeredLookup',
          lookup: namespace,
          retainMissingValue: retain,
          injective: injective,
        },
      })
    end
  end
end

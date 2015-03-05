module Druid
  class Granularity
    include ActiveModel::Model

    attr_accessor :type
    validates :type, inclusion: { in: %w(period) }

    class PeriodValidator < ActiveModel::EachValidator
      def validate_each(record, attribute, value)
        valid = ISO8601::Duration.new(value) rescue nil
        record.errors.add(attribute, 'must be a valid ISO 8601 period') unless valid
      end
    end

    attr_accessor :period
    validates :period, period: true

    attr_accessor :timeZone
    validates :timeZone, allow_nil: true, inclusion: {
      in: ActiveSupport::TimeZone.all.map { |tz| tz.tzinfo.name }
    }

    class OriginValidator < ActiveModel::EachValidator
      def validate_each(record, attribute, value)
        return unless value
        valid = ISO8601::DateTime.new(value) rescue nil
        record.errors.add(attribute, 'must be a valid ISO 8601 time') unless valid
      end
    end

    attr_accessor :origin
    validates :origin, origin: true

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end

  end
end

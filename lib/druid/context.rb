module Druid
  class Context
    include ActiveModel::Model

    attr_accessor :timeout
    validates :timeout, allow_nil: true, numericality: true

    attr_accessor :priority
    validates :priority, allow_nil: true, numericality: true

    attr_accessor :queryId

    attr_accessor :useCache
    validates :useCache, allow_nil: true, inclusion: { in: [true, false] }

    attr_accessor :populateCache
    validates :populateCache, allow_nil: true, inclusion: { in: [true, false] }

    attr_accessor :bySegment
    validates :bySegment, allow_nil: true, inclusion: { in: [true, false] }

    attr_accessor :finalize
    validates :finalize, allow_nil: true, inclusion: { in: [true, false] }

    attr_accessor :chunkPeriod

    def initialize(attributes = {})
      super
      @queryId ||= SecureRandom.uuid
    end

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end

  end
end

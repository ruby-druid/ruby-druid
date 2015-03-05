module Druid
  class Context
    include ActiveModel::Model

    attr_accessor :timeout
    validates :timeout, numericality: true

    attr_accessor :priority
    validates :priority, numericality: true

    attr_accessor :queryId

    attr_accessor :useCache
    validates :useCache, inclusion: { in: [true, false] }

    attr_accessor :populateCache
    validates :populateCache, inclusion: { in: [true, false] }

    attr_accessor :bySegment
    validates :bySegment, inclusion: { in: [true, false] }

    attr_accessor :finalize
    validates :finalize, inclusion: { in: [true, false] }

    def initialize(attributes = {})
      super
      @queryId = SecureRandom.uuid
    end

    def as_json(options = {})
      super(options.merge(except: %w(errors validation_context)))
    end

  end
end

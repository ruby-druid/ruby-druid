module Druid
  module Serializable
    def to_s
      to_h.to_s
    end

    def as_json(*a)
      to_h
    end

    def to_json(*a)
      MultiJson.dump(as_json)
    end
  end
end

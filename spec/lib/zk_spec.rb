module ZK
  def self.new(uri, opts = {})
    Mock.new uri, opts
  end

  class Mock
    include RSpec::Matchers

    def initialize(uri, opts)
      expect(uri).to eq('test-uri')
      expect(opts).to eq({ :chroot => :check })
      @registrations = {}
      @paths = {
        '/disco' => ['a', 'b'],
        '/disco/a' => ['b1', 'm1'],
        '/disco/b' => ['b2', 'm2']
      }
      @unregisters = []
    end

    def register(path, opts, &block)
      expect(opts).to eq({ :only => :child })
      expect(@registrations[path]).to eq(nil)

      zk = self
      block.define_singleton_method :unregister do
        zk.instance_variable_get('@unregisters').push path
        zk.instance_variable_get('@registrations').delete path
      end
      @registrations[path] = block
    end

    def unregistrations
      result = @unregisters
      @unregisters = []
      result
    end

    def children(path, opts)
      expect(@registrations[path]).to be_a(Proc)

      value = @paths[path]
      throw "no mock code for #{path}" unless value
      value
    end

    def get(path)
      case path
      when '/disco/a/b1'
        [{ :address => 'b1_address', :port => 80 }.to_json]
      when '/disco/a/m1'
        [{ :address => 'm1_address', :port => 81 }.to_json]
      when '/disco/b/b2'
        [{ :address => 'b2_address', :port => 90 }.to_json]
      when '/disco/b/m2'
        [{ :address => 'm2_address', :port => 85 }.to_json]
      when '/disco/a/b3'
        [{ :address => 'b3_address', :port => 83 }.to_json]
      else
        throw "no mock code for #{path}"
      end
    end

    def change(uri, values)
      @paths[uri] = values
      @registrations[uri].call if @registrations[uri]
    end

    def on_expired_session
    end
  end
end

class RestClientResponseMock
  def initialize(code, value)
    @code = code
    @value = value
  end

  def code
    @code
  end

  def to_str
    @value.to_str
  end
end

describe Druid::ZK do
  it 'reports services and data sources correctly' do
    calls = []
    expect(RestClient::Request).to receive(:execute).at_least(:once) do |opts|
      uri_match = opts[:url].match(/^http:\/\/(.+)_address:(.+)\/druid\/v2\/datasources\/$/)

      host = uri_match[1]
      port = uri_match[2].to_i

      calls.push([host, port])

      case host
      when 'b1'
        RestClientResponseMock.new(200, ['s1', 's2'].to_json)
      when 'b2'
        RestClientResponseMock.new(200, ['s3', 's4'].to_json)
      when 'b3'
        RestClientResponseMock.new(200, ['s5', 's6'].to_json)
      else
        RestClientResponseMock.new(404, nil)
      end
    end

    zk = Druid::ZK.new('test-uri', :discovery_path => '/disco')

    expect(calls).to eq([
      ['b1', 80],
      ['m1', 81],
      ['b2', 90],
      ['m2', 85]
    ])
    expect(zk.services).to eq(['a', 'b'])
    expect(zk.data_sources).to eq({
      'a/s1' => 'http://b1_address:80/druid/v2/',
      'a/s2' => 'http://b1_address:80/druid/v2/',
      'b/s3' => 'http://b2_address:90/druid/v2/',
      'b/s4' => 'http://b2_address:90/druid/v2/'
    })

    calls = []
    mock = zk.instance_variable_get('@zk')
    expect(mock.unregistrations).to eq([])

    # unregister a whole service
    mock.change '/disco', ['a']
    expect(calls).to eq([])
    expect(zk.services).to eq(['a'])
    expect(zk.data_sources).to eq({
      'a/s1' => 'http://b1_address:80/druid/v2/',
      'a/s2' => 'http://b1_address:80/druid/v2/'
    })
    expect(mock.unregistrations).to eq(['/disco/b'])
    # register it again
    mock.change '/disco', ['a', 'b']
    expect(calls).to eq([
      ['b2', 90],
      ['m2', 85]
    ])
    expect(zk.services).to eq(['a', 'b'])
    expect(zk.data_sources).to eq({
      'a/s1' => 'http://b1_address:80/druid/v2/',
      'a/s2' => 'http://b1_address:80/druid/v2/',
      'b/s3' => 'http://b2_address:90/druid/v2/',
      'b/s4' => 'http://b2_address:90/druid/v2/'
    })
    expect(mock.unregistrations).to eq([])

    #register a new broker
    calls = []
    mock.change '/disco/a', ['b1', 'b3']
    expect(calls).to eq([['b3', 83]])
    expect(zk.services).to eq(['a', 'b'])
    expect(zk.data_sources).to eq({
      'a/s1' => 'http://b1_address:80/druid/v2/',
      'a/s2' => 'http://b1_address:80/druid/v2/',
      'b/s3' => 'http://b2_address:90/druid/v2/',
      'b/s4' => 'http://b2_address:90/druid/v2/',
      'a/s5' => 'http://b3_address:83/druid/v2/',
      'a/s6' => 'http://b3_address:83/druid/v2/'
    })
    expect(mock.unregistrations).to eq(['/disco/a'])
    # unregister it
    calls = []
    mock.change '/disco/a', ['b1']
    expect(calls).to eq([])
    expect(zk.services).to eq(['a', 'b'])
    expect(zk.data_sources).to eq({
      'a/s1' => 'http://b1_address:80/druid/v2/',
      'a/s2' => 'http://b1_address:80/druid/v2/',
      'b/s3' => 'http://b2_address:90/druid/v2/',
      'b/s4' => 'http://b2_address:90/druid/v2/'
    })
    expect(mock.unregistrations).to eq(['/disco/a'])
  end
end

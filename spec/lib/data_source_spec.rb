describe Druid::DataSource do

  context '#post' do
    it 'parses response on 200' do
      # MRI
      stub_request(:post, "http://www.example.com/druid/v2").
        with(:body => "{\"context\":{\"queryId\":null},\"queryType\":\"timeseries\",\"intervals\":[\"2013-04-04T00:00:00+00:00/2013-04-04T00:00:00+00:00\"],\"granularity\":\"all\",\"dataSource\":\"test\"}",
          :headers => { 'Accept' => '*/*', 'Accept-Encoding' => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'Content-Type' => 'application/json', 'User-Agent' => 'Ruby' }).
        to_return(:status => 200, :body => '[]', :headers => {})
      # JRuby ... *sigh
      stub_request(:post, "http://www.example.com/druid/v2").
        with(:body => "{\"context\":{\"queryId\":null},\"granularity\":\"all\",\"intervals\":[\"2013-04-04T00:00:00+00:00/2013-04-04T00:00:00+00:00\"],\"queryType\":\"timeseries\",\"dataSource\":\"test\"}",
          :headers => { 'Accept' => '*/*', 'Accept-Encoding' => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'Content-Type' => 'application/json', 'User-Agent' => 'Ruby' }).
        to_return(:status => 200, :body => '[]', :headers => {})
      ds = Druid::DataSource.new('test/test', 'http://www.example.com/druid/v2')
      query = Druid::Query::Builder.new.interval('2013-04-04', '2013-04-04').granularity(:all).query
      query.context.queryId = nil
      expect(ds.post(query)).to be_empty
    end

    it 'raises on request failure' do
      # MRI
      stub_request(:post, 'http://www.example.com/druid/v2').
        with(:body => "{\"context\":{\"queryId\":null},\"queryType\":\"timeseries\",\"intervals\":[\"2013-04-04T00:00:00+00:00/2013-04-04T00:00:00+00:00\"],\"granularity\":\"all\",\"dataSource\":\"test\"}",
          :headers => { 'Accept' => '*/*', 'Accept-Encoding' => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'Content-Type' => 'application/json', 'User-Agent' => 'Ruby' }).
        to_return(:status => 666, :body => 'Strange server error', :headers => {})
      # JRuby ... *sigh
      stub_request(:post, 'http://www.example.com/druid/v2').
        with(:body => "{\"context\":{\"queryId\":null},\"granularity\":\"all\",\"intervals\":[\"2013-04-04T00:00:00+00:00/2013-04-04T00:00:00+00:00\"],\"queryType\":\"timeseries\",\"dataSource\":\"test\"}",
          :headers => { 'Accept' => '*/*', 'Accept-Encoding' => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'Content-Type' => 'application/json', 'User-Agent' => 'Ruby' }).
        to_return(:status => 666, :body => 'Strange server error', :headers => {})
      ds = Druid::DataSource.new('test/test', 'http://www.example.com/druid/v2')
      query = Druid::Query::Builder.new.interval('2013-04-04', '2013-04-04').granularity(:all).query
      query.context.queryId = nil
      expect { ds.post(query) }.to raise_error(Druid::DataSource::Error)
    end
  end

  context '#metadata' do
    it 'parses metrics on 200' do
      stub_request(:get, 'http://www.example.com/druid/v2/datasources/test').
        to_return(:status => 200, :body => '{}', :headers => {})
      ds = Druid::DataSource.new('test/test', 'http://www.example.com/druid/v2/')
      expect(ds.metrics).to be_nil
    end

    it 'raises on request failure' do
      stub_request(:get, 'http://www.example.com/druid/v2/datasources/test').
        to_return(:status => 666, :body => 'Strange server error', :headers => {})
      ds = Druid::DataSource.new('test/test', 'http://www.example.com/druid/v2/')
      expect { ds.metrics }.to raise_error(RuntimeError)
    end
  end

  context '#metadata!' do
    it 'includes interval in metadata request' do
      stub = stub_request(:get, 'http://www.example.com/druid/v2/datasources/test?interval=2015-04-10T00:00:00+00:00/2015-04-17T00:00:00+00:00').
        to_return(:status => 200, :body => '{}', :headers => {})
      ds = Druid::DataSource.new('test/test', 'http://www.example.com/druid/v2/')
      ds.metadata!(:interval => ['2015-04-10', '2015-04-17'])
      expect(stub).to have_been_requested
    end
  end

end

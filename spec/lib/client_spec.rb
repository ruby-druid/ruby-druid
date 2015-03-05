describe Druid::Client do

  it 'calls zookeeper on intialize' do
    expect(Druid::ZK).to receive(:new).with('test_uri', {})
    Druid::Client.new('test_uri')
  end

  it 'returns the correct data source' do
    stub_request(:get, "http://www.example.com/druid/v2/datasources/test").
      with(:headers => { 'Accept'=>'*/*', 'Accept-Encoding' => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent' => 'Ruby' }).
      to_return(:status => 200, :body => "{\"dimensions\":[\"d1\", \"d2\"], \"metrics\":[\"m1\", \"m2\"]}", :headers => {})
    expect(Druid::ZK).to receive(:new).and_return(double(Druid::ZK, :data_sources => { 'test/test' => 'http://www.example.com/druid/v2/' }, :close! => true))
    client = Druid::Client.new('test_uri')
    ds = client.data_source('test/test')
    expect(ds.name).to eq('test')
    expect(ds.metrics).to eq(['m1', 'm2'])
    expect(ds.dimensions).to eq(['d1', 'd2'])
  end

end

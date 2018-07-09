describe Druid::Query do

  before :each do
    @query = Druid::Query::Builder.new
  end

  it 'takes a query type' do
    @query.query_type('query_type')
    expect(JSON.parse(@query.query.to_json)['queryType']).to eq('query_type')
  end

  it 'sets query type by group_by' do
    @query.group_by
    expect(JSON.parse(@query.query.to_json)['queryType']).to eq('groupBy')
  end

  it 'sets query type to timeseries' do
    @query.timeseries
    expect(JSON.parse(@query.query.to_json)['queryType']).to eq('timeseries')
  end

  it 'takes dimensions from group_by method' do
    @query.group_by(:a, :b, :c)
    expect(JSON.parse(@query.query.to_json)['dimensions']).to eq([{"type"=>"default", "dimension"=>"a", "outputName"=>"a"},
                                                                  {"type"=>"default", "dimension"=>"b", "outputName"=>"b"},
                                                                  {"type"=>"default", "dimension"=>"c", "outputName"=>"c"}])
  end

  it 'takes dimension, metric and threshold from topn method' do
    @query.topn(:a, :b, 25)
    result = JSON.parse(@query.query.to_json)
    expect(result['dimension']).to eq('a')
    expect(result['metric']).to eq('b')
    expect(result['threshold']).to eq(25)
  end

  describe '#postagg' do
    it 'build a post aggregation with a constant right' do
      @query.postagg{(a + 1).as ctr }

      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"+",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"a"},
         {"type"=>"constant", "value"=>1}],
        "name"=>"ctr"}])
    end

    it 'build a + post aggregation' do
      @query.postagg{(a + b).as ctr }
      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"+",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"a"},
        {"type"=>"fieldAccess", "fieldName"=>"b"}],
        "name"=>"ctr"}])
    end

    it 'build a - post aggregation' do
      @query.postagg{(a - b).as ctr }
      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"-",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"a"},
        {"type"=>"fieldAccess", "fieldName"=>"b"}],
        "name"=>"ctr"}])
    end

    it 'build a * post aggregation' do
      @query.postagg{(a * b).as ctr }
      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"*",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"a"},
        {"type"=>"fieldAccess", "fieldName"=>"b"}],
        "name"=>"ctr"}])
    end

    it 'build a / post aggregation' do
      @query.postagg{(a / b).as ctr }
      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"/",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"a"},
        {"type"=>"fieldAccess", "fieldName"=>"b"}],
      "name"=>"ctr"}])
    end

    it 'build a complex post aggregation' do
      @query.postagg{((a / b) * 1000).as ctr }
      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"*",
        "fields"=>
        [{"type"=>"arithmetic", "fn"=>"/", "fields"=>
          [{"type"=>"fieldAccess", "fieldName"=>"a"},
           {"type"=>"fieldAccess", "fieldName"=>"b"}]},
        {"type"=>"constant", "value"=>1000}],
      "name"=>"ctr"}])
    end

    it 'adds fields required by the postagg operation to longsum' do
      @query.postagg{ (a/b).as c }
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq([
        {"type"=>"longSum", "name"=>"a", "fieldName"=>"a"},
        {"type"=>"longSum", "name"=>"b", "fieldName"=>"b"}
      ])
    end

    it 'chains aggregations' do
      @query.postagg{(a / b).as ctr }.postagg{(b / a).as rtc }

      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([{"type"=>"arithmetic",
        "fn"=>"/",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"a"},
        {"type"=>"fieldAccess", "fieldName"=>"b"}],
      "name"=>"ctr"},
      {"type"=>"arithmetic",
        "fn"=>"/",
        "fields"=>
        [{"type"=>"fieldAccess", "fieldName"=>"b"},
        {"type"=>"fieldAccess", "fieldName"=>"a"}],
      "name"=>"rtc"}
      ])
    end

    it 'builds a javascript post aggregation' do
      @query.postagg { js('function(agg1, agg2) { return agg1 + agg2; }').as result }
      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq([
        {
          'type' => 'javascript',
          'name' => 'result',
          'fieldNames' => ['agg1', 'agg2'],
          'function' => 'function(agg1, agg2) { return agg1 + agg2; }'
        }
      ])
    end

    it 'raises an error when an invalid javascript function is used' do
      expect {
        @query.postagg { js('{ return a_with_b - a; }').as b }
      }.to raise_error
    end

    it 'build a post aggregation with hyperUniqueCardinality' do
      post_agg = Druid::PostAggregationOperation.new(
        Druid::PostAggregationField.new(fieldName: 'a', type: 'hyperUniqueCardinality'),
        :/,
        2
      )
      post_agg.name = 'a_2'
      @query.query.postAggregations << post_agg

      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq(
        [{
          'type' => 'arithmetic',
          'fn' => '/',
          'fields' => [
            { 'type' => 'hyperUniqueCardinality', 'fieldName' => 'a' },
            { 'type' => 'constant', 'value' => 2 }
          ],
          'name' => 'a_2'
        }]
      )
    end
  end

  it 'builds aggregations on long_sum' do
    @query.long_sum(:a, :b, :c)
    expect(JSON.parse(@query.query.to_json)['aggregations']).to eq([
      { 'type' => 'longSum', 'name' => 'a', 'fieldName' => 'a'},
      { 'type' => 'longSum', 'name' => 'b', 'fieldName' => 'b'},
      { 'type' => 'longSum', 'name' => 'c', 'fieldName' => 'c'}
    ])
  end

  describe '#min' do
    it 'builds aggregations with "min" type' do
      @query.min(:a, :b)
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        { 'type' => 'min', 'name' => 'a', 'fieldName' => 'a'},
        { 'type' => 'min', 'name' => 'b', 'fieldName' => 'b'}
      ]
    end
  end

  describe '#max' do
    it 'builds aggregations with "max" type' do
      @query.max(:a, :b)
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        { 'type' => 'max', 'name' => 'a', 'fieldName' => 'a'},
        { 'type' => 'max', 'name' => 'b', 'fieldName' => 'b'}
      ]
    end
  end

  describe '#hyper_unique' do
    it 'builds aggregation with "hyperUnique"' do
      @query.hyper_unique(:a, :b)
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        { 'type' => 'hyperUnique', 'name' => 'a', 'fieldName' => 'a'},
        { 'type' => 'hyperUnique', 'name' => 'b', 'fieldName' => 'b'}
      ]
    end
  end

  describe '#theta_sketch' do
    it 'builds aggregation with "theta_sketch"' do
      @query.theta_sketch('user_id_sketch', 'B_unique_users')
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        { 'type' => 'thetaSketch', 'name' => 'B_unique_users', 'fieldName' => 'user_id_sketch' }
      ]
    end

    it 'build a thetaSketch post aggregation with filtered aggregations' do
      @query.filtered_aggregation(:user_id_sketch, :A_unique_users, :thetaSketch) do
        product.eq('A')
      end
      @query.filtered_aggregation(:user_id_sketch, :B_unique_users, :thetaSketch) do
        product.eq('B')
      end
      @query.theta_sketch_postagg(
        'final_unique_users',
        'INTERSECT',
        %w[A_unique_users B_unique_users]
      )

      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        {
          'type' => 'filtered',
          'filter' => {
            'type' => 'selector',
            'dimension' => 'product',
            'value' => 'A'
          },
          'aggregator' => {
            'type' => 'thetaSketch', 'name' => 'A_unique_users',
            'fieldName' => 'user_id_sketch'
          }
        },
        {
          'type' => 'filtered',
          'filter' => {
            'type' => 'selector',
            'dimension' => 'product',
            'value' => 'B'
          },
          'aggregator' => {
            'type' => 'thetaSketch', 'name' => 'B_unique_users',
            'fieldName' => 'user_id_sketch'
          }
        }
      ]

      expect(JSON.parse(@query.query.to_json)['postAggregations']).to eq [
        {
          'type' => 'thetaSketchEstimate',
          'name' => 'final_unique_users',
          'field' => {
            'type' => 'thetaSketchSetOp',
            'name' => 'final_unique_users_sketch',
            'func' => 'INTERSECT',
            'fields' => [
              {
                'type' => 'fieldAccess',
                'fieldName' => 'A_unique_users'
              },
              {
                'type' => 'fieldAccess',
                'fieldName' => 'B_unique_users'
              }
            ]
          }
        }
      ]
    end
  end

  describe '#cardinality' do
    it 'builds aggregation with "cardinality" type' do
      @query.cardinality(:a, [:dim1, :dim2], true)
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        { 'type' => 'cardinality', 'name' => 'a', 'fieldNames' => ['dim1', 'dim2'], 'byRow' => true }
      ]
    end
  end

  describe '#js_aggregation' do
    it 'builds aggregation with "javascript" type' do
      @query.js_aggregation(:aggregate, [:x, :y],
        aggregate: "function(current, a, b)      { return current + (Math.log(a) * b); }",
        combine:   "function(partialA, partialB) { return partialA + partialB; }",
        reset:     "function()                   { return 10; }"
      )
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [{
        'type' => 'javascript',
        'name' => 'aggregate',
        'fieldNames' => ['x', 'y'],
        'fnAggregate' => 'function(current, a, b)      { return current + (Math.log(a) * b); }',
        'fnCombine' =>   'function(partialA, partialB) { return partialA + partialB; }',
        'fnReset' =>     'function()                   { return 10; }'
      }]
    end
  end

  describe '#filtered_aggregation' do
    it 'builds filtered aggregations' do
      @query.filtered_aggregation(:a, :a_filtered, :longSum) do
        b.eq(2) & c.neq(3)
      end
      expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
        {
          'type' => 'filtered',
          'filter' => {
            'type' => 'and',
            'fields' => [
              { 'dimension' => 'b', 'type' => 'selector', 'value' => 2 },
              {
                'type' => 'not',
                'field' => {
                  'dimension' => 'c', 'type' => 'selector', 'value' => 3
                }
              }
            ]
          },
          'aggregator' => { 'type' => 'longSum', 'name' => 'a_filtered', 'fieldName' => 'a' }
        }
      ]
    end
  end

  describe '#first_last_aggregators' do
    %w[doubleFirst doubleLast longFirst longLast floatFirst floatLast].each do |type|
      it "builds aggregations with '#{type}' type" do
        @query.send(type.underscore, :a, :b)
        expect(JSON.parse(@query.query.to_json)['aggregations']).to eq [
          { 'type' => type, 'name' => 'a', 'fieldName' => 'a'},
          { 'type' => type, 'name' => 'b', 'fieldName' => 'b'}
        ]
      end
    end
  end

  it 'appends long_sum properties from aggregations on calling long_sum again' do
    @query.long_sum(:a, :b, :c)
    @query.double_sum(:x,:y)
    @query.long_sum(:d, :e, :f)
    expect(JSON.parse(@query.query.to_json)['aggregations'].sort{|x,y| x['name'] <=> y['name']}).to eq([
      { 'type' => 'longSum', 'name' => 'a', 'fieldName' => 'a'},
      { 'type' => 'longSum', 'name' => 'b', 'fieldName' => 'b'},
      { 'type' => 'longSum', 'name' => 'c', 'fieldName' => 'c'},
      { 'type' => 'longSum', 'name' => 'd', 'fieldName' => 'd'},
      { 'type' => 'longSum', 'name' => 'e', 'fieldName' => 'e'},
      { 'type' => 'longSum', 'name' => 'f', 'fieldName' => 'f'},
      { 'type' => 'doubleSum', 'name' => 'x', 'fieldName' => 'x'},
      { 'type' => 'doubleSum', 'name' => 'y', 'fieldName' => 'y'}
    ])
  end

  it 'removes duplicate aggregation fields' do
    @query.long_sum(:a, :b)
    @query.long_sum(:b)

    expect(JSON.parse(@query.query.to_json)['aggregations']).to eq([
      { 'type' => 'longSum', 'name' => 'a', 'fieldName' => 'a'},
      { 'type' => 'longSum', 'name' => 'b', 'fieldName' => 'b'},
    ])
  end

  it 'must be chainable' do
    q = [Druid::Query::Builder.new]
    q.push q[-1].query_type('a')
    q.push q[-1].data_source('b')
    q.push q[-1].group_by('c')
    q.push q[-1].long_sum('d')
    q.push q[-1].double_sum('e')
    q.push q[-1].filter{a.eq 1}
    q.push q[-1].interval("2013-01-26T00", "2020-01-26T00:15")
    q.push q[-1].granularity(:day)

    q.each do |instance|
      expect(instance).to eq(q[0])
    end
  end

  it 'parses intervals from strings' do
    @query.interval('2013-01-26T00', '2020-01-26T00:15')
    expect(JSON.parse(@query.query.to_json)['intervals']).to eq(['2013-01-26T00:00:00+00:00/2020-01-26T00:15:00+00:00'])
  end

  it 'takes multiple intervals' do
    @query.intervals([['2013-01-26T00', '2020-01-26T00:15'],['2013-04-23T00', '2013-04-23T15:00']])
    expect(JSON.parse(@query.query.to_json)['intervals']).to eq(["2013-01-26T00:00:00+00:00/2020-01-26T00:15:00+00:00", "2013-04-23T00:00:00+00:00/2013-04-23T15:00:00+00:00"])
  end

  it 'accepts Time objects for intervals' do
    @query.interval(a = Time.now, b = Time.now + 1)
    expect(JSON.parse(@query.query.to_json)['intervals']).to eq(["#{a.iso8601}/#{b.iso8601}"])
  end

  it 'takes a granularity from string' do
    @query.granularity('all')
    expect(JSON.parse(@query.query.to_json)['granularity']).to eq('all')
  end

  it 'should take a period' do
    @query.granularity("P1D", 'Europe/Berlin')
    expect(@query.query.as_json['granularity']).to eq({
      'type' => "period",
      'period' => "P1D",
      'timeZone' => "Europe/Berlin"
    })
  end

  describe '#filter' do
    it 'creates a in_circ filter' do
      @query.filter{a.in_circ [[52.0,13.0], 10.0]}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({
      "type" => "spatial",
      "dimension" => "a",
      "bound" => {
          "type" => "radius",
          "coords" => [52.0, 13.0],
          "radius" =>  10.0
        }
      })
    end

    it 'creates a in_rec filter' do
      @query.filter{a.in_rec [[10.0, 20.0], [30.0, 40.0]] }
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({
      "type" => "spatial",
      "dimension" => "a",
      "bound" => {
          "type" => "rectangular",
          "minCoords" => [10.0, 20.0],
          "maxCoords" => [30.0, 40.0]
        }
      })
    end

    it 'creates an equals filter' do
      @query.filter{a.eq 1}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({"type"=>"selector", "dimension"=>"a", "value"=>1})
    end

    it 'creates an equals filter with ==' do
      @query.filter{a == 1}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({"type"=>"selector", "dimension"=>"a", "value"=>1})
    end

    it 'creates a not filter' do
      @query.filter{!a.eq 1}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"field" =>
        {"type"=>"selector", "dimension"=>"a", "value"=>1},
      "type" => "not"})
    end

    it 'creates a not filter with neq' do
      @query.filter{a.neq 1}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"field" =>
        {"type"=>"selector", "dimension"=>"a", "value"=>1},
      "type" => "not"})
    end

    it 'creates a not filter with !=' do
      @query.filter{a != 1}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"field" =>
        {"type"=>"selector", "dimension"=>"a", "value"=>1},
      "type" => "not"})
    end

    it 'creates an and filter' do
      @query.filter{a.neq(1) & b.eq(2) & c.eq('foo')}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"fields" => [
        {"type"=>"not", "field"=>{"type"=>"selector", "dimension"=>"a", "value"=>1}},
        {"type"=>"selector", "dimension"=>"b", "value"=>2},
        {"type"=>"selector", "dimension"=>"c", "value"=>"foo"}
      ],
      "type" => "and"})
    end

    it 'creates an or filter' do
      @query.filter{a.neq(1) | b.eq(2) | c.eq('foo')}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"fields" => [
        {"type"=>"not", "field"=> {"type"=>"selector", "dimension"=>"a", "value"=>1}},
        {"type"=>"selector", "dimension"=>"b", "value"=>2},
        {"type"=>"selector", "dimension"=>"c", "value"=>"foo"}
      ],
      "type" => "or"})
    end

    it 'chains filters' do
      @query.filter{a.eq(1)}.filter{b.eq(2)}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"fields" => [
        {"type"=>"selector", "dimension"=>"a", "value"=>1},
        {"type"=>"selector", "dimension"=>"b", "value"=>2}
      ],
      "type" => "and"})
    end

    it 'creates filter from hash' do
      @query.filter a:1, b:2
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"fields" => [
        {"type"=>"selector", "dimension"=>"a", "value"=>1},
        {"type"=>"selector", "dimension"=>"b", "value"=>2}
      ],
      "type" => "and"})
    end

    context 'when type argument is :nin' do
      it 'creates nin filter from hash' do
        @query.filter({ a: 1, b: 2 }, :nin)
        expect(JSON.parse(@query.query.to_json)['filter']).to eq({'fields' => [
          {'type' => 'not', 'field' => { 'dimension' => 'a', 'type' => 'selector', 'value' => 1} },
          {'type' => 'not', 'field' => { 'dimension' => 'b', 'type' => 'selector', 'value' => 2} }
        ],
        'type' => 'and'})
      end
    end

    it 'creates an in statement with or filter' do
      @query.filter{a.in [1,2,3]}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"fields" => [
        {"type"=>"selector", "dimension"=>"a", "value"=>1},
        {"type"=>"selector", "dimension"=>"a", "value"=>2},
        {"type"=>"selector", "dimension"=>"a", "value"=>3}
      ],
      "type" => "or"})
    end

    it 'creates a nin statement with and filter' do
      @query.filter{a.nin [1,2,3]}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq( {"fields" => [
        {"field"=>{"type"=>"selector", "dimension"=>"a", "value"=>1},"type" => "not"},
        {"field"=>{"type"=>"selector", "dimension"=>"a", "value"=>2},"type" => "not"},
        {"field"=>{"type"=>"selector", "dimension"=>"a", "value"=>3},"type" => "not"}
      ],
      "type" => "and"})
    end

    it 'creates a javascript with > filter' do
      @query.filter{a > 100}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({
        "type" => "javascript",
        "dimension" => "a",
        "function" => "function(a) { return(a > 100); }"
      })
    end

    it 'creates a mixed javascript filter' do
      @query.filter{(a >= 128) & (a != 256)}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({"fields" => [
        {"type" => "javascript", "dimension" => "a", "function" => "function(a) { return(a >= 128); }"},
        {"field" => {"type" => "selector", "dimension" => "a", "value" => 256}, "type" => "not"}
      ],
      "type" => "and"})
    end

    it 'creates a complex javascript filter' do
      @query.filter{(a >= 4) & (a <= '128')}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({"fields" => [
        {"type" => "javascript", "dimension" => "a", "function" => "function(a) { return(a >= 4); }"},
        {"type" => "javascript", "dimension" => "a", "function" => "function(a) { return(a <= \"128\"); }"}
      ],
      "type" => "and"})
    end

    it 'creates a custom javascript filter' do
      @query.filter{a.javascript("function(a) { return true; }")}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({
        "type" => "javascript",
        "dimension" => "a",
        "function" => "function(a) { return true; }"
      })
    end

    it 'can chain two in statements' do
      @query.filter{a.in([1,2,3]) & b.in([1,2,3])}
      expect(JSON.parse(@query.query.to_json)['filter']).to eq({"type"=>"and", "fields"=>[
        {"type"=>"or", "fields"=>[
          {"type"=>"selector", "dimension"=>"a", "value"=>1},
          {"type"=>"selector", "dimension"=>"a", "value"=>2},
          {"type"=>"selector", "dimension"=>"a", "value"=>3}
        ]},
        {"type"=>"or", "fields"=>[
          {"type"=>"selector", "dimension"=>"b", "value"=>1},
          {"type"=>"selector", "dimension"=>"b", "value"=>2},
          {"type"=>"selector", "dimension"=>"b", "value"=>3}
        ]}
      ]})
    end
  end

  describe '#having' do
    subject(:having) { JSON.parse(@query.to_json)['having'] }

    it 'creates an equalTo clause using ==' do
      @query.having { a == 100 }
      expect(@query.query.as_json['having']).to eq({ 'type' => 'equalTo', 'aggregation' => 'a', 'value' => 100 })
    end

    it 'creates a not equalTo clause using !=' do
      @query.having { a != 100 }
      expect(@query.query.as_json['having']).to eq({
        'type' => 'not',
        'havingSpec' => { 'type' => 'equalTo', 'aggregation' => 'a', 'value' => 100 },
      })
    end

    it 'creates a greaterThan clause using >' do
      @query.having { a > 100 }
      expect(@query.query.as_json['having']).to eq({ 'type' => 'greaterThan', 'aggregation' => 'a', 'value' => 100 })
    end

    it 'creates a lessThan clause using <' do
      @query.having { a < 100 }
      expect(@query.query.as_json['having']).to eq({ 'type' => 'lessThan', 'aggregation' => 'a', 'value' => 100 })
    end

    it 'creates an add clause using &' do
      @query.having { (a > 100) & (b > 200) }
      expect(@query.query.as_json['having']).to eq({
        'type' => 'and',
        'havingSpecs' => [
          { 'type' => 'greaterThan', 'aggregation' => 'a', 'value' => 100 },
          { 'type' => 'greaterThan', 'aggregation' => 'b', 'value' => 200 },
        ]
      })
    end

    it 'creates an or clause using |' do
      @query.having { (a > 100) | (b > 200) }
      expect(@query.query.as_json['having']).to eq({
        'type' => 'or',
        'havingSpecs' => [
          { 'type' => 'greaterThan', 'aggregation' => 'a', 'value' => 100 },
          { 'type' => 'greaterThan', 'aggregation' => 'b', 'value' => 200 },
        ]
      })
    end

    it 'creates a not clause using !' do
      @query.having { !((a == 100) & (b == 200)) }
      expect(@query.query.as_json['having']).to eq({
        'type' => 'not',
        'havingSpec' => {
          'type' => 'and',
          'havingSpecs' => [
            { 'type' => 'equalTo', 'aggregation' => 'a', 'value' => 100 },
            { 'type' => 'equalTo', 'aggregation' => 'b', 'value' => 200 },
          ]
        }
      })
    end

    it 'combines successive calls with and operator' do
      @query.having { a > 100 }.having { b > 200 }.having { c > 300 }
      expect(@query.query.as_json['having']).to eq({
        'type' => 'and',
        'havingSpecs' => [
          { 'type' => 'greaterThan', 'aggregation' => 'a', 'value' => 100 },
          { 'type' => 'greaterThan', 'aggregation' => 'b', 'value' => 200 },
          { 'type' => 'greaterThan', 'aggregation' => 'c', 'value' => 300 },
        ]
      })
    end
  end

  it 'should query regexp using .regexp(string)' do
    expect(JSON.parse(@query.filter{a.regexp('[1-9].*')}.query.to_json)['filter']).to eq({
      "dimension"=>"a",
      "type"=>"regex",
      "pattern"=>"[1-9].*"
    })
  end

  it 'should query regexp using .eq(regexp)' do
    expect(JSON.parse(@query.filter{a.in(/abc.*/)}.query.to_json)['filter']).to eq({
      "dimension"=>"a",
      "type"=>"regex",
      "pattern"=>"abc.*"
    })
  end

  it 'should query regexp using .in([regexp])' do
    expect(JSON.parse(@query.filter{ a.in(['b', /[a-z].*/, 'c']) }.query.to_json)['filter']).to eq({
      "type"=>"or",
      "fields"=>[
        {"dimension"=>"a", "type"=>"selector", "value"=>"b"},
        {"dimension"=>"a", "type"=>"regex", "pattern"=>"[a-z].*"},
        {"dimension"=>"a", "type"=>"selector", "value"=>"c"}
      ]
    })
  end

  it 'takes type, limit and columns from limit method' do
    @query.limit(10, :a => 'ASCENDING', :b => 'DESCENDING')
    result = JSON.parse(@query.query.to_json)
    expect(result['limitSpec']).to eq({
      'type' => 'default',
      'limit' => 10,
      'columns' => [
        { 'dimension' => 'a', 'direction' => 'ASCENDING'},
        { 'dimension' => 'b', 'direction' => 'DESCENDING'}
      ]
    })
  end
end

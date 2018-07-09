# ruby-druid

A Ruby client for [Druid](http://druid.io). Includes a [Squeel](https://github.com/ernie/squeel)-like query DSL and generates a JSON query that can be sent to Druid directly.

[![Gem Version](https://badge.fury.io/rb/ruby-druid.png)](http://badge.fury.io/rb/ruby-druid)
[![Build Status](https://travis-ci.org/ruby-druid/ruby-druid.png)](https://travis-ci.org/ruby-druid/ruby-druid)
[![Code Climate](https://codeclimate.com/github/ruby-druid/ruby-druid.png)](https://codeclimate.com/github/ruby-druid/ruby-druid)
[![Dependency Status](https://gemnasium.com/ruby-druid/ruby-druid.png)](https://gemnasium.com/ruby-druid/ruby-druid)

## Installation

Add this line to your application's Gemfile:

```
gem 'ruby-druid'
```

And then execute:

```
bundle
```

Or install it yourself as:

```
gem install ruby-druid
```

## Usage

A query can be constructed and sent like so:

```ruby
data_source = Druid::Client.new('zk1:2181,zk2:2181/druid').data_source('service/source')
query = Druid::Query::Builder.new.long_sum(:aggregate1).last(1.day).granularity(:all)
result = data_source.post(query)
```

The `post` method on the `DataSource` returns the parsed response from the Druid server as an array.

If you don't want to use ZooKeeper for broker discovery, you can explicitly construct a `DataSource`:

```ruby
data_source = Druid::DataSource.new('service/source', 'http://localhost:8080/druid/v2')
```

### GroupBy

A [GroupByQuery](http://druid.io/docs/latest/querying/groupbyquery.html) sets the
dimensions to group the data.

`queryType` is set automatically to `groupBy`.

```ruby
Druid::Query::Builder.new.group_by([:dimension1, :dimension2])
```

### TimeSeries

A [TimeSeriesQuery](http://druid.io/docs/latest/querying/timeseriesquery.html) returns an array of JSON objects where each object represents a value asked for by the timeseries query.

```ruby
Druid::Query::Builder.new.time_series([:aggregate1, :aggregate2])
```

### Aggregations

#### longSum, doubleSum, count, min, max, hyperUnique

```ruby
Druid::Query::Builder.new.long_sum([:aggregate1, :aggregate2])
```

In the same way could be used the following methods for [aggregations](http://druid.io/docs/latest/querying/aggregations.html) adding: `double_sum, count, min, max, hyper_unique`

#### cardinality

```ruby
Druid::Query::Builder.new.cardinality(:aggregate, [:dimension1, dimension2], <by_row: true | false>)
```

#### javascript

For example calculation for `sum(log(x)/y) + 10`:

```ruby
Druid::Query::Builder.new.js_aggregation(:aggregate, [:x, :y],
  aggregate: "function(current, a, b)      { return current + (Math.log(a) * b); }",
  combine:   "function(partialA, partialB) { return partialA + partialB; }",
  reset:     "function()                   { return 10; }"
)
```

#### filtered aggregation

A filtered aggregator wraps any given aggregator, but only aggregates the values for which the given dimension filter matches.

```ruby
Druid::Query::Builder.new.filtered_aggregation(:aggregate1, :aggregate_1_name, :longSum) do
  dimension1.neq 1 & dimension2.neq 2
end
```

### Post Aggregations

A simple syntax for post aggregations with +,-,/,* can be used like:

```ruby
query = Druid::Query::Builder.new.long_sum([:aggregate1, :aggregate2])
query.postagg { (aggregate2 + aggregate2).as output_field_name }
```

Required fields for the postaggregation are fetched automatically by the library.

Javascript post aggregations are also supported:

```ruby
query.postagg { js('function(aggregate1, aggregate2) { return aggregate1 + aggregate2; }').as result }
```

### thetaSketch

A theta sketch object can be thought of as a Set data structure.

```ruby
query.theta_sketch('user_id_sketch', 'B_unique_users')
```

DataSketches aggregators are useful combined with filtered aggregations.

```ruby
query.filtered_aggregation(:user_id_sketch, :A_unique_users, :thetaSketch) do
  product.eq('A')
end

query.filtered_aggregation(:user_id_sketch, :B_unique_users, :thetaSketch) do
  product.eq('B')
end
```

And then used by a post aggregations to calculate `INTERSECTION` or `UNION`.

```ruby
query.theta_sketch_postagg(
  'final_unique_users',
  'INTERSECT',
  %w[A_unique_users B_unique_users]
)
```

### Query Interval

The interval for the query takes a string with date and time or objects that provide an `iso8601` method.

```ruby
query = Druid::Query::Builder.new.long_sum(:aggregate1)
query.interval("2013-01-01T00", Time.now)
```

### Result Granularity

The granularity can be `:all`, `:none`, `:minute`, `:fifteen_minute`, `:thirthy_minute`, `:hour` or `:day`.

It can also be a period granularity as described in the [Druid documentation](http://druid.io/docs/latest/querying/granularities.html).

The period `'day'` or `:day` will be interpreted as `'P1D'`.

If a period granularity is specifed, the (optional) second parameter is a time zone. It defaults to the machines local time zone. i.e.

```ruby
query = Druid::Query::Builder.new.long_sum(:aggregate1)
query.granularity(:day)
```

is (on my box) the same as

```ruby
query = Druid::Query::Builder.new.long_sum(:aggregate1)
query.granularity('P1D', 'Europe/Berlin')
```

### Having filters

```ruby
# equality
Druid::Query::Builder.new.having { metric == 10 }
```

```ruby
# inequality
Druid::Query::Builder.new.having { metric != 10 }
```

```ruby
# greater, less
Druid::Query::Builder.new.having { metric > 10 }
Druid::Query::Builder.new.having { metric < 10 }
```

#### Compound having filters

Having filters can be combined with boolean logic.

```ruby
# and
Druid::Query::Builder.new.having { (metric != 1) & (metric2 != 2) }
```

```ruby
# or
Druid::Query::Builder.new.having { (metric == 1) | (metric2 == 2) }
```

```ruby
# not
Druid::Query::Builder.new.having{ !metric.eq(1) }
```

### Filters

Filters are set by the `filter` method. It takes a block or a hash as parameter.

Filters can be chained `filter{...}.filter{...}`

#### Base Filters

```ruby
# equality
Druid::Query::Builder.new.filter{dimension.eq 1}
Druid::Query::Builder.new.filter{dimension == 1}
```

```ruby
# inequality
Druid::Query::Builder.new.filter{dimension.neq 1}
Druid::Query::Builder.new.filter{dimension != 1}
```

```ruby
# greater, less
Druid::Query::Builder.new.filter{dimension > 1}
Druid::Query::Builder.new.filter{dimension >= 1}
Druid::Query::Builder.new.filter{dimension < 1}
Druid::Query::Builder.new.filter{dimension <= 1}
```

```ruby
# JavaScript
Druid::Query::Builder.new.filter{a.javascript('dimension >= 1 && dimension < 5')}
```

#### Compound Filters

Filters can be combined with boolean logic.

```ruby
# and
Druid::Query::Builder.new.filter{dimension.neq 1 & dimension2.neq 2}
```

```ruby
# or
Druid::Query::Builder.new.filter{dimension.neq 1 | dimension2.neq 2}
```

```ruby
# not
Druid::Query::Builder.new.filter{!dimension.eq(1)}
```

#### Inclusion Filter

This filter creates a set of equals filters in an or filter.

```ruby
Druid::Query::Builder.new.filter{dimension.in(1,2,3)}
```
#### Geographic filter

These filters have to be combined with time_series and do only work when coordinates is a spatial dimension [GeographicQueries](http://druid.io/docs/latest/development/geo.html)

```ruby
Druid::Query::Builder.new.time_series().long_sum([:aggregate1]).filter{coordinates.in_rec [[50.0,13.0],[54.0,15.0]]}
```

```ruby
Druid::Query::Builder.new.time_series().long_sum([:aggregate1]).filter{coordinates.in_circ [[53.0,13.0], 5.0]}
```

#### Exclusion Filter

This filter creates a set of not-equals fitlers in an and filter.

```ruby
Druid::Query::Builder.new.filter{dimension.nin(1,2,3)}
```

#### Hash syntax

Sometimes it can be useful to use a hash syntax for filtering for example if you already get them from a list or parameter hash.

```ruby
Druid::Query::Builder.new.filter{dimension => 1, dimension1 =>2, dimension2 => 3}
# which is equivalent to
Druid::Query::Builder.new.filter{dimension.eq(1) & dimension1.eq(2) & dimension2.eq(3)}
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

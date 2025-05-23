# PartitionCache

## Overview
PartitionCache - A caching middleware that allows caching of partition-identifiers in heavily partitioned datasets.
This middleware enables executing known SQL queries and storing sets of partitions with valid results.
For subsequent queries, these entries can be used and combined to reduce the search space.

It is a Python library that enables query rewriting of SQL statements. 
It can be used via CLI or as a Python library.
Currently, PostgreSQL can be used as an underlying database for CLI operations that require access to the underlying dataset, while various choices are available as a cache backend.

To efficiently utilize this approach, the working environment needs to fulfill the following properties:
- Complex SQL queries which need more than a few milliseconds to compute
- Readonly dataset (Appendable datasets are WIP)
- (Analytical) Searches across separate partitions (e.g., cities, spatial areas, days for time series, or sets of connected networks of property graphs)
    - This will work only partially with aggregations across partitions
- To enable positive effects on new queries, different subqueries need to be conjunctively (AND) connected

## How does it work?

Reduction of the search space:

- Incoming queries get recomposed in various variants and are executed in parallel, retrieving sets of all partitions in which at least one match was found
- All recomposed queries get stored together with their set of partition identifiers
- Frequent subqueries can also be stored with their partition identifiers
- New queries get checked against the cache. If the query or a part of the query is present in the cache, the search space can be restricted to the stored partition identifiers

###  Example

"For all cities in the world, find where a public park larger than 1000 m² lies within 50m of a street named 'main street'."

For this type of search, we can separate the search space for each city in our dataset. This would not help much on a normal search, but with PartitionCache, we can store the cities for which this search returned at least one result.

In case we have a later search that uses the same query, for example, "For all cities in Europe, find where a public park larger than 1000 m² lies within 50m of a street named 'main street'," we do not need to recompute this task for all cities. We can take the list of cities from the earlier search and only check for the additional constraints ("in Europe"). As the cities are provided in SQL, it does not restrict the database optimizer.

Further, as we observed the condition "a public park larger than 1000 m²," we created a set of cities where the park exists and also sped up following queries that asked for other properties. 
For example, "all schools in Europe which are within 1km of a public park larger than 1000 m²" - in this case, we can intersect the list of cities in Europe with the list of cities with a park and need to look for schools only in these cities.

## Install

```
pip install git+https://github.com/MPoppinga/PartitionCache@main
```

##  Usage

Notes:
At the current state, the library only supports a specific subset of SQL syntax which is available for PostgreSQL.
For example, CTEs are not supported, and JOINs and subqueries are only partially supported. We aim to increase the robustness and flexibility of our approach in future releases.

An example workload is available at [https://github.com/MPoppinga/ComplexMine](https://github.com/MPoppinga/ComplexMine).

### CLI Usage

#### Cache population
Usage: pcache-observer 

pcache-observer is a process that monitors the queue for new cache requests, allowing asynchronous population of the cache:

```
pcache-observer \
  --db-backend=postgresql \
  --db-name=mydb \
  --cache-backend=redis \
  --partition-key="partition_key" \ 
  --long-running-query-timeout=300
```

Usage: pcache-add 

pcache-add allows adding individual queries to the cache directly or sending them to the queue:

```
pcache-add \
  --query="SELECT * FROM table WHERE partition_key = 'value'" \
  --partition-key="partition_key" \
  --db-name=mydb \
  --cache-backend=redis
```

```
pcache-add \
  --query="SELECT * FROM table WHERE partition_key = 'value'" \
  --queue \
  --partition-key="partition_key" \
  --env-file=.env
```

#### Cache Usage

Usage: pcache-get

pcache-get returns the list of all partition keys based on the provided query, restricting the search space to as few partition_keys as possible:

```
pcache-get \
  --query="SELECT * FROM table" \
  --cache-backend=rocksdb
```

#### Cache Management

pcache-manage allows copying, deleting, and archiving entries from the individual cache backends that have been set up.

Usage: pcache-manage 

```
pcache-manage \
  --export \
  --cache=redis \ 
  --env-file=.env \
  --file=redis_export.pkl
```

```
pcache-manage \
  --delete \
  --cache=redis \ 
  --env-file=.env 
```

```
pcache-manage \
  --copy \
  --from=rocksdb_bit \ 
  --to=postgresql_array \ 
  --env-file=.env 
```

### Direct Usage
If using a Python application, it's possible to directly call the relevant functions.

## License

PartitionCache is licensed under the GNU Lesser General Public License v3.0.

See [COPYING](COPYING) and [COPYING.LESSER](COPYING.LESSER) for more details.

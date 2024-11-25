# PartitionCache

## Overview
PartitionCache - A caching middleware, allowing to cache partition-identifiers in heavily partitioned datasets.
This middleware allows executing known SQL queries and storing a set of partitions with valid results.
For following queries, these entries can be used and combined to reduce the search space.

It is a Python library that enables query rewriting of SQL statements. 
It can be used via CLI or as a Python library.
Currently, PostgreSQL can be used as an underlying database for CLI operations that require access to the underlying dataset, while various choices are available as a cache backend.


To efficiently utilize this approach, the working environment needs to fulfill the following properties:
- Complex SQL queries which need more than a few milliseconds to compute
- Readonly dataset (Appendable datasets are WIP)
- (Analytical) Searches across separate partitions (e.g. Cities, spatial areas, Days for time series or sets of connected networks of property graphs)
    - This will work only partially with aggregations across partitions
- To enable positive effects on new queries, different subqueries need to be conjunctive (AND) connected




## How does it work?

Reduction of the search space

- Incoming queries get recomposed in various variants and are executed in parallel, retrieving sets of all partitions in which at least one match was found
- All recomposed queries get stored together with their set of partition identifiers.
- Frequent subqueries can also be stored with their partition identifiers
- New queries get checked against the cache. If the query or a part of the query is present in the cache, the search space can be restricted to the stored partition identifiers


###  Example

"For all Cities in the world, find where a public park larger than 1000 m² lies within 50m of a street named "main street" ."

For this type of search, we can separate the search space for each city in our dataset. This would not help much on a normal search, but with PartitionCache, we can store the cities for which this search was returning at least one result.
In case we have later another search that uses the same query, for example, "For all Cities in Europe, find where a public park larger than 1000 m² lies within 50m of a street named "main street".	" We do not need to recompute this task for all cities, we can take the list of cities from the earlier search and only check for the additional constraints ("in Europe"). As the cities are provided in SQL, it does not restrict the database optimizer.

Further, as we observed the condition "a public park larger than 1000 m²," We created a set of cities where the park exists and also sped up following queries that asked for other properties. 
Like "all schools in Europe which are within 1km of a public park larger than 1000 m²" in this case, we can intersect the List of cities in Europe with the List of Cities with a park and need to look for schools only in these cities.


## Install

```
pip install git+https://github.com/MPoppinga/PartitionCache@main
```

##  Usage

Notes:
At the current state the library does only support a specific subset of SQL syntax which is available for PostgreSQL.
For Example CTEs are not supported and JOINS and Subqueries are only partially supported. We aim to increase the robustness and flexibility of our approach in future releases.


### CLI Usage

####  Cache population
Usage: pcache-observer 


pcache-observer is a process, that monitors the queue for new cache requests, allowing asynchronous population of the cache

```
pcache-observer \
  --db_backend=postgresql \
  --db-name=mydb \
  --db_env-file=.env \
  --cache-backend=redis \
  --partition_key="partition_key" \ 
  --long_running_query_timeout=300

```


Usage: pcache-add 

pcache-add allows to add individual queries to the cache directly or send them to the queue

```
pcache-add \
  --query="SELECT * FROM table" \
  --db-name=mydb \
  --env-file=.env \
  --cache-backend=redis

```


```
pcache-add \
  --query="SELECT * FROM table" \
  --queue \
  --env-file=.env
```

#### Cache Usage

Usage: pcache-get

pcache-get returns the list of all partition keys based on the provided query, restricting the search space to as few partition_keys as possible

```
pcache-get \
  --query="SELECT * FROM table" \
  --cache-backend=rocksdb

```


#### Cache Management

pcache-manage allows to copy, delete, archive entries from the individial cache backends thath have been set up

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
If using a python application its possible to direclty call the relevant functions. # TODO comprehensive documenttion



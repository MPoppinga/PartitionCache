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

## Multi-Partition Support

PartitionCache supports multiple partition keys with different datatypes:

- **Multiple Partition Keys**: Manage different partition strategies simultaneously (e.g., `city_id`, `region_id`, `time_bucket`)
- **Multiple Datatypes**: Different partitions can store different data types (integer, float, text, timestamp)
- **Variable Bit Lengths**: Bit array handlers support different bit sizes per partition
- **Automatic Validation**: Datatype conflicts are automatically detected and prevented

See [Multi-Partition Documentation](docs/cache_handlers_multi_partition.md) for detailed usage examples.

##  API

PartitionCache provides a clean, consistent API with automatic datatype validation and helpful error messages:

```python
import partitioncache

# Create a cache handler with automatic datatype validation
cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")

# Store partition keys for a query
cache.set_set("query_hash", {1, 2, 3, 4, 5})

# Retrieve partition keys
partition_keys = cache.get("query_hash")

# Get partition keys for a complex query
partition_keys, subqueries, hits = partitioncache.get_partition_keys(
    query="SELECT * FROM users WHERE age > 25",
    cache_handler=cache.underlying_handler,
    partition_key="user_id"
)

# Extend query with cached partition keys
optimized_query = partitioncache.extend_query_with_partition_keys(
    query="SELECT * FROM users WHERE age > 25",
    partition_keys=partition_keys,
    partition_key="user_id"
)


# List all available backends and their supported datatypes
backends = partitioncache.list_cache_types()
print(backends["postgresql_array"])  # ['integer', 'float', 'text', 'timestamp']
```

### Example

"For all cities in the world, find where a public park larger than 1000 m² lies within 50m of a street named 'main street'."

For this type of search, we can separate the search space for each city in our dataset. This would not help much on a normal search, but with PartitionCache, we can store the cities for which this search returned at least one result.

In case we have a later search that uses the same query, for example, "For all cities in Europe, find where a public park larger than 1000 m² lies within 50m of a street named 'main street'," we do not need to recompute this task for all cities. We can take the list of cities from the earlier search and only check for the additional constraints ("in Europe"). As the cities are provided in SQL, it does not restrict the database optimizer.

Further, as we observed the condition "a public park larger than 1000 m²," we created a set of cities where the park exists and also sped up following queries that asked for other properties. 
For example, "all schools in Europe which are within 1km of a public park larger than 1000 m²" - in this case, we can intersect the list of cities in Europe with the list of cities with a park and need to look for schools only in these cities.

## Install

```
pip install git+https://github.com/MPoppinga/PartitionCache@main
```

## Usage

Notes:
At the current state, the library only supports a specific subset of SQL syntax which is available for PostgreSQL.
For example, CTEs are not supported, and JOINs and subqueries are only partially supported. We aim to increase the robustness and flexibility of our approach in future releases.

An example workload is available at [examples/openstreetmap_poi](examples/openstreetmap_poi/).

### Python API

#### Basic Usage

```python
import partitioncache

# Create cache with automatic datatype validation
cache = partitioncache.create_cache(
    cache_type="postgresql_array",
    partition_key="city_id", 
    datatype="integer"
)

# Store query results
cache.set_set("query_hash_123", {1, 5, 10, 15})

# Retrieve cached results
partition_keys = cache.get("query_hash_123")
# Returns: {1, 5, 10, 15}

# Check available cache types and datatypes
print(partitioncache.list_cache_types())
# Returns: {'postgresql_array': ['integer', 'float', 'text', 'timestamp'], ...}
```

#### Advanced Query Processing

```python
# Get partition keys for a complex query
partition_keys, num_subqueries, cache_hits = partitioncache.get_partition_keys(
    query="SELECT * FROM pois WHERE type='restaurant' AND city_id IN (1,2,3)",
    cache_handler=cache.underlying_handler,
    partition_key="city_id"
)

if partition_keys:
    # Extend original query with cached partition keys
    optimized_query = partitioncache.extend_query_with_partition_keys(
        query="SELECT * FROM pois WHERE type='restaurant'",
        partition_keys=partition_keys,
        partition_key="city_id",
        method="IN"
    )
    print(f"Optimized query: {optimized_query}")
```

#### Multiple Partition Keys

```python
# Create handlers for different partition strategies
zipcode_cache = partitioncache.create_cache("postgresql_array", "zipcode", "integer")
district_cache = partitioncache.create_cache("postgresql_array", "district", "text")

# Store data in different partitions
zipcode_cache.set_set("restaurants_query", {10001, 10002, 10003})
district_cache.set_set("restaurants_query", {"Manhattan", "Brooklyn"})
```

### CLI Usage

#### Cache Population

Add queries to cache directly:

```bash
pcache-add --direct \
  --query "SELECT DISTINCT city_id FROM pois WHERE type='restaurant'" \
  --partition-key "city_id" \
  --partition-datatype "integer" \
  --cache-backend "postgresql_array" \
  --env .env
```

Add queries to processing queue:

```bash
pcache-add --queue-original \
  --query "SELECT DISTINCT city_id FROM pois WHERE type='restaurant'" \
  --partition-key "city_id" \
  --partition-datatype "integer" \
  --env .env
```

Add from file with automatic backend recommendation:

```bash
# The tool will suggest optimal backends for your datatype
pcache-add --direct \
  --query-file "my_query.sql" \
  --partition-key "zipcode" \
  --partition-datatype "integer" \
  --cache-backend "$(pcache-add --help | grep 'Recommended')" \
  --env .env
```

#### Queue Monitoring

Monitor and process queued queries:

```bash
pcache-observer \
  --cache-backend "postgresql_array" \
  --db-backend "postgresql"
```

#### Cache Usage

Retrieve partition keys for a query:

```bash
pcache-get \
  --query "SELECT * FROM pois WHERE type='restaurant'" \
  --partition-key "city_id" \
  --partition-datatype "integer" \
  --cache-backend "postgresql_array" \
  --env .env
```

Get partition keys in different formats:

```bash
# JSON format
pcache-get --query "SELECT * FROM pois WHERE type='restaurant'" \
  --partition-key "city_id" --partition-datatype "integer" \
  --cache-backend "postgresql_array" --output-format json --env .env

# One per line
pcache-get --query "SELECT * FROM pois WHERE type='restaurant'" \
  --partition-key "city_id" --partition-datatype "integer" \
  --cache-backend "postgresql_array" --output-format lines --env .env
```

#### Cache Management

```bash
# Export cache
pcache-manage --export \
  --cache "postgresql_array" \
  --file "cache_backup.pkl"

# Copy between cache types
pcache-manage --copy \
  --from "rocksdb" \
  --to "postgresql_array"

# Delete specific partition
pcache-manage --delete-partition \
  --cache "postgresql_array" \
  --partition-key "city_id"
```

### Supported Cache Backends

| Backend | Integer | Float | Text | Timestamp | Notes |
|---------|---------|-------|------|-----------|-------|
| `postgresql_array` | ✓ | ✓ | ✓ | ✓ | Full-featured, recommended |
| `postgresql_bit` | ✓ | ✗ | ✗ | ✗ | Memory efficient for integers |
| `redis` | ✓ | ✗ | ✓ | ✗ | Distributed caching |
| `redis_bit` | ✓ | ✗ | ✗ | ✗ | Memory efficient Redis |
| `rocksdb` | ✓ | ✗ | ✓ | ✗ | File-based, good for development |
| `rocksdb_bit` | ✓ | ✗ | ✗ | ✗ | Memory efficient file-based |
| `rocksdict` | ✓ | ✓ | ✓ | ✓ | Full-featured file-based |

## Examples

### OpenStreetMap POI Example

A comprehensive example using real OpenStreetMap data with both zipcode and administrative district partitioning:

```bash
cd examples/openstreetmap_poi
docker-compose up -d
python process_osm_data.py
python run_poi_queries.py
```

See [examples/openstreetmap_poi/README.md](examples/openstreetmap_poi/README.md) for detailed instructions.

## License

PartitionCache is licensed under the GNU Lesser General Public License v3.0.

See [COPYING](COPYING) and [COPYING.LESSER](COPYING.LESSER) for more details.

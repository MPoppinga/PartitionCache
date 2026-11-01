# PartitionCache API Reference

This document provides a comprehensive reference for the PartitionCache Python API, including all public functions, classes, and their usage patterns.

## Module Structure

```
partitioncache/
├── __init__.py           # Main API exports
├── apply_cache.py        # Cache application functions
├── query_processor.py    # Query processing utilities
├── queue.py             # Queue operations
├── cache_handler/       # Cache backend implementations
├── queue_handler/       # Queue provider implementations
└── db_handler/          # Database abstraction layer
```

## Core API Functions

### Cache Management

#### `create_cache_helper(cache_type: str, partition_key: str, datatype: str | None, **kwargs) -> PartitionCacheHelper`

Creates a cache helper with automatic validation and configuration.

**Parameters:**
- `cache_type` (str): Backend type (`postgresql_array`, `redis_set`, etc.)
- `partition_key` (str): Default partition key name
- `datatype` (str | None): Data type (`integer`, `float`, `text`, `timestamp`). Can be `None` if the partition key is already registered.
- `**kwargs`: Additional configuration options (e.g., bitsize for bit handlers)

**Returns:**
- `PartitionCacheHelper`: Configured cache helper instance

**Example:**
```python
import partitioncache

# PostgreSQL array backend with integer partitions
cache = partitioncache.create_cache_helper(
    "postgresql_array", 
    "user_id", 
    "integer"
)

# Redis backend with text partitions
cache = partitioncache.create_cache_helper(
    "redis_set",
    "city_name", 
    "text"
)

# RocksDB with all datatype support
cache = partitioncache.create_cache_helper(
    "rocksdict",
    "timestamp_bucket",
    "timestamp"
)
```

#### `list_cache_types()`

Lists all available cache backends and their datatype support.

**Returns:**
- `Dict[str, List[str]]`: Backend names mapped to supported datatypes

**Example:**
```python
backends = partitioncache.list_cache_types()
print(backends)
# {
#   'redis_set': ['integer', 'text'],
#   'redis_bit': ['integer'],
#   'redis_roaringbit': ['integer'],
#   'rocksdict': ['float', 'geometry', 'integer', 'text', 'timestamp'],
#   'rocksdict_roaringbit': ['integer'],
#   'postgresql_array': ['float', 'integer', 'text', 'timestamp'],
#   'postgresql_bit': ['integer'],
#   'postgresql_roaringbit': ['integer'],
#   'postgis_bbox': ['geometry'],
#   'rocksdb_set': ['integer', 'text'],  # only when RocksDB is installed
#   'rocksdb_bit': ['integer']           # only when RocksDB is installed
# }

# Check if backend supports datatype
if 'float' in backends['postgresql_array']:
    print("PostgreSQL array supports float datatypes")
```

**Note:** `get_cache_handler()` additionally accepts the backends `duckdb_bit` and `rocksdict_h3_grouped`, which are not listed by `list_cache_types()`.

### Query Processing

#### `get_partition_keys(query: str, cache_handler: AbstractCacheHandler, partition_key: str, min_component_size=2, canonicalize_queries=False, follow_graph: bool = True, auto_detect_partition_join: bool = True, partition_join_table: str | None = None, bucket_steps: float = 1.0, add_constraints: dict[str, str] | None = None, remove_constraints_all: list[str] | None = None, remove_constraints_add: list[str] | None = None, skip_partition_key_joins: bool = False, geometry_column: str | None = None, max_conditions_removed: int | None = None, protected_patterns: list[str] | None = None)`

Retrieves cached partition keys for a query with comprehensive statistics.

**Parameters:**
- `query` (str): SQL query to analyze
- `cache_handler` (AbstractCacheHandler): Cache backend instance
- `partition_key` (str): Partition column name
- `min_component_size` (int): The minimum number of tables in the partial queries.
- `canonicalize_queries` (bool): If True, the query is canonicalized before hashing.
- `follow_graph` (bool): If True, only connected partial queries are considered (default: True)
- `auto_detect_partition_join` (bool): Whether to auto-detect partition-join tables (default: True)
- `partition_join_table` (str | None): Explicitly specified partition-join table alias or name
- `bucket_steps` (float): Step size for normalizing distance conditions (default: 1.0)
- `add_constraints` (dict[str, str] | None): Dict mapping table names to constraints to add
- `remove_constraints_all` (list[str] | None): List of attribute names to remove from all query variants
- `remove_constraints_add` (list[str] | None): List of attribute names to remove, creating additional variants
- `skip_partition_key_joins` (bool): Spatial mode — generate query variants without partition-key equijoins (default: False)
- `geometry_column` (str | None): Geometry column for spatial variant generation
- `max_conditions_removed` (int | None): Upper bound on the number of conditions removed when generating reduced variants
- `protected_patterns` (list[str] | None): Condition substrings that must never be removed

**Returns:**
- `Tuple[Set, int, int]`: (partition_keys, subqueries_generated, cache_hits)

**Example:**
```python
import partitioncache

cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")

# Check for cached partition keys
partition_keys, subqueries, hits = partitioncache.get_partition_keys(
    query="SELECT * FROM orders WHERE status = 'pending'",
    cache_handler=cache.underlying_handler,
    partition_key="user_id"
)

print(f"Found {len(partition_keys)} partition keys")
print(f"Cache hits: {hits}/{subqueries} subqueries")
```

#### `get_partition_keys_lazy(query, cache_handler, partition_key, ...)`

Same parameters as `get_partition_keys`, but instead of materializing the partition keys it returns a lazy SQL subquery that computes the intersection inside the database. Requires a lazy-capable backend (`AbstractCacheHandler_Lazy`). Used internally by `apply_cache_lazy()`.

**Returns:**
- `Tuple[str | None, int, int]`: (lazy_subquery, subqueries_generated, cache_hits)

#### `extend_query_with_partition_keys(query: str, partition_keys: set[int] | set[str] | set[float] | set[datetime], partition_key: str, method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"] = "IN", p0_alias: str | None = None, analyze_tmp_table: bool = True, auto_detect_partition_join: bool = True, partition_join_table: str | None = None)`

Optimizes a query by adding partition key restrictions.

**Parameters:**
- `query` (str): Original SQL query
- `partition_keys` (set[int] | set[str] | set[float] | set[datetime]): Partition keys to restrict to
- `partition_key` (str): Partition column name
- `method` (Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"]): Optimization method
- `p0_alias` (str | None): The alias of the table to use for the partition key.
- `analyze_tmp_table` (bool): Whether to create index and analyze. (only for temporary table methods)
- `auto_detect_partition_join` (bool): Whether to auto-detect partition-join tables when resolving the target alias (default: True)
- `partition_join_table` (str | None): Explicitly specified partition-join table alias or name

**Returns:**
- `str`: Optimized SQL query

**Example:**
```python
# Original query
query = "SELECT * FROM users WHERE age > 25"
partition_keys = {1, 5, 10, 15, 20}

# Add partition restrictions
optimized = partitioncache.extend_query_with_partition_keys(
    query=query,
    partition_keys=partition_keys,
    partition_key="user_id",
    method="IN"
)

print(optimized)
# SELECT * FROM users WHERE age > 25 AND user_id IN (1, 5, 10, 15, 20)
```

> **Executing temp-table methods (`TMP_TABLE_IN` / `TMP_TABLE_JOIN`).** These methods return a
> **multi-statement script**: `CREATE TEMPORARY TABLE ... ON COMMIT DROP; INSERT ...; ANALYZE ...; SELECT ...`.
> The temp table uses `ON COMMIT DROP`, so the **entire script must run in a single transaction** — otherwise
> the table is dropped right after `CREATE` (and never reclaimed if you split statements across separate
> autocommit transactions). Because the final `SELECT` is the *last* statement, a plain `fetchall()` after
> `cursor.execute(script)` reads the first statement (`CREATE TABLE`, no rows); walk `cursor.nextset()` to the
> last result set instead. The library's `PostgresDBHandler.execute` does this for you (see
> `partitioncache.db_handler.postgres.fetch_final_result_set`). Minimal pattern with a raw psycopg cursor:
>
> ```python
> from partitioncache.db_handler.postgres import fetch_final_result_set
>
> with conn.cursor() as cur:           # one execute() == one transaction
>     cur.execute(optimized)           # the multi-statement script
>     rows = fetch_final_result_set(cur)  # rows of the final SELECT; temp table auto-dropped on commit
> ```

#### `extend_query_with_partition_keys_lazy(query: str, lazy_subquery: str, partition_key: str, method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"] = "IN_SUBQUERY", p0_alias: str | None = None, analyze_tmp_table: bool = True, auto_detect_partition_join: bool = True, partition_join_table: str | None = None, sql_dialect: str = "postgres")`

Lazy counterpart to `extend_query_with_partition_keys`: integrates a lazy SQL subquery (as returned by `get_partition_keys_lazy()`) into the query instead of a materialized key set.

**Returns:**
- `str`: Extended SQL query

#### `apply_cache_lazy(query: str, cache_handler: AbstractCacheHandler_Lazy, partition_key: str, method: str = "IN_SUBQUERY", ..., sql_dialect: str = "postgres", geometry_column: str | None = None, buffer_distance: float | None = None, **kwargs)`

**Recommended** - High-performance cache application with lazy evaluation. Supports spatial mode.

**Parameters:**
- `query` (str): SQL query to optimize
- `cache_handler` (AbstractCacheHandler_Lazy): Cache backend instance
- `partition_key` (str): Partition column name
- `method` (Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]): Integration method
- `p0_alias` (str | None, optional): Table alias for cache restrictions
- `min_component_size` (int, optional): Minimum size of query components to consider for cache lookup (default: 2)
- `canonicalize_queries` (bool, optional): Whether to canonicalize queries before hashing (default: False)
- `follow_graph` (bool, optional): Whether to follow the query graph for generating variants (default: True)
- `analyze_tmp_table` (bool, optional): Enable temp table analysis (default: True)
- `sql_dialect` (str, optional): sqlglot dialect used to parse and render non-spatial queries (default: `"postgres"`; use `"duckdb"` for DuckDB queries)
- `use_p0_table` (bool, optional): Rewrite the query to use a p0 table/star-schema (default: False)
- `p0_table_name` (str | None, optional): Name of the p0 table. Defaults to `{partition_key}_mv`
- `auto_detect_partition_join` (bool, optional): Whether to auto-detect partition-join tables (default: True)
- `partition_join_table` (str | None, optional): Explicitly specified partition-join table alias or name
- `bucket_steps` (float, optional): Step size for normalizing distance conditions (default: 1.0)
- `add_constraints` (dict[str, str] | None, optional): Dict mapping table names to constraints to add
- `remove_constraints_all` (list[str] | None, optional): List of attribute names to remove from all query variants
- `remove_constraints_add` (list[str] | None, optional): List of attribute names to remove, creating additional variants
- `max_conditions_removed` (int | None, optional): Upper bound on the number of conditions removed when generating reduced variants (default: None)
- `protected_patterns` (list[str] | None, optional): Condition substrings that must never be removed (default: None)
- `skip_partition_key_joins` (bool, optional): Spatial mode — generate query variants without partition-key equijoins (default: False)
- `geometry_column` (str | None, optional): Enables spatial mode. Uses this geometry column for variant SELECT clauses and spatial filter. Requires a spatial cache handler with `get_spatial_filter_lazy()` method.
- `buffer_distance` (float | None, optional): Buffer distance in meters for spatial filter (required when `geometry_column` is set).
- `spatial_method` (Literal["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE", "DUMP_TMP_TABLE", "DUMP_CTE"], optional): Strategy for applying the spatial filter (default: "SUBDIVIDE_TMP_TABLE")
- `subdivide_max_vertices` (int, optional): Maximum vertices per subdivided filter geometry (default: 256)
- `cost_estimator` (Callable[[str], float | None] | None, optional): Returns the planner cost of a query without executing it, e.g. `PostgresDBHandler.estimate_cost`. Only used together with `min_query_cost` (default: None)
- `min_query_cost` (float | None, optional): Skip the cache when the estimated cost of the original query is below this value — see [Cost gate](#cost-gate-min_query_cost) (default: None, gate off)

**Returns:**
- `Tuple[str, Dict]`: (enhanced_query, statistics). With `min_query_cost` set, the statistics also contain `cost_gated` (1 if the cache was skipped) and `query_cost` (rounded estimate, when available)

**Example:**
```python
# Best-performance approach
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM orders o WHERE status = 'pending'",
    cache_handler=cache.underlying_handler,
    partition_key="user_id",
    method="TMP_TABLE_IN",
    p0_alias="o"
)

print(f"Cache performance: {stats['cache_hits']}/{stats['generated_variants']}")
print(f"Query enhanced: {stats['enhanced']}")

# Execute enhanced query directly
# enhanced_query contains complete optimized SQL
```

**Spatial Mode Example:**
```python
# Spatial cache application with PostGIS H3 handler
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'",
    cache_handler=spatial_cache_handler,
    partition_key="spatial_h3",
    geometry_column="geom",
    buffer_distance=500.0,  # meters
    min_component_size=1,
)

if stats['enhanced']:
    print(f"Query enhanced with spatial filter ({stats['cache_hits']} cache hits)")
```

**Performance Benefits:**
- Lazy SQL subquery evaluation
- Optimal database query plan integration

**Integration Methods:**

```python
# Method 1: Direct subquery integration (fastest for simple queries or a low number of partitions)
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM products WHERE category = 'electronics'",
    cache_handler=cache.underlying_handler,
    partition_key="store_id",
    method="IN_SUBQUERY"
)

# Method 2: Temporary table with IN clause (best for complex queries or a high number of partitions)
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM products WHERE category = 'electronics'",
    cache_handler=cache.underlying_handler,
    partition_key="store_id",
    method="TMP_TABLE_IN",
    analyze_tmp_table=True  # Enables indexing and statistics
)

# Method 3: Temporary table with JOIN (best for large result sets)
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT p.*, s.name FROM products p JOIN stores s ON p.store_id = s.id",
    cache_handler=cache.underlying_handler,
    partition_key="store_id",
    method="TMP_TABLE_JOIN"
)
```

#### Cost gate (`min_query_cost`)

For cheap queries the cache overhead (variant generation, cache lookup, temp table) exceeds what the restriction can save. `apply_cache_lazy` and `apply_cache` can therefore ask the database planner first and return the query unchanged when its estimated cost is below `min_query_cost`. The check runs before variant generation, so a gated query costs one `EXPLAIN` only.

```python
from partitioncache.db_handler.postgres import PostgresDBHandler

data_db = PostgresDBHandler(host, port, user, password, dbname)  # the data database, not the cache database
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query=query,
    cache_handler=cache.underlying_handler,
    partition_key="user_id",
    cost_estimator=data_db.estimate_cost,
    min_query_cost=10_000,  # calibrate per installation, see below
)
if stats.get("cost_gated"):
    print(f"cache skipped, planner cost {stats['query_cost']}")
```

- `estimate_cost` is implemented for PostgreSQL (`EXPLAIN (FORMAT JSON)`, `Plan.Total Cost`) and MySQL (`EXPLAIN FORMAT=JSON`, `query_block.cost_info.query_cost`). DuckDB, SQLite and MariaDB return `None` — they expose no scalar cost — and the cache is applied as usual. Any callable with the same signature works as `cost_estimator`.
- Cost units are planner units, not milliseconds, and depend on `seq_page_cost`/`random_page_cost`; the threshold has to be calibrated per installation and workload, e.g. by comparing `estimate_cost` of representative queries with their measured runtime with and without the cache.
- The gate only catches queries that are cheap *according to the planner*. Planner over-estimates (a query estimated expensive that finishes in milliseconds) pass the gate and get the cache as before; large cached key sets and bad plans for the enhanced query are not covered.
- A failing estimator, or one that returns something other than a finite number, is logged and the cache is applied; `min_query_cost` without `cost_estimator` raises `ValueError`.
- Every query that is not gated pays one extra `EXPLAIN` round trip on the data connection.
- `PostgresDBHandler.estimate_cost` runs the `EXPLAIN` in a transaction (or savepoint) that is always rolled back, so it does not commit or abort a transaction the caller has open, and it rejects text that contains more than one statement. MySQL returns `None` for plans without a top-level cost, such as `UNION`.

#### `apply_cache(query, cache_handler, partition_key, ..., geometry_column=None, buffer_distance=None)`

Applies partition cache to a query using regular (non-lazy) cache handlers. Supports spatial mode. Accepts the same `cost_estimator` / `min_query_cost` gate as `apply_cache_lazy`.

**Spatial Parameters** (in addition to standard parameters):
- `geometry_column` (str | None, optional): Enables spatial mode. Requires a spatial cache handler with `get_spatial_filter()` method.
- `buffer_distance` (float | None, optional): Buffer distance in meters for spatial filter (required when `geometry_column` is set).

**Example:**
```python
# Non-lazy spatial cache application
enhanced_query, stats = partitioncache.apply_cache(
    query="SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'",
    cache_handler=spatial_cache_handler,
    partition_key="spatial_h3",
    geometry_column="geom",
    buffer_distance=500.0,
    min_component_size=1,
)
```

#### `extend_query_with_spatial_filter(query, spatial_filter_wkb, geometry_column, buffer_distance, srid=4326, p0_alias=None)`

Extends a SQL query with a pre-computed spatial filter geometry (WKB bytes). Non-lazy counterpart to `extend_query_with_spatial_filter_lazy()`.

**Parameters:**
- `query` (str): The original SQL query.
- `spatial_filter_wkb` (bytes): Pre-computed spatial filter geometry as WKB bytes.
- `geometry_column` (str): The geometry column name in the target table.
- `buffer_distance` (float): Buffer distance in meters.
- `srid` (int, optional): SRID of the spatial filter geometry (default: 4326).
- `p0_alias` (str | None, optional): Table alias to apply the filter to.

**Returns:**
- `str`: The extended SQL query with spatial filter.

#### `extend_query_with_spatial_filter_lazy(query, spatial_filter_sql, geometry_column, buffer_distance, srid=4326, p0_alias=None)`

Extends a SQL query with a spatial filter using a lazy SQL subquery that produces geometry.

**Parameters:**
- `query` (str): The original SQL query.
- `spatial_filter_sql` (str): SQL subquery returning a geometry to filter against.
- `geometry_column` (str): The geometry column name in the target table.
- `buffer_distance` (float): Buffer distance in meters (uses geography cast for SRID 4326; skipped for metric SRIDs).
- `srid` (int, optional): SRID of the spatial filter geometry (default: 4326).
- `p0_alias` (str | None, optional): Table alias to apply the filter to.

**Returns:**
- `str`: The extended SQL query with spatial filter.

Both spatial extension functions additionally accept `use_intersects`, `spatial_method`, `subdivide_max_vertices`, `auto_detect_partition_join`, and `partition_join_table`.

#### `extend_query_with_h3_cell_filter_lazy(query, cell_sql, geometry_column, srid, resolution, p0_alias=None)`

Extends a query with an H3 cell membership filter from a SQL subquery producing cell IDs: creates an indexed temporary table from `cell_sql` and adds `h3_lat_lng_to_cell(<centroid>, resolution)::bigint IN (SELECT cell FROM <tmp>)`. Pure cell lookup — no geometry reconstruction.

**Returns:**
- `str`: Multi-statement script (same single-transaction rules as the other temp-table methods).

#### `extend_query_with_h3_cell_filter(query, cell_ids, geometry_column, srid, resolution, p0_alias=None)`

Non-lazy counterpart: builds the same filter from a pre-computed `set[int]` of H3 cell IDs (temporary table from a `VALUES` clause).

#### `extend_query_with_h3_cell_lookup(query, cell_ids, cell_mode="inline", h3_cell_table=None, h3_cell_column="h3_cell_id", h3_cell_id_column=None, p0_alias=None, geometry_column=None, srid=4326, resolution=9, filter_all_tables=True)`

Filters a query via H3 cell-ID lookup with three modes: `"inline"` (compute `h3_lat_lng_to_cell` on the fly from `geometry_column`), `"mv"` (join a materialized view named in `h3_cell_table`), or `"column"` (use a pre-computed cell-ID column named in `h3_cell_id_column`). With `filter_all_tables=True` (default), all aliases referencing the same base table are filtered, giving the planner accurate cardinality estimates for cross-join queries.

**Returns:**
- `str`: The extended SQL query.

### Queue Operations

#### `push_to_original_query_queue(query: str, partition_key: str = "partition_key", partition_datatype: str | None = None, queue_provider: str | None = None)`

Adds queries to the processing queue for background cache population.

**Parameters:**
- `query` (str): SQL query to process
- `partition_key` (str): Partition column name
- `partition_datatype` (str | None): Partition data type
- `queue_provider` (str | None): The queue provider to use.

**Returns:**
- `bool`: Success status

**Example:**
```python
# Add query for background processing
success = partitioncache.push_to_original_query_queue(
    query="SELECT DISTINCT city_id FROM restaurants WHERE rating > 4.0",
    partition_key="city_id",
    partition_datatype="integer"
)

if success:
    print("Query added to processing queue")
```

#### `push_to_query_variant_queue(query_hash_pairs: list[tuple[str, str]], partition_key: str = "partition_key", partition_datatype: str = "integer", queue_provider: str | None = None, cache_backend: str | None = None)`

Pushes pre-decomposed query variants — `(query, hash)` pairs as produced by `generate_all_query_hash_pairs()` — directly to the second-tier queue, bypassing decomposition. This is the preferred public name; `push_to_query_fragment_queue` is the historical name for the same callable and remains available.

**Returns:**
- `bool`: True if all pairs were pushed successfully

**Example:**
```python
from partitioncache.query_processor import generate_all_query_hash_pairs

pairs = generate_all_query_hash_pairs(query, partition_key="city_id")
partitioncache.push_to_query_variant_queue(pairs, partition_key="city_id", partition_datatype="integer")
```

#### `get_queue_lengths(queue_provider: str | None = None)`

Retrieves current queue status for monitoring.

**Parameters:**
- `queue_provider` (str | None): The queue provider to use.

**Returns:**
- `dict`: Dictionary with 'original_query_queue' and 'query_fragment_queue' queue lengths.

**Example:**
```python
lengths = partitioncache.get_queue_lengths()
original = lengths.get("original_query_queue", 0)
fragment = lengths.get("query_fragment_queue", 0)
print(f"Queues: {original} original, {fragment} fragments")

# Monitor processing progress
import time
for i in range(10):
    lengths = partitioncache.get_queue_lengths()
    orig = lengths.get("original_query_queue", 0)
    frag = lengths.get("query_fragment_queue", 0)
    print(f"Progress: {orig} + {frag} = {orig + frag} total")
    time.sleep(5)
```

## Cache Handler Classes

### AbstractCacheHandler

Base class for all cache backends implementing the core interface.

The examples in this section operate on a **raw handler instance** (e.g. `cache = partitioncache.get_cache_handler("postgresql_array", singleton=True)` or `helper.underlying_handler`), whose methods take an explicit `partition_key` argument. The `PartitionCacheHelper` (see below) binds the partition key instead.

#### Core Methods

##### `set_entry(key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], query_text: str, partition_key: str = "partition_key", force_update: bool = False) -> bool`

High-level method that atomically stores both cache data and query metadata.

This is the **preferred way** to populate the cache as it ensures both cache data and query metadata are stored consistently in a single operation.

**Parameters:**
- `key` (str): Cache key (usually query hash)
- `partition_key_identifiers` (Set): Set of partition key values to cache
- `query_text` (str): SQL query text to store with the cache entry
- `partition_key` (str): Partition identifier (default: "partition_key")
- `force_update` (bool): If True, always update cache data. If False, only update metadata if cache entry exists (default: False)

**Returns:**
- `bool`: Success status

**Example:**
```python
from partitioncache.query_processor import hash_query

# Generate hash for query
query = "SELECT * FROM restaurants WHERE rating > 4.0"
query_hash = hash_query(query)

# Store both cache data and query metadata atomically
success = cache.set_entry(
    key=query_hash,
    partition_key_identifiers={1, 5, 10, 15, 20},
    query_text=query,
    partition_key="city_id"
)

# Later retrieve with full context
cached_data = cache.get(query_hash, "city_id")
cached_query = cache.get_query(query_hash, "city_id")
```

##### `set_cache(key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool`

Stores a set of partition keys in the cache (cache data only).

**Note:** For most use cases, prefer `set_entry()` which also stores query metadata.

**Parameters:**
- `key` (str): Cache key (usually query hash)
- `partition_key_identifiers` (Set): Set of partition key values
- `partition_key` (str): Partition identifier

**Returns:**
- `bool`: Success status

**Example:**
```python
# Store partition keys only (query metadata stored separately)
success = cache.set_cache(
    key="restaurants_rating_4plus",
    value={1, 5, 10, 15, 20}
)
```

##### `get(key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None`

Retrieves cached partition keys.

**Parameters:**
- `key` (str): Cache key to retrieve
- `partition_key` (str): Partition identifier

**Returns:**
- `Set | None`: Cached partition keys or None if not found

**Example:**
```python
# Retrieve cached data
cached_cities = cache.get("restaurants_rating_4plus")
if cached_cities:
    print(f"Found {len(cached_cities)} cities in cache")
```

##### `exists(key: str, partition_key: str = "partition_key", check_query: bool = False) -> bool`

Checks if a cache entry exists. With `check_query=True`, query metadata is consulted first: a missing query record returns False; an `'ok'` status additionally checks the cache entry; an error status (`'timeout'`/`'failed'`) returns True without a cache check.

**Returns:**
- `bool`: True if key exists

##### `delete(key: str, partition_key: str = "partition_key") -> bool`

Removes a cache entry.

**Returns:**
- `bool`: Success status

##### `get_all_keys(partition_key="partition_key")`

Lists all cache keys for a partition.

**Returns:**
- `List[str]`: All cache keys

##### `get_partition_keys()`

Lists all partition keys and their datatypes.

**Returns:**
- `List[Tuple[str, str]]`: [(partition_key, datatype), ...]

**Example:**
```python
# Multi-partition usage
partitions = cache.get_partition_keys()
for partition_key, datatype in partitions:
    keys = cache.get_all_keys(partition_key)
    print(f"Partition '{partition_key}' ({datatype}): {len(keys)} keys")
```

#### Advanced Methods

##### `get_intersected(keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]`

Computes intersection of multiple cached sets.

**Parameters:**
- `keys` (Set[str]): Cache keys to intersect
- `partition_key` (str): Partition identifier

**Returns:**
- `Tuple[Set, int]`: (intersection_result, successful_lookups)

**Example:**
```python
# Find intersection of multiple query results
intersection, hits = cache.get_intersected(
    keys={"restaurants_italian", "restaurants_rating_4plus", "restaurants_downtown"}
)
print(f"Intersection: {intersection} (from {hits} cache hits)")
```

##### `get_intersected_lazy(keys: set[str], partition_key: str = "partition_key") -> tuple[str | None, int]`

**Advanced** - Returns lazy SQL subquery for intersection operations. Defined on `AbstractCacheHandler_Lazy` (lazy-capable backends such as the PostgreSQL family and `duckdb_bit`), not on the base class.

**Returns:**
- `Tuple[str | None, int]`: (sql_subquery, cache_hits)

**Example:**
```python
# Get lazy subquery for optimal database integration
lazy_subquery, hits = cache.get_intersected_lazy(
    keys={"restaurants_italian", "restaurants_rating_4plus"}
)

if lazy_subquery:
    optimized_query = f"""
        SELECT * FROM restaurants r 
        WHERE r.city_id IN ({lazy_subquery})
    """
```

### PartitionCacheHelper

High-level wrapper providing additional convenience methods and validation.

#### Properties

- `underlying_handler`: Access to the underlying cache handler
- `partition_key`: The partition key for this handler.
- `datatype`: The datatype for this handler.

#### Methods

The helper wraps a subset of the handler interface. All methods operate on the helper's bound partition key — they take **no** `partition_key` parameter. Failures are logged and return neutral values (False/None/empty) instead of raising.

- `set_entry(key, partition_key_identifiers, query_text, force_update=False)` - **Preferred** write path (cache data + query metadata)
- `set_cache(key, partition_key_identifiers)` - Store cache data only
- `get(key)` - Retrieve cached partition keys
- `exists(key, check_query=False)` - Check if entry exists
- `delete(key)` - Remove entry
- `set_null(key)` / `is_null(key)` - Negative-cache markers
- `set_query(key, querytext)` - Store query metadata only
- `get_intersected(keys)` - Intersect several cached sets
- `filter_existing_keys(keys, check_query=False)` - Subset of keys present in cache
- `get_all_keys()` - All keys of the bound partition
- `delete_partition()` - Drop the partition's data
- `close()` - Close the underlying handler

`LazyPartitionCacheHelper` (returned automatically by `create_cache_helper` for lazy-capable backends) additionally provides `get_intersected_lazy(keys)`.

Handler-level operations without a helper counterpart — e.g. `get_query`, `get_all_queries`, `register_partition_key`, `get_partition_keys` (list all partitions) — are reached via `underlying_handler`:

**Example:**
```python
# Create helper with default partition key
cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")

from partitioncache.query_processor import hash_query

# Store entry with query metadata (recommended)
query = "SELECT * FROM orders WHERE status = 'pending'"
query_hash = hash_query(query)
cache.set_entry(query_hash, {1, 5, 10}, query)

# Retrieve cache data and metadata
user_ids = cache.get(query_hash)
original_query = cache.underlying_handler.get_query(query_hash, cache.partition_key)
```

##### Working with additional partition keys

`register_partition_key(partition_key, datatype, **kwargs)` lives on the **handler**, not the helper. To work with several partition keys, create one helper per key (they share the same singleton handler), or use the handler directly:

```python
# One helper per partition key (recommended)
city_cache = partitioncache.create_cache_helper("postgresql_array", "city_id", "integer")
price_cache = partitioncache.create_cache_helper("postgresql_array", "price_range", "float")

price_cache.set_cache("expensive_items", {99.99, 199.99})

# Or register directly on the handler
cache.underlying_handler.register_partition_key("region_id", "integer")
```

## Query Processing Utilities

### `generate_all_query_hash_pairs(query: str, partition_key: str, min_component_size=1, follow_graph=True, keep_all_attributes=True, canonicalize_queries=False, ...)`

The exported lower-level API: generates all query variants and returns them as `list[tuple[str, str]]` of `(variant_query, hash)` pairs — the format consumed by `push_to_query_variant_queue()`. Accepts the full variant-generation parameter set (`auto_detect_partition_join`, `partition_join_table`, `max_component_size`, `bucket_steps`, `add_constraints`, `remove_constraints_all`, `remove_constraints_add`, `max_conditions_removed`, `protected_patterns`, and the spatial parameters `skip_partition_key_joins`/`geometry_column`).

### `generate_all_hashes(query: str, partition_key: str, min_component_size=1, follow_graph=True, fix_attributes=True, canonicalize_queries=False)`

Generates query variants and their hashes for cache lookup. Thin wrapper around `generate_all_query_hash_pairs` returning only the hashes.

**Parameters:**
- `query` (str): SQL query to process
- `partition_key` (str): Partition column name
- `min_component_size` (int): The minimum number of tables in the partial queries.
- `follow_graph` (bool): If True, only connected partial queries are returned.
- `fix_attributes` (bool): If True, only partial queries with original attributes are returned.
- `canonicalize_queries` (bool): If True, the query is canonicalized before hashing.

**Returns:**
- `List[str]`: A list of hashes.

**Example:**
```python
from partitioncache.query_processor import generate_all_hashes

# Generate variants for cache lookup
hashes = generate_all_hashes(
    query="SELECT * FROM users WHERE age > 25 AND city = 'NYC'",
    partition_key="user_id"
)

for hash_value in hashes:
    print(f"Hash: {hash_value}")
```

## Error Handling

PartitionCache uses standard Python exceptions for error handling. Most operations return boolean values or None to indicate success/failure states.

### Common Error Patterns

```python
import partitioncache

try:
    # Cache operations
    cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")
    result = cache.get("query_hash")
    
    # Check operation success
    if not cache.set_cache("key1", {1, 2, 3}):
        print("Failed to store data in cache")
        
except ValueError as e:
    print(f"Configuration or validation error: {e}")
    # Handle datatype mismatches, invalid parameters
    
except Exception as e:
    print(f"Unexpected error: {e}")
    # Handle database connection issues, etc.
```

## Configuration and Environment

### Environment Variables

```python
import os

# Database configuration
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PORT'] = '5432'
os.environ['DB_USER'] = 'partitioncache_user'
os.environ['DB_PASSWORD'] = 'secure_password'
os.environ['DB_NAME'] = 'partitioncache_db'

# Cache backend
os.environ['CACHE_BACKEND'] = 'postgresql_array'
os.environ['PG_ARRAY_CACHE_TABLE_PREFIX'] = 'myapp_cache_'

# Queue provider
os.environ['QUERY_QUEUE_PROVIDER'] = 'postgresql'
os.environ['PG_QUEUE_TABLE_PREFIX'] = 'myapp_queue_'

# Redis configuration (if using Redis backends)
os.environ['REDIS_HOST'] = 'redis-server'
os.environ['REDIS_PORT'] = '6379'
os.environ['REDIS_PASSWORD'] = 'redis_password'
```

### Programmatic Configuration

`get_cache_handler(cache_type, singleton=False)` accepts no connection kwargs — all connection settings are read from environment variables. Set them programmatically before creating the handler if needed:

```python
import os
from partitioncache.cache_handler import get_cache_handler

os.environ["DB_HOST"] = "custom-db-server"
os.environ["DB_NAME"] = "custom_db"
os.environ["DB_USER"] = "custom_user"
os.environ["DB_PASSWORD"] = "custom_password"
os.environ["DB_PORT"] = "5432"
os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "custom_cache_"

cache_handler = get_cache_handler("postgresql_array", singleton=True)
```

## Advanced Usage Patterns

### Multi-Partition Applications

```python
from datetime import datetime

# One helper per partition key; they share the same singleton handler
city_cache = partitioncache.create_cache_helper("postgresql_array", "city_id", "integer")
region_cache = partitioncache.create_cache_helper("postgresql_array", "region_name", "text")
time_cache = partitioncache.create_cache_helper("postgresql_array", "time_bucket", "timestamp")

# Store data across partitions (recommended: use set_entry with query text)
from partitioncache.query_processor import hash_query

spatial_query = "SELECT * FROM locations WHERE city_id IN (1,5,10)"
city_cache.set_entry(hash_query(spatial_query), {1, 5, 10}, spatial_query)

temporal_query = "SELECT * FROM events WHERE time_bucket >= '2024-01-01'"
time_cache.set_entry(hash_query(temporal_query), {datetime(2024, 1, 1), datetime(2024, 2, 1)}, temporal_query)

regional_query = "SELECT * FROM regions WHERE region_name IN ('north','south')"
region_cache.set_entry(hash_query(regional_query), {"north", "south"}, regional_query)

# Enumerate partitions via the underlying handler
handler = city_cache.underlying_handler
for partition_key, datatype in handler.get_partition_keys():
    keys = handler.get_all_keys(partition_key)
    print(f"Partition {partition_key} ({datatype}): {len(keys)} cached queries")
```

### Hierarchical Caching

```python
def implement_hierarchical_cache():
    """Implement fallback cache hierarchy."""
    
    # Create cache hierarchy
    primary_cache = partitioncache.create_cache_helper("postgresql_array", "city_id", "integer")
    fallback_cache = partitioncache.create_cache_helper("postgresql_array", "region_id", "integer")
    
    def get_with_fallback(query_hash):
        # Try primary cache first (each helper is bound to its partition key)
        result = primary_cache.get(query_hash)
        if result:
            return result, "city_level"
            
        # Fallback to region level
        result = fallback_cache.get(query_hash)
        if result:
            return result, "region_level"
            
        return None, "cache_miss"
    
    return get_with_fallback
```

### Performance Monitoring

```python
import time
from functools import wraps

def monitor_cache_performance(func):
    """Decorator for monitoring cache operations."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            print(f"Cache operation {func.__name__} completed in {duration:.3f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            print(f"Cache operation {func.__name__} failed after {duration:.3f}s: {e}")
            raise
    return wrapper

# Apply monitoring to cache operations
@monitor_cache_performance
def cached_query_lookup(query, cache, partition_key):
    return partitioncache.get_partition_keys(query, cache.underlying_handler, partition_key)
```

### Batch Operations

```python
def batch_cache_population(queries_and_results, cache):
    """Efficiently populate cache with multiple queries."""
    
    success_count = 0
    for query, partition_keys in queries_and_results:
        # Generate hash for query
        from partitioncache.query_processor import hash_query
        query_hash = hash_query(query)
        
        # Store in cache (helper is bound to its partition key)
        if cache.set_cache(query_hash, partition_keys):
            success_count += 1
    
    print(f"Batch population: {success_count}/{len(queries_and_results)} successful")
    return success_count
```

## Best Practices

### 1. Error Handling

```python
def robust_cache_operation(cache, operation, *args, **kwargs):
    """Robust cache operation with fallback."""
    try:
        result = getattr(cache, operation)(*args, **kwargs)
        if result is False:  # Operation failed
            print(f"Cache operation {operation} failed silently")
            return None
        return result
    except Exception as e:
        print(f"Cache operation {operation} raised exception: {e}")
        return None

# Usage
cached_data = robust_cache_operation(cache, 'get', 'query_hash')
```

### 2. Connection Management

```python
import atexit

def setup_cache_with_cleanup():
    """Setup cache with proper cleanup."""
    cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")
    
    # Register cleanup function
    atexit.register(lambda: cache.underlying_handler.close())
    
    return cache
```

### 3. Performance Optimization

```python
# Use apply_cache_lazy for optimal performance
def optimized_query_execution(query, cache, partition_key):
    """Execute query with optimal cache integration."""
    
    # Use lazy cache application
    enhanced_query, stats = partitioncache.apply_cache_lazy(
        query=query,
        cache_handler=cache.underlying_handler,
        partition_key=partition_key,
        method="TMP_TABLE_IN"  # Best for complex queries
    )
    
    if stats['enhanced']:
        print(f"Query optimized: {stats['cache_hits']}/{stats['generated_variants']} cache hits")
        # Execute enhanced_query which contains optimized SQL
        return enhanced_query
    else:
        print("No cache optimization available")
        return query
```

This API reference provides comprehensive coverage of all PartitionCache functionality with practical examples for effective usage in applications.

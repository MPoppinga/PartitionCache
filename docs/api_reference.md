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

#### `create_cache_helper(cache_type: str, partition_key: str, datatype: str | None) -> PartitionCacheHelper`

Creates a cache helper with automatic validation and configuration.

**Parameters:**
- `cache_type` (str): Backend type (`postgresql_array`, `redis_set`, etc.)
- `partition_key` (str): Default partition key name
- `datatype` (str | None): Data type (`integer`, `float`, `text`, `timestamp`). Can be `None` if the partition key is already registered.

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
#   'postgresql_array': ['float', 'integer', 'text', 'timestamp'],
#   'postgresql_bit': ['integer'],
#   'postgresql_roaringbit': ['integer'],
#   'redis_bit': ['integer'],
#   'redis_set': ['integer', 'text'],
#   'rocksdb_bit': ['integer'],
#   'rocksdb_set': ['integer', 'text'],
#   'rocksdict': ['float', 'integer', 'text', 'timestamp']
# }

# Check if backend supports datatype
if 'float' in backends['postgresql_array']:
    print("PostgreSQL array supports float datatypes")
```

### Query Processing

#### `get_partition_keys(query: str, cache_handler: AbstractCacheHandler, partition_key: str, min_component_size=2, canonicalize_queries=False)`

Retrieves cached partition keys for a query with comprehensive statistics.

**Parameters:**
- `query` (str): SQL query to analyze
- `cache_handler` (AbstractCacheHandler): Cache backend instance
- `partition_key` (str): Partition column name
- `min_component_size` (int): The minimum number of tables in the partial queries.
- `canonicalize_queries` (bool): If True, the query is canonicalized before hashing.

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

#### `extend_query_with_partition_keys(query: str, partition_keys: set[int] | set[str] | set[float] | set[datetime], partition_key: str, method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"] = "IN", p0_alias: str | None = None, analyze_tmp_table: bool = True)`

Optimizes a query by adding partition key restrictions.

**Parameters:**
- `query` (str): Original SQL query
- `partition_keys` (set[int] | set[str] | set[float] | set[datetime]): Partition keys to restrict to
- `partition_key` (str): Partition column name
- `method` (Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"]): Optimization method
- `p0_alias` (str | None): The alias of the table to use for the partition key.
- `analyze_tmp_table` (bool): Whether to create index and analyze. (only for temporary table methods)

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

#### `apply_cache_lazy(query: str, cache_handler: AbstractCacheHandler_Lazy, partition_key: str, method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"] = "IN_SUBQUERY", p0_alias: str | None = None, min_component_size: int = 2, canonicalize_queries: bool = False, follow_graph: bool = True, analyze_tmp_table: bool = True, use_p0_table: bool = False, p0_table_name: str | None = None, auto_detect_star_join: bool = True, star_join_table: str | None = None, bucket_steps: float = 1.0, add_constraints: dict[str, str] | None = None, remove_constraints_all: list[str] | None = None, remove_constraints_add: list[str] | None = None)`

**Recommended** - High-performance cache application with lazy evaluation.

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
- `use_p0_table` (bool, optional): Rewrite the query to use a p0 table/star-schema (default: False)
- `p0_table_name` (str | None, optional): Name of the p0 table. Defaults to `{partition_key}_mv`
- `auto_detect_star_join` (bool, optional): Whether to auto-detect star-join tables (default: True)
- `star_join_table` (str | None, optional): Explicitly specified star-join table alias or name
- `bucket_steps` (float, optional): Step size for normalizing distance conditions (default: 1.0)
- `add_constraints` (dict[str, str] | None, optional): Dict mapping table names to constraints to add
- `remove_constraints_all` (list[str] | None, optional): List of attribute names to remove from all query variants
- `remove_constraints_add` (list[str] | None, optional): List of attribute names to remove, creating additional variants

**Returns:**
- `Tuple[str, Dict]`: (enhanced_query, statistics)

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

##### `set_cache(key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool`

Stores a set of partition keys in the cache (cache data only).

**Note:** For most use cases, prefer `set_entry()` which also stores query metadata.

**Parameters:**
- `key` (str): Cache key (usually query hash)
- `value` (Set): Set of partition key values
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

##### `exists(key: str, partition_key: str = "partition_key") -> bool`

Checks if a cache entry exists.

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

**Advanced** - Returns lazy SQL subquery for intersection operations.

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

All `AbstractCacheHandler` methods are available on the helper, but without the `partition_key` parameter since it uses the helper's default partition key.

**Recommended methods:**
- `set_entry(key, partition_key_identifiers, query_text, force_update=False)` - **Preferred** method for storing cache entries with query metadata
- `get(key)` - Retrieve cached partition keys
- `get_query(key)` - Retrieve query metadata
- `exists(key, check_query=False)` - Check if entry exists

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
original_query = cache.get_query(query_hash)
```

##### `register_partition_key(partition_key, datatype, **kwargs)`

Explicitly register a new partition key with validation.

**Example:**
```python
# Register new partition types
cache.register_partition_key("region_id", "integer")
cache.register_partition_key("timestamp_bucket", "timestamp")
cache.register_partition_key("price_range", "float")

# Use with different partitions
cache.set_cache("expensive_items", {99.99, 199.99}, partition_key="price_range")
```

## Query Processing Utilities

### `generate_all_hashes(query: str, partition_key: str, min_component_size=1, follow_graph=True, fix_attributes=True, canonicalize_queries=False)`

Generates query variants and their hashes for cache lookup.

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

### `normalize_query(query)`

Normalizes SQL query for consistent hashing.

**Example:**
```python
from partitioncache.query_processor import normalize_query

# Normalize for consistent comparison
normalized = normalize_query("SELECT   *   FROM users WHERE age>25")
# Returns: "SELECT * FROM users WHERE age > 25"
```

## Error Handling

### Exception Classes

#### `PartitionCacheError`

Base exception for PartitionCache-specific errors.

#### `ValidationError`

Raised for datatype or configuration validation errors.

#### `BackendError`

Raised for cache backend-specific errors.

### Error Handling Patterns

```python
import partitioncache
from partitioncache.exceptions import PartitionCacheError, ValidationError

try:
    # Cache operations
    cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")
    result = cache.get("query_hash")
    
except ValidationError as e:
    print(f"Configuration error: {e}")
    # Handle validation issues
    
except BackendError as e:
    print(f"Backend error: {e}")
    # Handle cache backend issues
    
except PartitionCacheError as e:
    print(f"General PartitionCache error: {e}")
    # Handle other PartitionCache issues
    
except Exception as e:
    print(f"Unexpected error: {e}")
    # Handle unexpected issues
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

```python
# Direct handler creation with configuration
from partitioncache.cache_handler import get_cache_handler

cache_handler = get_cache_handler(
    cache_type="postgresql_array",
    db_name="custom_db",
    db_host="custom-db-server",
    db_user="custom_user",
    db_password="custom_password",
    db_port=5432,
    db_tableprefix="custom_cache_"
)
```

## Advanced Usage Patterns

### Multi-Partition Applications

```python
# Setup multiple partition strategies
cache = partitioncache.create_cache_helper("postgresql_array", "default", "integer")

# Register different partition types
cache.register_partition_key("city_id", "integer")
cache.register_partition_key("region_name", "text")  
cache.register_partition_key("time_bucket", "timestamp")

# Store data across partitions (recommended: use set_entry with query text)
from partitioncache.query_processor import hash_query

# Best practice: store with query metadata
spatial_query = "SELECT * FROM locations WHERE city_id IN (1,5,10)"
cache.set_entry(hash_query(spatial_query), {1, 5, 10}, spatial_query, partition_key="city_id")

temporal_query = "SELECT * FROM events WHERE time_bucket IN ('2024-01','2024-02')"  
cache.set_entry(hash_query(temporal_query), {"2024-01", "2024-02"}, temporal_query, partition_key="time_bucket")

regional_query = "SELECT * FROM regions WHERE region_name IN ('north','south')"
cache.set_entry(hash_query(regional_query), {"north", "south"}, regional_query, partition_key="region_name")

# Query across partitions
for partition_key, datatype in cache.get_partition_keys():
    keys = cache.get_all_keys(partition_key)
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
        # Try primary cache first
        result = primary_cache.get(query_hash, partition_key="city_id")
        if result:
            return result, "city_level"
            
        # Fallback to region level
        result = fallback_cache.get(query_hash, partition_key="region_id")
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
def batch_cache_population(queries_and_results, cache, partition_key):
    """Efficiently populate cache with multiple queries."""
    
    success_count = 0
    for query, partition_keys in queries_and_results:
        # Generate hash for query
        from partitioncache.query_processor import hash_query
        query_hash = hash_query(query)
        
        # Store in cache
        if cache.set_cache(query_hash, partition_keys, partition_key):
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

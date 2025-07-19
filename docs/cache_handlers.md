# Cache Handlers

## Overview

PartitionCache supports multiple cache backends with different performance characteristics and datatype support. The system provides a unified interface across all backends while allowing each to optimize for specific use cases.

## Supported Cache Backends

### PostgreSQL Backends

#### PostgreSQL Array Handler
- **Type**: `postgresql_array`
- **Storage**: Native PostgreSQL arrays 
- **Datatypes**: `integer`, `float`, `text`, `timestamp`
- **Best for**: Mixed datatypes, complex queries, full SQL features
- **Memory**: Moderate efficiency
- **Scalability**: Excellent (database-native)

#### PostgreSQL Bit Handler  
- **Type**: `postgresql_bit`
- **Storage**: Fixed-length bit arrays (PostgreSQL BIT(n) columns)
- **Datatypes**: `integer` only
- **Configuration**: Requires mandatory `PG_BIT_CACHE_BITSIZE` environment variable
- **Best for**: Large integer datasets, memory efficiency if all integers are used as partition keys
- **Memory**: Highly efficient for integers
- **Scalability**: Excellent (database-native)

#### PostgreSQL Roaring Bitmap Handler
- **Type**: `postgresql_roaringbit`
- **Storage**: Roaring bitmaps (compressed bit arrays)
- **Datatypes**: `integer` only
- **Best for**: Sparse integer datasets, maximum compression
- **Memory**: Extremely efficient for sparse data
- **Scalability**: Excellent (database-native)

### Redis Backends

#### Redis Set Handler
- **Type**: `redis_set`  
- **Storage**: Redis sets
- **Datatypes**: `integer`, `text`
- **Best for**: Distributed caching, high throughput
- **Memory**: Good efficiency
- **Scalability**: Excellent (network-distributed)

#### Redis Bit Handler
- **Type**: `redis_bit`
- **Storage**: Redis bit arrays
- **Datatypes**: `integer` only  
- **Best for**: Large integer datasets in distributed setup
- **Memory**: Efficient for integers
- **Scalability**: Excellent (network-distributed)

### RocksDB Backends

#### RocksDB Set Handler
- **Type**: `rocksdb_set`
- **Storage**: RocksDB key-value store
- **Datatypes**: `integer`, `text`
- **Best for**: File-based caching, development, embedded use
- **Memory**: Disk-based with cache
- **Scalability**: Good (single-instance)

#### RocksDB Bit Handler
- **Type**: `rocksdb_bit`
- **Storage**: RocksDB with bit arrays
- **Datatypes**: `integer` only
- **Best for**: Large integer datasets without database server
- **Memory**: Highly efficient for integers
- **Scalability**: Good (single-instance)

#### RocksDict Handler
- **Type**: `rocksdict`
- **Storage**: RocksDB with rich datatypes
- **Datatypes**: `integer`, `float`, `text`, `timestamp`
- **Best for**: Development, flexible storage, embedded applications
- **Memory**: Disk-based with cache
- **Scalability**: Good (single-instance)

## Multi-Partition Support

### Architecture

All cache handlers support multiple partition keys with independent datatypes:

**PostgreSQL Handlers:**
- Separate tables per partition: `{table_name}_cache_{partition_key}`
- Metadata table: `{table_name}_partition_metadata` 
- Shared queries table: `{table_name}_queries`
- Queue integration: Dual-strategy operations for optimal performance

**Redis/RocksDB Handlers:**
- Namespaced keys: `cache:{partition_key}:{key}`
- Metadata keys: `_partition_metadata:{partition_key}`

### Queue Integration

**PostgreSQL Queue Handler Features:**
- **Regular Functions**: Use `INSERT ... ON CONFLICT DO NOTHING` for maximum performance
- **Priority Functions**: Non-blocking priority increment with `FOR UPDATE NOWAIT`
- **Automatic Deployment**: Non-blocking functions deployed during handler initialization
- **Graceful Fallback**: System works without functions, just with potential blocking

### Usage Examples

```python
import partitioncache

# Create handler
cache = partitioncache.create_cache_helper("postgresql_array", "default", "integer")

# Multiple partition keys with different datatypes
cache.set_cache("spatial_query", {1, 5, 10}, partition_key="city_id")
cache.set_cache("temporal_query", {2, 7, 12}, partition_key="time_bucket") 
cache.set_cache("text_query", {"NYC", "LA"}, partition_key="city_names")

# Retrieve data for specific partition
results = cache.get("spatial_query", partition_key="city_id")

# List all partitions
partitions = cache.get_partition_keys()
# Returns: [("city_id", "integer"), ("time_bucket", "integer"), ("city_names", "text")]
```

### Datatype Validation

The system automatically validates datatypes and prevents conflicts:

```python
cache.set_cache("key1", {1, 2, 3}, partition_key="test")
# Partition "test" is now locked to "integer" datatype

cache.set_cache("key2", {"a", "b"}, partition_key="test")  
# Raises: ValueError: Partition key 'test' already exists with datatype 'integer'
```

## Performance Characteristics

### Backend Selection Matrix

| Use Case | Volume | Datatypes | Recommended Backend | Notes |
|----------|--------|-----------|-------------------|-------|
| High-volume integers | Large | integer only | postgresql_roaringbit, postgresql_bit, redis_bit | Bit arrays most efficient |
| Mixed datatypes | Any | multiple | postgresql_array, rocksdict | Full datatype support |
| Distributed system | Any | integer, text | redis_set, redis_bit | Network-accessible |
| Development/testing | Any | any | rocksdict | No server setup required |
| Time-series data | Large | timestamp | postgresql_array | Native timestamp support |
| Geographic data | Large | float | postgresql_array | PostGIS support |

### Memory Efficiency Comparison

For 1 million integer partition keys:

TODO: Numbers not updated yet, needs to be updated for current implementation
| Backend | Memory Usage | Storage Format |
|---------|-------------|----------------|
| postgresql_bit | ~100 KB | Bit array |
| postgresql_roaringbit | ~? KB | Roaring bitmap |
| redis_bit | ~1 MB | Bit array |
| rocksdb_bit | ~125 KB | Bit array |
| postgresql_array | ~ 300 KB | Integer array |
| redis_set | ~8 MB | Set of strings |
| rocksdb_set | ~200 KB | Key-value pairs |
| rocksdict | ~200 KB | Serialized data |

## API Reference

### Core Methods

All cache handlers provide these methods:

```python
# Storage operations
set_entry(key: str, partition_key_identifiers: set, query_text: str, partition_key: str = "partition_key", force_update: bool = False) -> bool
set_cache(key: str, partition_key_identifiers: set, partition_key: str = "partition_key") -> bool
set_null(key: str, partition_key: str = "partition_key") -> bool
set_query(key: str, querytext: str, partition_key: str = "partition_key") -> bool

# Retrieval operations  
get(key: str, partition_key: str = "partition_key") -> set | None
exists(key: str, partition_key: str = "partition_key") -> bool
is_null(key: str, partition_key: str = "partition_key") -> bool

# Management operations
delete(key: str, partition_key: str = "partition_key") -> bool
get_all_keys(partition_key: str = "partition_key") -> list[str]
get_partition_keys() -> list[tuple[str, str]]
filter_existing_keys(keys: set, partition_key: str = "partition_key") -> set
get_datatype(partition_key: str) -> str | None
register_partition_key(partition_key: str, datatype: str, **kwargs) -> None

# Advanced operations
get_intersected(keys: set[str], partition_key: str = "partition_key") -> tuple[set, int]
get_intersected_lazy(keys: set[str], partition_key: str = "partition_key") -> tuple[str | None, int]
close() -> None
```

### Recommended Cache Population Method

#### `set_entry()` - The Preferred API

The `set_entry()` method is the **recommended way** to populate cache handlers as it atomically stores both cache data and query metadata in a single operation:

```python
import partitioncache
from partitioncache.query_processor import hash_query

# Create cache handler
cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")

# Best practice: Use set_entry() for complete cache population
query = "SELECT * FROM orders WHERE status = 'pending' AND amount > 100"
query_hash = hash_query(query)
user_ids = {1, 5, 10, 15, 20}  # Users matching the query

# Single atomic operation stores both cache data and query metadata
success = cache.set_entry(
    key=query_hash,
    partition_key_identifiers=user_ids,
    query_text=query
)

if success:
    print("✅ Cache entry stored successfully")
    
    # Later retrieval includes both data and metadata
    cached_users = cache.get(query_hash)         # Returns: {1, 5, 10, 15, 20}
    cached_query = cache.get_query(query_hash)   # Returns: "SELECT * FROM orders..."
    
    print(f"Cached {len(cached_users)} user IDs")
    print(f"Original query: {cached_query}")
```

#### Benefits of `set_entry()`

1. **Atomic Operation**: Ensures cache data and query metadata are stored consistently
2. **Full Context**: Associates queries with their cache results for debugging and analysis  
3. **Reliability**: Single transaction reduces the chance of partial writes
4. **Performance**: More efficient than separate `set_cache()` + `set_query()` calls
5. **Metadata Tracking**: Enables rich query analysis and cache monitoring

#### Alternative Lower-Level Methods

For advanced use cases, you can use separate operations:

```python
# Lower-level approach (not recommended for most cases)
cache.set_cache("query_hash", {1, 2, 3}, partition_key="user_id")
cache.set_query("query_hash", "SELECT * FROM users...", partition_key="user_id")

# set_entry() is equivalent to the above but atomic and safer
cache.set_entry("query_hash", {1, 2, 3}, "SELECT * FROM users...", partition_key="user_id")
```

### Enhanced Error Handling

All methods return boolean values for success/failure:

```python
# Check operation success
if not cache.set_cache("key1", {1, 2, 3}, partition_key="cities"):
    logger.error("Failed to store data in cache")
    
# Robust deletion
if cache.delete("key1", partition_key="cities"):
    logger.info("Successfully deleted key1")
else:
    logger.warning("Key1 deletion failed or key didn't exist")
```

## Backend-Specific Features

### PostgreSQL Bit Handler

**Configuration Requirements:**
The PostgreSQL bit handler requires a mandatory bitsize configuration:

```bash
# Required environment variable
export PG_BIT_CACHE_BITSIZE=10000  # Must be set
```

**Fixed-Length Bit Arrays:**
All cache tables use fixed-length `BIT(n)` columns for consistent BIT_AND operations:

```python
# Create partition with fixed bit size (determined by configuration)
cache._create_partition_table("large_dataset", bitsize=50000)
cache._create_partition_table("small_dataset", bitsize=1000)

# Get bit size for partition
bitsize = cache._get_partition_bitsize("large_dataset")  
# Returns: 50000 (integer value, never None)
```

**Automatic Schema Updates:**
Database triggers automatically update cache table schemas when bitsize changes:

```python
# Changing bitsize triggers schema update
cache._set_partition_bitsize("large_dataset", 75000)
# Automatically runs: ALTER TABLE cache_large_dataset ALTER COLUMN partition_keys TYPE BIT(75000)
```

### Variable Bit Sizes

Different partition keys can have different bit array sizes:

```python
cache = partitioncache.create_cache_helper("postgresql_bit", "default", "integer")

# Create partitions with different bit sizes
cache._create_partition_table("small_partition", bitsize=1000)
cache._create_partition_table("large_partition", bitsize=10000)

# Store data respecting bit size limits
cache.set_cache("query1", {1, 100, 999}, partition_key="small_partition")  # OK
cache.set_cache("query2", {1, 1000, 49999}, partition_key="large_partition")  # OK
```

### PostgreSQL Roaring Bitmap Handler

The roaring bitmap handler provides exceptional compression for sparse integer datasets:

```python
cache = partitioncache.create_cache_helper("postgresql_roaringbit", "default", "integer")

from pyroaring import BitMap
from bitarray import bitarray

# Multiple input formats supported:
# 1. Set of integers
cache.set_cache("query1", {1, 1000, 10000, 100000}, "partition")

# 2. List of integers  
cache.set_cache("query2", [1, 5, 10, 50], "partition")

# 3. BitMap directly
rb = BitMap([1, 2, 3, 1000])
cache.set_cache("query3", rb, "partition")

# 4. Bitarray conversion
ba = bitarray(1000)
ba.setall(0)
ba[1] = ba[10] = ba[100] = 1
cache.set_cache("query4", ba, "partition")

# Returns BitMap objects
result = cache.get("query1", "partition")
if result:
    print(f"Cardinality: {len(result)}")
    print(f"Values: {set(result)}")
    
# Efficient intersection operations
intersection, count = cache.get_intersected({"query1", "query2"}, "partition")
print(f"Intersection: {set(intersection)} from {count} keys")
```

#### Roaring Bitmap Advantages

- **Compression**: Extremely efficient for sparse datasets
- **No Size Limits**: No predefined bitsize required (unlike bit handler)
- **Fast Operations**: Optimized intersection and union operations
- **Memory Efficient**: Adaptive compression based on data density


## Best Practices

### Partition Key Strategy
- Use descriptive names: `"city_id"`, `"user_region"`, `"time_bucket"`
- Avoid conflicts with metadata prefixes
- Keep names consistent across your application

### Datatype Selection  
- Choose appropriate datatypes for your data
- Consider storage efficiency (bit arrays for large integer sets)
- Plan for future datatype needs

### Performance Optimization
- Separate hot/cold data into different partitions
- Use partition keys that align with query patterns
- Use PostgreSQL partitioning or multi-column indexes with the partition key to optimize query performance
- Monitor partition sizes and redistribute if needed

### Backend Selection
- Use PostgreSQL backends for transactional consistency
- Use Redis backends for distributed caching
- Use RocksDB backends for development and embedded use

## Troubleshooting

### Common Issues

**Datatype Mismatch Error**
```
ValueError: Partition key 'test' already exists with datatype 'integer'
```
**Solution**: Use `get_partition_keys()` to check existing datatypes

**Bit Size Overflow**
```
ValueError: Value {50000} is out of range for bitarray of size 10000
```
**Solution**: Increase partition bitsize or use different handler

**Missing Partition Key**
```python
cache.get("key", partition_key="nonexistent")  # Returns None
```
**Solution**: Check partition exists with `get_partition_keys()`

### Debug Commands

```python
# List all partitions and datatypes
partitions = cache.get_partition_keys()
print(f"Available partitions: {partitions}")

# Check partition-specific data
keys = cache.get_all_keys("my_partition")
print(f"Keys in 'my_partition': {keys}")

# For PostgreSQL bit handler - check bitsize
if hasattr(cache, '_get_partition_bitsize'):
    bitsize = cache._get_partition_bitsize("my_partition")
    print(f"Bitsize for 'my_partition': {bitsize}")
```

## Implementation Details

### Database Schema (PostgreSQL)

**Metadata Table:**
```sql
CREATE TABLE cache_partition_metadata (
    partition_key TEXT PRIMARY KEY,
    datatype TEXT NOT NULL,
    bitsize INTEGER NOT NULL,  -- Required for bit handler
    created_at TIMESTAMP DEFAULT now()
);
```

**Cache Tables (Array Handler):**
```sql
CREATE TABLE cache_{partition_key} (
    query_hash TEXT PRIMARY KEY,
    value INTEGER[]  -- or FLOAT[], TEXT[], TIMESTAMP[]
);
```

**Cache Tables (Bit Handler):**
```sql
CREATE TABLE cache_{partition_key} (
    query_hash TEXT PRIMARY KEY,
    partition_keys BIT({bitsize}),  -- Fixed-length bit array
    partition_keys_count INTEGER GENERATED ALWAYS AS (length(replace(partition_keys::text, '0', ''))) STORED
);
```

**Cache Tables (Roaring Bitmap Handler):**
```sql
CREATE TABLE cache_{partition_key} (
    query_hash TEXT PRIMARY KEY,
    partition_keys roaringbitmap,
    partition_keys_count INTEGER NOT NULL GENERATED ALWAYS AS (rb_cardinality(partition_keys)) STORED
);
```

### Thread Safety

All handlers maintain thread safety for:
- Metadata operations
- Partition creation
- Datatype validation
- Cross-partition isolation 

### Connection Management

Cache handlers automatically manage database connections:
- Connection pooling for PostgreSQL backends
- Redis connection management with reconnection logic
- RocksDB file handle optimization
- Graceful cleanup on handler close 
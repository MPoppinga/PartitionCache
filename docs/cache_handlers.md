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
- **Storage**: Bit arrays (PostgreSQL bit strings)
- **Datatypes**: `integer` only
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

**Redis/RocksDB Handlers:**
- Namespaced keys: `cache:{partition_key}:{key}`
- Metadata keys: `_partition_metadata:{partition_key}`

### Usage Examples

```python
import partitioncache

# Create handler
cache = partitioncache.create_cache_helper("postgresql_array", "default", "integer")

# Multiple partition keys with different datatypes
cache.set_set("spatial_query", {1, 5, 10}, partition_key="city_id")
cache.set_set("temporal_query", {2, 7, 12}, partition_key="time_bucket") 
cache.set_set("text_query", {"NYC", "LA"}, partition_key="city_names")

# Retrieve data for specific partition
results = cache.get("spatial_query", partition_key="city_id")

# List all partitions
partitions = cache.get_partition_keys()
# Returns: [("city_id", "integer"), ("time_bucket", "integer"), ("city_names", "text")]
```

### Datatype Validation

The system automatically validates datatypes and prevents conflicts:

```python
cache.set_set("key1", {1, 2, 3}, partition_key="test")
# Partition "test" is now locked to "integer" datatype

cache.set_set("key2", {"a", "b"}, partition_key="test")  
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
set_set(key: str, value: set, partition_key: str = "partition_key") -> bool
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

### Enhanced Error Handling

All methods return boolean values for success/failure:

```python
# Check operation success
if not cache.set_set("key1", {1, 2, 3}, partition_key="cities"):
    logger.error("Failed to store data in cache")
    
# Robust deletion
if cache.delete("key1", partition_key="cities"):
    logger.info("Successfully deleted key1")
else:
    logger.warning("Key1 deletion failed or key didn't exist")
```

## Backend-Specific Features

### PostgreSQL Bit Handler

Additional methods for bit size management:

```python
# Create partition with custom bit size
cache._create_partition_table("large_dataset", bitsize=50000)
cache._create_partition_table("small_dataset", bitsize=1000)

# Get bit size for partition
bitsize = cache._get_partition_bitsize("large_dataset")  
# Returns: 50000
```

### Variable Bit Sizes

Different partition keys can have different bit array sizes:

```python
cache = partitioncache.create_cache_helper("postgresql_bit", "default", "integer")

# Create partitions with different bit sizes
cache._create_partition_table("small_partition", bitsize=1000)
cache._create_partition_table("large_partition", bitsize=10000)

# Store data respecting bit size limits
cache.set_set("query1", {1, 100, 999}, partition_key="small_partition")  # OK
cache.set_set("query2", {1, 1000, 49999}, partition_key="large_partition")  # OK
```

### PostgreSQL Roaring Bitmap Handler

The roaring bitmap handler provides exceptional compression for sparse integer datasets:

```python
cache = partitioncache.create_cache_helper("postgresql_roaringbit", "default", "integer")

from pyroaring import BitMap
from bitarray import bitarray

# Multiple input formats supported:
# 1. Set of integers
cache.set_set("query1", {1, 1000, 10000, 100000}, "partition")

# 2. List of integers  
cache.set_set("query2", [1, 5, 10, 50], "partition")

# 3. BitMap directly
rb = BitMap([1, 2, 3, 1000])
cache.set_set("query3", rb, "partition")

# 4. Bitarray conversion
ba = bitarray(1000)
ba.setall(0)
ba[1] = ba[10] = ba[100] = 1
cache.set_set("query4", ba, "partition")

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
    bitsize INTEGER,  -- Only for bit handler
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
    partition_keys BIT({bitsize}),
    partition_keys_count INTEGER NOT NULL GENERATED ALWAYS AS (length(replace(partition_keys::text, '0', ''))) STORED
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
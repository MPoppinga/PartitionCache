# PartitionCache System Overview

PartitionCache is a high-performance caching middleware for partition-based query optimization. The system provides multiple cache backends, queue-based asynchronous processing, and automatic partition key management to accelerate database queries through intelligent caching.

## System Architecture

PartitionCache follows a modular architecture with distinct layers for cache handling, queue management, and query processing:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PartitionCache System                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                        Streamlined API Layer                           │ │
│ │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │ │
│ │  │ create_cache()  │  │ list_cache_     │  │ get_partition_  │        │ │
│ │  │ - Auto-detect   │  │ types()         │  │ keys()          │        │ │
│ │  │ - Validation    │  │ - Backend info  │  │ - Query         │        │ │
│ │  │ - Error help    │  │ - Compatibility │  │ - Processing    │        │ │
│ │  └─────────────────┘  └─────────────────┘  └─────────────────┘        │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                         Cache Handler Layer                            │ │
│ │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │ │
│ │  │ PostgreSQL      │  │ Redis           │  │ RocksDB         │        │ │
│ │  │ - Array (all)   │  │ - Set (int/txt) │  │ - Set (int/txt) │        │ │
│ │  │ - Bit (int)     │  │ - Bit (int)     │  │ - Bit (int)     │        │ │
│ │  │                 │  │                 │  │ - Dict (all)    │        │ │
│ │  └─────────────────┘  └─────────────────┘  └─────────────────┘        │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                          Queue System                                  │ │
│ │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │ │
│ │  │ Incoming Queue  │──│ Fragment        │──│ Outgoing Queue  │        │ │
│ │  │ - Original SQL  │  │ Processor       │  │ - Exec Ready    │        │ │
│ │  │ - Partition Key │  │ - Breakdown     │  │ - Hash + Key    │        │ │
│ │  │ - Priority*     │  │ - Validation    │  │ - Priority*     │        │ │
│ │  └─────────────────┘  └─────────────────┘  └─────────────────┘        │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                        Processing Layer                                │ │
│ │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │ │
│ │  │ Monitor         │  │ PostgreSQL Queue Processor       │  │ Query           │        │ │
│ │  │ (Python)        │  │ - Database      │  │ - Apply Cache   │        │ │
│ │  │ - External      │  │ - Auto Recovery │  │ - Rewrite       │        │ │
│ │  │ - Flexible      │  │                 │  │ - Optimize      │        │ │
│ │  └─────────────────┘  └─────────────────┘  └─────────────────┘        │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                          Database Layer                                │ │
│ │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │ │
│ │  │ PostgreSQL      │  │ MySQL           │  │ SQLite          │        │ │
│ │  │ - Full support  │  │ - Standard SQL  │  │ - Development   │        │ │
│ │  │ - Native types  │  │ - Compatibility │  │ - Testing       │        │ │
│ │  │ - Triggers      │  │                 │  │                 │        │ │
│ │  └─────────────────┘  └─────────────────┘  └─────────────────┘        │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                  (* PostgreSQL only)
```

## Key Components

### Streamlined API

The streamlined API provides a user-friendly interface with automatic configuration and validation:

```python
import partitioncache

# Simple cache creation with automatic validation
cache = partitioncache.create_cache("postgresql_array", "user_id", "integer")

# Store and retrieve partition keys
cache.set_set("query_hash", {1, 2, 3, 4, 5})
results = cache.get("query_hash")  # Returns: {1, 2, 3, 4, 5}

# List available backends
backends = partitioncache.list_cache_types()
print(backends)  # Shows compatibility matrix
```

**Key Features:**
- **Automatic Backend Selection**: Recommendations based on use case
- **Validation and Error Messages**: Helpful guidance for configuration issues
- **Datatype Compatibility**: Automatic checking and suggestions
- **Simplified Configuration**: Minimal setup required

### Cache Handlers

Multiple cache backends supporting different datatypes and performance characteristics:

#### PostgreSQL Backends
- **Array Handler**: Supports all datatypes (integer, float, text, timestamp)
- **Bit Handler**: Integer-only with highly efficient bit arrays

#### Redis Backends  
- **Set Handler**: Integer and text support with distributed caching
- **Bit Handler**: Integer-only with memory-efficient bit arrays

#### RocksDB Backends
- **Set Handler**: Integer and text with file-based storage
- **Bit Handler**: Integer-only with local file storage  
- **Dict Handler**: All datatypes with rich serialization

### Multi-Partition Support

All cache handlers support multiple independent partition keys:

```python
# Different partition keys with different datatypes
cache.set_set("spatial_query", {1, 5, 10}, partition_key="city_id")
cache.set_set("temporal_query", {"2024-01", "2024-02"}, partition_key="time_bucket")
cache.set_set("user_query", {100, 200, 300}, partition_key="user_region")

# List all partitions
partitions = cache.get_partition_keys()
# Returns: [("city_id", "integer"), ("time_bucket", "text"), ("user_region", "integer")]
```

### Queue System

Sophisticated two-queue architecture for scalable processing:

**Incoming Queue**: Accepts original SQL queries with partition keys
**Outgoing Queue**: Contains processed query fragments ready for execution

**Providers:**
- **PostgreSQL**: Priority processing, durable storage, LISTEN/NOTIFY
- **Redis**: High throughput, memory-based, distributed

### Processing Models

#### Monitor Processing (Traditional)
External Python observer scripts that poll queues and process items:

```bash
# Run external monitor
python -m partitioncache.cli.monitor_cache_queue \
    --db-backend postgresql \
    --cache-backend postgresql_array \
    --db-name mydb
```

#### PostgreSQL Queue Processor
Database-native processing using PostgreSQL's pg_cron:

```sql
-- Automatic processing every second
SELECT partitioncache_process_queue('partitioncache');
```

**Benefits:**
- No external process management
- Built-in error recovery and monitoring
- Superior performance and reliability

### Query Processing

Advanced query rewriting and optimization:

```python
# Get partition keys for complex queries
partition_keys, num_subqueries, cache_hits = partitioncache.get_partition_keys(
    query="SELECT * FROM events WHERE type='click' AND user_id IN (1,2,3)",
    cache_handler=cache.underlying_handler,
    partition_key="user_id"
)

# Apply cached results to optimize queries
optimized_query = partitioncache.extend_query_with_partition_keys(
    query="SELECT * FROM events WHERE type='click'",
    partition_keys=partition_keys,
    partition_key="user_id",
    method="IN"
)
```

## Datatype Support Matrix

| Backend | Integer | Float | Text | Timestamp | Memory Efficiency | Scalability |
|---------|---------|-------|------|-----------|------------------|-------------|
| **postgresql_array** | ✅ | ✅ | ✅ | ✅ | Moderate | Excellent |
| **postgresql_bit** | ✅ | ❌ | ❌ | ❌ | High | Excellent |
| **redis_set** | ✅ | ❌ | ✅ | ❌ | Good | Excellent |
| **redis_bit** | ✅ | ❌ | ❌ | ❌ | High | Excellent |
| **rocksdb_set** | ✅ | ❌ | ✅ | ❌ | Good | Good |
| **rocksdb_bit** | ✅ | ❌ | ❌ | ❌ | High | Good |
| **rocksdict** | ✅ | ✅ | ✅ | ✅ | Good | Good |

## Configuration Management

### Environment Variables

PartitionCache automatically detects configuration from environment variables:

```bash
# Database configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=app_user
DB_PASSWORD=secure_password
DB_NAME=app_database

# Cache backend selection
CACHE_BACKEND=postgresql_array

# Cache table prefixes
PG_ARRAY_CACHE_TABLE_PREFIX=myapp_cache
PG_BIT_CACHE_TABLE_PREFIX=myapp_bit_cache

# Queue configuration
QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5432
PG_QUEUE_USER=queue_user
PG_QUEUE_PASSWORD=queue_password
PG_QUEUE_DB=queue_database
PG_QUEUE_TABLE_PREFIX=myapp_queue
```

### Automatic Detection

The system automatically determines optimal configurations:

```python
import os

# Automatic table prefix detection
def get_table_prefix_from_env() -> str:
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
    
    if cache_backend == "postgresql_array":
        prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "partitioncache")
        return f"{prefix}_"
    elif cache_backend == "postgresql_bit":
        prefix = os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "partitioncache")
        return f"{prefix}_"
    
    return "partitioncache_"  # fallback
```

## Performance Characteristics

### Memory Efficiency (1M integer partition keys)

| Backend | Memory Usage | Storage Format | Best Use Case |
|---------|-------------|----------------|---------------|
| postgresql_bit | ~125 KB | Bit array | Large integer datasets |
| redis_bit | ~125 KB | Bit array | Distributed integer datasets |
| rocksdb_bit | ~125 KB | Bit array | Local integer datasets |
| postgresql_array | ~4 MB | Integer array | Mixed datatypes |
| redis_set | ~8 MB | Set strings | High-throughput text/int |
| rocksdb_set | ~6 MB | Key-value | Development/embedded |
| rocksdict | ~4 MB | Serialized | Development all types |

### Throughput Comparison

| Provider | Queries/sec | Latency | Concurrency | Durability |
|----------|-------------|---------|-------------|------------|
| **PostgreSQL Direct** | 1000-5000 | Low | High | Excellent |
| **PostgreSQL Monitor** | 500-2000 | Medium | Medium | Excellent |
| **Redis Queue** | 5000-20000 | Very Low | Very High | None |

## CLI Tools

Comprehensive command-line interface for all operations:

### Cache Management
```bash
# Add queries to cache
pcache-add --direct \
  --query "SELECT DISTINCT user_id FROM events" \
  --partition-key "user_id"

# Add to queue for processing
pcache-add --queue \
  --query "SELECT DISTINCT user_id FROM events" \
  --partition-key "user_id"

# Read from cache
pcache-read --cache-backend postgresql_array \
  --partition-key user_id \
  --query-hash "abc123"

# Manage cache operations
pcache-manage --count-cache --partition-key user_id
pcache-manage --clear-cache --partition-key user_id
```

### Queue Operations
```bash
# Monitor queue processing
python -m partitioncache.cli.monitor_cache_queue \
  --db-backend postgresql \
  --cache-backend postgresql_array

# Queue management  
pcache-manage --count-queue
pcache-manage --clear-queue
```

### PostgreSQL Queue Processor
```bash
# Setup PostgreSQL queue processor
python -m partitioncache.cli.setup_postgresql_queue_processor \
  --frequency 1 \
  --max-parallel-jobs 3 \
  enable

# Monitor processor status
python -m partitioncache.cli.setup_postgresql_queue_processor status
python -m partitioncache.cli.setup_postgresql_queue_processor logs
```

## Use Cases and Recommendations

### High-Volume Integer Partitions
**Scenario**: Million+ partition keys, integer-only data
**Recommended**: `postgresql_bit` or `redis_bit`
**Benefits**: ~99% memory reduction compared to other backends

```python
cache = partitioncache.create_cache("postgresql_bit", "postal_code", "integer")
cache.set_set("restaurant_query", {10001, 10002, 10003, 10004})
```

### Mixed Datatype Applications
**Scenario**: Different partition keys need different datatypes
**Recommended**: `postgresql_array` or `rocksdict`
**Benefits**: Full datatype flexibility

```python
cache = partitioncache.create_cache("postgresql_array", "default", "integer")
cache.set_set("cities", {1, 2, 3}, partition_key="city_id")
cache.set_set("regions", {"north", "south"}, partition_key="region_name")
```

### Distributed Systems
**Scenario**: Multiple application instances need shared cache
**Recommended**: `redis_set` or `redis_bit`
**Benefits**: Network-accessible, high concurrency

```python
cache = partitioncache.create_cache("redis_set", "session_id", "text",
                                   host="redis-cluster.example.com")
```

### Development and Testing
**Scenario**: Local development without server setup
**Recommended**: `rocksdict`
**Benefits**: No external dependencies, full feature support

```python
cache = partitioncache.create_cache("rocksdict", "test_partition", "integer")
```

### Time-Series Data
**Scenario**: Timestamp-based partitioning
**Recommended**: `postgresql_array`
**Benefits**: Native timestamp support and operations

```python
from datetime import datetime
cache = partitioncache.create_cache("postgresql_array", "time_bucket", "timestamp")
cache.set_set("events", {datetime(2024, 1, 1), datetime(2024, 1, 2)})
```

## Security Considerations

### Database Security
- **Minimum Permissions**: Grant only required database permissions
- **Connection Security**: Use SSL/TLS for database connections
- **Access Control**: Restrict access to cache management functions

### Query Security
- **Parameterized Queries**: All SQL operations use parameter binding
- **Input Validation**: Comprehensive validation of user inputs
- **SQL Injection Prevention**: Built-in protection mechanisms

### Network Security
- **Redis Authentication**: Support for Redis AUTH
- **Network Isolation**: Recommend VPC/private networks
- **Encryption**: Support for encrypted connections

## Migration Paths

### From Legacy Systems
1. **Assessment**: Identify current partition strategies
2. **Backend Selection**: Choose optimal cache backend
3. **Gradual Migration**: Migrate partition by partition
4. **Validation**: Verify cache accuracy and performance

### Between Backends
```python
# Migration utility function
def migrate_cache_backend(old_cache, new_cache, partition_key):
    keys = old_cache.get_all_keys(partition_key)
    for key in keys:
        data = old_cache.get(key, partition_key)
        query = old_cache.get_query(key, partition_key)
        
        new_cache.set_set(key, data, partition_key=partition_key)
        if query:
            new_cache.set_query(key, query, partition_key=partition_key)
```

### Queue Provider Migration
1. **Drain Current Queues**: Process all pending items
2. **Update Configuration**: Change environment variables
3. **Restart Services**: Monitor processes and PostgreSQL queue processor
4. **Verify Operations**: Test queue functionality

## Monitoring and Observability

### Built-in Metrics
- **Cache Hit Rates**: Track query cache effectiveness
- **Queue Depths**: Monitor processing backlogs
- **Processing Times**: Measure query execution performance
- **Error Rates**: Track failures and timeouts

### Log Analysis
```sql
-- PostgreSQL queue processor performance
SELECT 
    AVG(execution_time_ms) as avg_time,
    MAX(execution_time_ms) as max_time,
    COUNT(*) as total_jobs
FROM partitioncache_processor_log 
WHERE status = 'success'
AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour';

-- Error analysis
SELECT error_msg, COUNT(*) as frequency
FROM partitioncache_processor_log 
WHERE status = 'failed'
GROUP BY error_msg
ORDER BY frequency DESC;
```

### Custom Monitoring
```python
import partitioncache

# Monitor cache sizes
def monitor_cache_metrics(cache, partition_key):
    keys = cache.get_all_keys(partition_key)
    total_size = sum(len(cache.get(key, partition_key) or set()) for key in keys)
    
    return {
        'partition_key': partition_key,
        'total_keys': len(keys),
        'total_partition_values': total_size,
        'avg_partition_size': total_size / len(keys) if keys else 0
    }

# Monitor queue health
def monitor_queue_health():
    incoming, outgoing = partitioncache.get_queue_lengths()
    return {
        'incoming_depth': incoming,
        'outgoing_depth': outgoing,
        'processing_backlog': incoming + outgoing
    }
```

## Best Practices

### Performance Optimization
1. **Backend Selection**: Choose appropriate backend for data characteristics
2. **Partition Strategy**: Align partition keys with query patterns
3. **Index Optimization**: Ensure database indexes support cache operations
4. **Connection Pooling**: Use connection pools for high-concurrency applications

### Operational Excellence
1. **Monitoring**: Implement comprehensive monitoring and alerting
2. **Backup Strategy**: Include cache metadata in backup procedures
3. **Capacity Planning**: Monitor growth and plan for scaling
4. **Documentation**: Maintain clear documentation of partition strategies

### Development Workflow
1. **Local Development**: Use RocksDB backends for local testing
2. **Staging Environment**: Mirror production cache configuration
3. **Testing**: Include cache functionality in automated tests
4. **Deployment**: Use infrastructure as code for consistent environments

This comprehensive system overview provides the foundation for understanding PartitionCache's architecture, capabilities, and optimal usage patterns for different scenarios. 
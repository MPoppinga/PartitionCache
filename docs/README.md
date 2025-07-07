# PartitionCache Documentation

PartitionCache is a high-performance caching middleware for partition-based query optimization. The system provides multiple cache backends, queue-based asynchronous processing, and automatic partition key management to accelerate database queries through intelligent caching.

## Quick Start

### Basic Cache Operations
```python
import partitioncache

# Create cache helper with automatic validation
cache = partitioncache.create_cache_helper("postgresql_array", "user_id", "integer")

# Store and retrieve partition keys with query metadata (recommended)
from partitioncache.query_processor import hash_query

query = "SELECT * FROM users WHERE status = 'active'"
query_hash = hash_query(query)
cache.set_entry(query_hash, {1, 2, 3, 4, 5}, query)  # Stores both data and query

results = cache.get(query_hash)        # Returns: {1, 2, 3, 4, 5}
original_query = cache.get_query(query_hash)  # Returns: "SELECT * FROM users WHERE status = 'active'"
```

### Production Workflow: Queue-Based Population & Query Optimization
```python
import partitioncache

# 1. Send queries to queue for asynchronous cache population
partitioncache.push_to_original_query_queue(
    query="SELECT DISTINCT user_id FROM events WHERE type='purchase' AND region='US'",
    partition_key="user_id"
)
# Queue processor will execute query and cache results

# 2. Later, optimize queries using the populated cache
optimized_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM events WHERE type='purchase' AND amount > 100",
    cache_handler=partitioncache.get_cache_handler("postgresql_array"),
    partition_key="user_id",
    method="IN_SUBQUERY"  # or "TMP_TABLE_JOIN" for large result sets
)

print(f"Cache hits: {stats['cache_hits']}/{stats['generated_variants']} variants")
# Execute optimized_query - it now includes: AND user_id IN (cached_partition_keys)
```

## System Architecture

PartitionCache follows a modular architecture with distinct layers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PartitionCache System                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                               API Layer                               │ │
│ │  create_cache_helper() → list_cache_types() → get_partition_keys()    │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                         Cache Handler Layer                            │ │
│ │    PostgreSQL (Array/Bit/RoaringBit) → Redis (Set/Bit) → RocksDB (Set/Bit/Dict)  │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                          Queue System                                  │ │
│ │    Original Query Queue → Fragment Processor → Fragment Queue         │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                        Cache Population Layer                           │ │
│ │   Monitor (Python) / PostgreSQL Queue Processor → Partition Keys    │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Documentation Index

### Core Documentation
- **[API Reference](api_reference.md)** - Detailed Python API documentation
- **[CLI Reference](cli_reference.md)** - Command-line tools documentation
- **[Architecture Diagrams](architecture_diagrams.md)** - Visual system representations

### Backend Selection
- **[Cache Handlers](cache_handlers.md)** - Backend comparison and selection guide
- **[Datatype Support](datatype_support.md)** - Datatype compatibility matrix

### Operations
- **[Queue System](queue_system.md)** - Two-queue architecture and providers
- **[PostgreSQL Queue Processor](postgresql_queue_processor.md)** - pg_cron integration
- **[Cache Optimization](cache_optimization.md)** - Cache-aware query optimization in pcache-monitor
- **[Cache Eviction](cache_eviction.md)** - Automatic cleanup strategies
- **[Production Deployment](production_deployment.md)** - Configuration and best practices

### Development
- **[Integration Test Guide](integration_test_guide.md)** - Testing documentation

### Examples & Tutorials
- **[Complete Workflow](complete_workflow_example.md)** - End-to-end tutorial
- **[Cache Management CLI](manage_cache_cli.md)** - pcache-manage usage guide
- **[Migration Guide](migration_guide.md)** - Backend migration procedures

## Key Concepts

### Cache Handlers
PartitionCache supports multiple cache backends optimized for different use cases:
- **PostgreSQL**: Array (all datatypes), Bit (integers), RoaringBit (integers)
- **Redis**: Set (integer/text), Bit (integers)  
- **RocksDB**: Set (integer/text), Bit (integers), Dict (all datatypes)

### Query Processing
PartitionCache uses sophisticated query decomposition to maximize cache effectiveness through:
1. Query analysis and AND condition identification
2. Variant generation for reusability
3. Parallel execution of query fragments
4. Set intersection optimization for future queries

### Datatype Support
| Backend | Integer | Float | Text | Timestamp |
|---------|---------|-------|------|-----------|
| postgresql_array | ✅ | ✅ | ✅ | ✅ |
| postgresql_bit | ✅ | ❌ | ❌ | ❌ |
| redis_set | ✅ | ❌ | ✅ | ❌ |
| rocksdb_set | ✅ | ❌ | ✅ | ❌ |
| rocksdict | ✅ | ✅ | ✅ | ✅ |

## Configuration

PartitionCache uses environment variables for configuration:

```bash
# Core settings
CACHE_BACKEND=postgresql_array
DB_HOST=localhost
DB_PORT=5432
DB_NAME=app_database

# Queue settings
QUERY_QUEUE_PROVIDER=postgresql
```

Look at the [.env.example](../.env.example) file for more details. See [Production Deployment Guide](production_deployment.md) for complete configuration options.

## CLI Tools

PartitionCache provides comprehensive command-line tools:

- **pcache-manage**: Central management tool for setup, status, and maintenance
- **pcache-add**: Add queries to cache (direct or via queue)
- **pcache-read**: Read cached partition keys
- **pcache-monitor**: Multi-threaded queue processor
- **pcache-postgresql-queue-processor**: Native PostgreSQL processing with pg_cron
- **pcache-postgresql-eviction-manager**: Automatic cache cleanup

See [CLI Reference](cli_reference.md) for detailed command documentation.

## Common Use Cases

### High-Volume Integer Partitions
For millions of integer partition keys, use `postgresql_bit` or `redis_bit` for memory reduction.

### Mixed Datatype Applications
When different partition keys need different datatypes, use `postgresql_array` or `rocksdict`.

### Development and Testing
For local development without external dependencies, use `rocksdict`.

## Getting Help

- For working examples, see the `examples/` directory in the project root
- Check the detailed documentation linked above for specific topics
- Review the [Complete Workflow Example](complete_workflow_example.md) for an end-to-end implementation guide
# Queue System

## Overview

PartitionCache implements a sophisticated two-queue system that separates query processing into distinct phases for improved flexibility, performance, and reliability. The system supports multiple queue providers with advanced features like priority processing and partition key management.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client        │    │  Fragment       │    │  Execution      │
│   Applications  │    │  Processor      │    │  Workers        │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │ Original  │  │───▶│  │ Query     │  │───▶│  │ Database  │  │
│  │ Queries + │  │    │  │ Breakdown │  │    │  │ Execution │  │
│  │ Part. Key │  │    │  │ + Part.   │  │    │  │ + Caching │  │
│  └───────────┘  │    │  │ Key       │  │    │  └───────────┘  │
└─────────────────┘    │  └───────────┘  │    └─────────────────┘
         │              └─────────────────┘              │
         ▼                       │                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Incoming Queue  │    │ Processing      │    │ Outgoing Queue  │
│ (PostgreSQL/    │    │ Thread          │    │ (PostgreSQL/    │
│  Redis)         │    │                 │    │  Redis)         │
│                 │    │ - Fragments     │    │                 │
│ - Original SQL  │    │ - Optimization  │    │ - Query Hash    │
│ - Partition Key │    │ - Validation    │    │ - Partition Key │
│ - Priority*     │    │ - Partition Key │    │ - Priority*     │
│ - User input    │    │   Management    │    │ - Ready to run  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                            * PostgreSQL only
```

## Queue Providers

### PostgreSQL Provider (Recommended)

**Features:**
- Priority-based processing with automatic increment for duplicates
- Partition key tracking in dedicated columns
- LISTEN/NOTIFY for real-time processing
- Comprehensive indexing for optimal performance

**Configuration:**
```bash
QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5432
PG_QUEUE_USER=queue_user
PG_QUEUE_PASSWORD=queue_password
PG_QUEUE_DB=partition_cache_queues
```

**Database Schema:**
```sql
-- Original Query Queue
CREATE TABLE original_query_queue (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(query, partition_key)
);

-- Query Fragment Queue  
CREATE TABLE query_fragment_queue (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    hash TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hash, partition_key)
);
```

### Redis Provider

**Features:**
- High-throughput FIFO processing
- Partition key support via JSON storage
- Network-distributed caching capability
- Lightweight for high-volume scenarios

**Configuration:**
```bash
QUERY_QUEUE_PROVIDER=redis
REDIS_HOST=localhost
REDIS_PORT=6379
QUERY_QUEUE_REDIS_DB=1
QUERY_QUEUE_REDIS_QUEUE_KEY=partition_cache_queue

# Optional Redis password
REDIS_PASSWORD=your_password
```

## Queue Operations

### Core Functions

#### `push_to_incoming_queue(query, partition_key="partition_key")`
Add original queries to the incoming queue:

```python
import partitioncache

# Add query with custom partition key
partitioncache.push_to_incoming_queue(
    query="SELECT * FROM users WHERE age > 25",
    partition_key="user_id"
)
```

#### `push_to_outgoing_queue(query_hash_pairs, partition_key="partition_key")`
Add processed query fragments to the outgoing queue:

```python
# Add query fragments with partition key
query_hash_pairs = [
    ("SELECT * FROM users WHERE age > 25 AND user_id = 1", "hash123"),
    ("SELECT * FROM users WHERE age > 25 AND user_id = 2", "hash456")
]
partitioncache.push_to_outgoing_queue(query_hash_pairs, partition_key="user_id")
```

#### `pop_from_incoming_queue()`
Retrieve original queries for processing:

```python
# Returns (query, partition_key) tuple or (None, None)
query, partition_key = partitioncache.pop_from_incoming_queue()
if query:
    print(f"Processing query for partition: {partition_key}")
```

#### `pop_from_outgoing_queue()`
Retrieve query fragments for execution:

```python
# Returns (query, hash, partition_key) tuple or (None, None, None)
query, hash_val, partition_key = partitioncache.pop_from_outgoing_queue()
if query:
    print(f"Executing query {hash_val} for partition: {partition_key}")
```

#### `get_queue_lengths()`
Monitor queue status:

```python
incoming_count, outgoing_count = partitioncache.get_queue_lengths()
print(f"Incoming: {incoming_count}, Outgoing: {outgoing_count}")
```

## Processing Architecture

### Monitor Cache Queue

The monitor implements a sophisticated two-threaded architecture:

**Thread 1: Fragment Processor**
- Consumes `(query, partition_key)` from incoming queue
- Uses partition key from queue (not command line)
- Breaks down queries using `generate_all_query_hash_pairs()`
- Pushes fragments with partition key to outgoing queue

**Thread 2: Execution Pool**
- Consumes `(query, hash, partition_key)` from outgoing queue
- Executes fragments against database
- Stores results in cache with partition key tracking

### Usage Example

```bash
# Monitor queue processing (partition key comes from queue)
pcache-monitor \
    --db-backend postgresql \
    --cache-backend postgresql_array \
    --db-name mydb
```

## Priority System (PostgreSQL Only)

### Automatic Priority Increment

The PostgreSQL provider automatically handles priority for duplicate queries:

```sql
-- First insertion
INSERT INTO original_query_queue (query, partition_key, priority)
VALUES ('SELECT * FROM table', 'key1', 1);

-- Duplicate insertion automatically increments priority
INSERT INTO original_query_queue (query, partition_key, priority)
VALUES ('SELECT * FROM table', 'key1', 1)
ON CONFLICT (query, partition_key)
DO UPDATE SET 
    priority = original_query_queue.priority + 1,
    updated_at = CURRENT_TIMESTAMP;
```

### Priority-Based Retrieval

Queries are processed in order of:
1. **Priority** (highest first)
2. **Creation time** (oldest first)

```sql
SELECT id, query, partition_key, priority
FROM original_query_queue
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

## CLI Tools

### Adding Queries to Queue

```bash
# Add to incoming queue with partition key
pcache-add \
    --query "SELECT * FROM users WHERE age > 25" \
    --queue \
    --partition-key "user_id"

# Generate fragments and add directly to outgoing queue
pcache-add \
    --query "SELECT * FROM complex_query" \
    --queue-fragments \
    --partition-key "custom_partition_key"
```

### Queue Management

```bash
# Show queue statistics
pcache-manage queue count

# Clear all queues
pcache-manage queue clear

# Clear specific queues
pcache-manage queue clear --original
pcache-manage queue clear --fragment
```

## Advanced Features

### Concurrency Control

The system uses PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` pattern for optimal concurrency:

```sql
-- Multiple processors can run simultaneously without blocking
SELECT id, query, hash, partition_key 
FROM query_fragment_queue 
ORDER BY priority DESC, created_at ASC 
FOR UPDATE SKIP LOCKED 
LIMIT 1;
```

**Benefits:**
- No blocking operations between processors
- Automatic load distribution
- Optimal resource utilization
- Natural failover handling

### LISTEN/NOTIFY Support

PostgreSQL provider includes real-time notifications:

```sql
-- Triggers sent when new items are added
CREATE TRIGGER trigger_notify_original_query_insert
    AFTER INSERT ON original_query_queue
    FOR EACH ROW EXECUTE FUNCTION notify_original_query_insert();

CREATE TRIGGER trigger_notify_fragment_query_insert
    AFTER INSERT ON query_fragment_queue
    FOR EACH ROW EXECUTE FUNCTION notify_fragment_query_insert();
```

Applications can listen for immediate processing:

```python
import psycopg2

conn = psycopg2.connect(connection_string)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Listen for new queue items
cur.execute("LISTEN new_original_query;")
cur.execute("LISTEN new_fragment_query;")

# Process notifications
while True:
    if select.select([conn], [], [], 5) == ([], [], []):
        continue
    conn.poll()
    while conn.notifies:
        notify = conn.notifies.pop(0)
        if notify.channel == "new_fragment_query":
            # Process immediately
            process_queue_item()
```

## Performance Characteristics

### Throughput Comparison

| Provider | Throughput | Latency | Scalability |
|----------|------------|---------|-------------|
| **PostgreSQL** | Medium-High | Low | Excellent |
| **Redis** | Very High | Very Low | Excellent |

### Memory Usage

| Component | PostgreSQL | Redis |
|-----------|------------|-------|
| **Queue Storage** | Disk-based | Memory-based |
| **Metadata** | SQL tables | JSON objects |
| **Indexes** | B-tree | Hash tables |

### Scalability Patterns

**PostgreSQL Provider:**
- Vertical scaling: Increase database resources
- Horizontal scaling: Read replicas for monitoring
- Partitioning: Time-based queue partitioning

**Redis Provider:**
- Vertical scaling: Increase Redis memory
- Horizontal scaling: Redis Cluster
- Sharding: Multiple Redis instances by partition key

## Monitoring and Debugging

### Queue Status Monitoring

```sql
-- PostgreSQL: Check queue depths and priorities
SELECT 
    'incoming' as queue_type,
    partition_key,
    priority,
    COUNT(*) as item_count,
    MIN(created_at) as oldest_item,
    MAX(created_at) as newest_item
FROM original_query_queue 
GROUP BY partition_key, priority
UNION ALL
SELECT 
    'outgoing' as queue_type,
    partition_key,
    priority,
    COUNT(*) as item_count,
    MIN(created_at) as oldest_item,
    MAX(created_at) as newest_item
FROM query_fragment_queue 
GROUP BY partition_key, priority
ORDER BY queue_type, priority DESC;
```

```bash
# Redis: Check queue contents
redis-cli -h localhost -p 6379 llen partition_cache_queue_original_query
redis-cli -h localhost -p 6379 llen partition_cache_queue_fragment_query

# Inspect queue items
redis-cli -h localhost -p 6379 lrange partition_cache_queue_original_query 0 4
```

### Performance Monitoring

```python
import time
import partitioncache

# Monitor processing rates
start_time = time.time()
initial_incoming, initial_outgoing = partitioncache.get_queue_lengths()

# Wait and measure
time.sleep(60)
final_incoming, final_outgoing = partitioncache.get_queue_lengths()

# Calculate processing rates
elapsed = time.time() - start_time
incoming_rate = (initial_incoming - final_incoming) / elapsed * 60  # per minute
outgoing_rate = (initial_outgoing - final_outgoing) / elapsed * 60  # per minute

print(f"Incoming processing rate: {incoming_rate:.1f} items/minute")
print(f"Outgoing processing rate: {outgoing_rate:.1f} items/minute")
```

## Error Handling and Recovery

### Database Connection Issues

The system includes comprehensive error handling:

```python
try:
    query, partition_key = partitioncache.pop_from_incoming_queue()
except Exception as e:
    logger.error(f"Queue operation failed: {e}")
    # Graceful degradation or retry logic
```

### Queue Corruption Recovery

**PostgreSQL:**
```sql
-- Check for orphaned items
SELECT COUNT(*) FROM original_query_queue 
WHERE created_at < NOW() - INTERVAL '24 hours';

-- Clean up old items if needed
DELETE FROM original_query_queue 
WHERE created_at < NOW() - INTERVAL '24 hours'
AND priority = 1;  -- Only clean up low-priority items
```

**Redis:**
```bash
# Check queue consistency
redis-cli -h localhost -p 6379 llen partition_cache_queue_original_query
redis-cli -h localhost -p 6379 llen partition_cache_queue_fragment_query

# Clear corrupted queues if needed
redis-cli -h localhost -p 6379 del partition_cache_queue_original_query
redis-cli -h localhost -p 6379 del partition_cache_queue_fragment_query
```

## Best Practices

### Queue Provider Selection

**Choose PostgreSQL when:**
- Priority processing is required
- Durable storage is important
- Complex monitoring needs
- Integration with existing PostgreSQL infrastructure

**Choose Redis when:**
- Maximum throughput is required
- Temporary storage is acceptable
- Distributed processing architecture
- Memory-based performance is critical

### Operational Guidelines

1. **Monitor Queue Depths**: Prevent unbounded growth
2. **Priority Management**: Use priorities for important queries
3. **Partition Key Strategy**: Align with data access patterns
4. **Error Recovery**: Implement robust error handling
5. **Capacity Planning**: Monitor processing rates vs. input rates

### Configuration Tuning

**High-Throughput Setup:**
```bash
# Redis provider for maximum throughput
QUERY_QUEUE_PROVIDER=redis
REDIS_HOST=high_memory_instance
REDIS_PORT=6379

# Multiple monitor processes
for i in {1..4}; do
    pcache-monitor &
done
```

**Priority-Aware Setup:**
```bash
# PostgreSQL provider for priority processing
QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=database_server

# Single monitor with priority processing
pcache-monitor --priority-processing
```

## Migration and Compatibility

### From Single Queue Systems

The two-queue system is backward compatible:

```python
# Old single-queue code still works
partitioncache.add_query_to_cache(query, cache_handler, partition_key)

# New two-queue code provides more control
partitioncache.push_to_incoming_queue(query, partition_key)
# ... processing happens ...
partitioncache.push_to_outgoing_queue(query_hash_pairs, partition_key)
```

### Provider Migration

**PostgreSQL to Redis:**
1. Drain PostgreSQL queues
2. Update environment variables
3. Restart monitor processes
4. Verify Redis connectivity

**Redis to PostgreSQL:**
1. Set up PostgreSQL tables
2. Update environment variables
3. Restart monitor processes
4. Verify priority processing

This two-queue architecture provides the foundation for scalable, reliable, and high-performance query processing in PartitionCache systems. 
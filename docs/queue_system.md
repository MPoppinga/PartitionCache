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
│ Original Query  │    │ Processing      │    │ Query Fragment  │
│ Queue           │    │ Thread          │    │ Queue           │
│ (PostgreSQL/    │    │                 │    │ (PostgreSQL/    │
│  Redis)         │    │ - Fragments     │    │  Redis)         │
│                 │    │ - Optimization  │    │                 │
│ - Original SQL  │    │ - Validation    │    │ - Query Hash    │
│ - Partition Key │    │ - Partition Key │    │ - Partition Key │
│ - Priority*     │    │   Management    │    │ - Priority*     │
│ - User input    │    │                 │    │ - Ready to run  │
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
- **Non-blocking queue operations** for optimal performance:
  - All queue operations use non-blocking upsert functions for proper concurrency handling
  - Batch processing for fragment queues improves performance on large query sets
  - Priority increment for duplicates ensures important queries get processed first

**Configuration:**
See [CLI Reference - Global Options](cli_reference.md#global-options) for complete environment setup.

**Database Schema:**
```sql
-- Original Query Queue
CREATE TABLE original_query_queue (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    partition_datatype TEXT,
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
    partition_datatype TEXT,
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
See [CLI Reference - Global Options](cli_reference.md#global-options) for Redis queue setup.

## Queue Operations

### PostgreSQL Queue Operations

All PostgreSQL queue operations now use a unified approach for optimal performance and reliability:

#### Non-blocking Queue Functions
- **Method**: Non-blocking functions with `FOR UPDATE SKIP LOCKED`
- **Performance**: Optimal throughput with proper concurrency handling  
- **Behavior**: Duplicate entries increment priority counter appropriately
- **Batch Support**: Fragment queues use efficient batch upsert operations
- **Fallback**: Graceful degradation to traditional upsert if functions unavailable

#### Key Benefits
- **No Blocking**: Avoids row-level locks that could stall high-concurrency scenarios
- **Priority Management**: Ensures frequently requested queries get higher priority
- **Batch Efficiency**: Processes multiple fragments in single database calls
- **Concurrency Safe**: Proper handling of concurrent insertions and updates

**Technical Implementation:**
The priority functions use PostgreSQL's `FOR UPDATE NOWAIT` to avoid blocking when encountering rows locked by `FOR UPDATE SKIP LOCKED`. The non-blocking approach:
1. Tries `SELECT ... FOR UPDATE NOWAIT` to check if row exists
2. If row exists and unlocked → UPDATE priority
3. If row doesn't exist → INSERT with ON CONFLICT handling
4. If row is locked → Return 'skipped_locked'
5. Handle concurrent INSERT races → Return 'skipped_concurrent'

**Python API for Priority Operations:**
```python
# Strategy 2: Priority operations
handler.push_to_original_query_queue_with_priority(query, partition_key, priority)
handler.push_to_query_fragment_queue_with_priority(query_hash_pairs, partition_key, priority)
```

The non-blocking functions (`non_blocking_fragment_queue_upsert()`, `non_blocking_original_queue_upsert()`) are automatically deployed during PostgreSQL queue handler initialization from `postgresql_queue_helper.sql`.

### Core Functions

#### `push_to_original_query_queue(query: str, partition_key: str = "partition_key", partition_datatype: str | None = None, queue_provider: str | None = None)`
Add original queries to the original query queue (uses Strategy 1):

```python
import partitioncache

# Add query with custom partition key (duplicates ignored)
partitioncache.push_to_original_query_queue(
    query="SELECT * FROM users WHERE age > 25",
    partition_key="user_id",
    partition_datatype="integer"
)
```

#### `push_to_query_fragment_queue(query_hash_pairs: list[tuple[str, str]], partition_key: str = "partition_key", partition_datatype: str = "integer", queue_provider: str | None = None)`
Add processed query fragments to the query fragment queue:

```python
# Add query fragments with partition key
query_hash_pairs = [
    ("SELECT * FROM users WHERE age > 25 AND user_id = 1", "hash123"),
    ("SELECT * FROM users WHERE age > 25 AND user_id = 2", "hash456")
]
partitioncache.push_to_query_fragment_queue(query_hash_pairs, "user_id", "integer")
```

#### `pop_from_original_query_queue(queue_provider: str | None = None)`
Retrieve original queries for processing:

```python
# Returns (query, partition_key, partition_datatype) tuple or None
result = partitioncache.pop_from_original_query_queue()
if result:
    query, partition_key, partition_datatype = result
    print(f"Processing query for partition: {partition_key}")
```

#### `pop_from_query_fragment_queue(queue_provider: str | None = None)`
Retrieve query fragments for execution:

```python
# Returns (query, hash, partition_key, partition_datatype) tuple or None
result = partitioncache.pop_from_query_fragment_queue()
if result:
    query, hash_val, partition_key, partition_datatype = result
    print(f"Executing query {hash_val} for partition: {partition_key}")
```

#### `get_queue_lengths(queue_provider: str | None = None)`
Monitor queue status:

```python
lengths = partitioncache.get_queue_lengths()
original_count = lengths.get("original_query_queue", 0)
fragment_count = lengths.get("query_fragment_queue", 0)
print(f"Original: {original_count}, Fragment: {fragment_count}")
```

## Processing Architecture

### Monitor Cache Queue

The monitor implements a sophisticated two-threaded architecture:

**Thread 1: Fragment Processor**
- Consumes `(query, partition_key, partition_datatype)` from original query queue
- Uses partition key from queue (not command line)
- Breaks down queries using `generate_all_query_hash_pairs()`
- Pushes fragments with partition key to query fragment queue

**Thread 2: Execution Pool**
- Consumes `(query, hash, partition_key, partition_datatype)` from query fragment queue
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
# Add to original query queue with partition key
pcache-add \
    --query "SELECT * FROM users WHERE age > 25" \
    --queue-original \
    --partition-key "user_id"

# Generate fragments and add directly to query fragment queue
pcache-add \
    --query "SELECT * FROM complex_query" \
    --queue \
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
import select

# Assume conn is a psycopg2 connection
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Listen for new queue items
cur.execute("LISTEN original_query_available;")
cur.execute("LISTEN query_fragment_available;")

# Process notifications
while True:
    if select.select([conn], [], [], 5) == ([], [], []):
        continue
    conn.poll()
    while conn.notifies:
        notify = conn.notifies.pop(0)
        if notify.channel == "query_fragment_available":
            # Process immediately
            process_queue_item()
```


## Monitoring and Debugging

### Queue Status Monitoring

```sql
-- PostgreSQL: Check queue depths and priorities
SELECT 
    'original_query_queue' as queue_type,
    partition_key,
    priority,
    COUNT(*) as item_count,
    MIN(created_at) as oldest_item,
    MAX(created_at) as newest_item
FROM original_query_queue 
GROUP BY partition_key, priority
UNION ALL
SELECT 
    'query_fragment_queue' as queue_type,
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
Use CLI tools to monitor queue processing rates
See [CLI Reference](cli_reference.md) for more details
Or use look up the queue lengths in the database:

```python
import time
import partitioncache

# Monitor processing rates
start_time = time.time()
initial_lengths = partitioncache.get_queue_lengths()
initial_incoming = initial_lengths.get("original_query_queue", 0)
initial_outgoing = initial_lengths.get("query_fragment_queue", 0)

# Wait and measure
time.sleep(60)
final_lengths = partitioncache.get_queue_lengths()
final_incoming = final_lengths.get("original_query_queue", 0)
final_outgoing = final_lengths.get("query_fragment_queue", 0)

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
    result = partitioncache.pop_from_original_query_queue()
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


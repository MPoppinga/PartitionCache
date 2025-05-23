# Two-Queue System Implementation

This document describes the implementation of the two-queue system for PartitionCache, which separates query processing into incoming and outgoing queues for improved flexibility and performance. The system now supports partition key management and priority-based processing.

## Overview

The two-queue system replaces the single queue approach with:

1. **Incoming Queue**: Accepts original queries with partition keys for asynchronous processing
2. **Outgoing Queue**: Contains pre-processed query fragments with partition keys ready for execution

### New Features

- **Partition Key Support**: All queue operations now include partition key tracking
- **Priority System**: PostgreSQL provider supports automatic priority increment for duplicate queries
- **Flexible Processing**: Monitor no longer requires partition key from command line

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

## Components

### 1. Queue Module (`src/partitioncache/queue.py`)

#### Updated Functions:

- `push_to_incoming_queue(query, partition_key="partition_key")`: Push original queries with partition key
- `push_to_outgoing_queue(query_hash_pairs, partition_key="partition_key")`: Push query fragments with partition key
- `pop_from_incoming_queue()`: Returns `(query, partition_key)` tuple
- `pop_from_outgoing_queue()`: Returns `(query, hash, partition_key)` tuple
- `get_queue_lengths()`: Get lengths of both queues

#### Provider Support:

**PostgreSQL Queue Provider:**
- Full priority support with automatic increment for duplicates
- Partition key tracking in dedicated columns
- Optimized indexes for priority-based retrieval

**Redis Queue Provider:**
- Partition key support via JSON storage
- FIFO processing (no priority system)
- Lightweight for high-throughput scenarios

### 2. Monitor Cache Queue (`src/partitioncache/cli/monitor_cache_queue.py`)

#### Enhanced Two-Threaded Architecture:

**Thread 1: Fragment Processor**
- Consumes `(query, partition_key)` from incoming queue
- Uses partition key from queue (not command line)
- Breaks down queries using `generate_all_query_hash_pairs()`
- Pushes fragments with partition key to outgoing queue

**Thread 2: Execution Pool**
- Consumes `(query, hash, partition_key)` from outgoing queue
- Executes fragments against database
- Stores results in cache with partition key tracking

### 3. CLI Tools

#### Updated `add_to_cache.py`:
- `--queue`: Add to incoming queue with partition key
- `--queue-fragments`: Generate fragments and add directly to outgoing queue with partition key
- Automatically passes `--partition-key` to queue operations

#### Updated `manage_cache.py`:
- `--count-queue`: Show lengths of both queues
- `--clear-queue`: Clear both queues
- `--clear-incoming-queue`: Clear only incoming queue
- `--clear-outgoing-queue`: Clear only outgoing queue

## Usage Examples

### 1. Basic Workflow with Partition Keys

```bash
# Add original query to incoming queue with custom partition key
python -m partitioncache.cli.add_to_cache \
    --query "SELECT * FROM users WHERE age > 25" \
    --queue \
    --partition-key "user_id"

# Monitor queue processing (partition key comes from queue)
python -m partitioncache.cli.monitor_cache_queue \
    --db-backend postgresql \
    --cache-backend postgresql_bit \
    --db-name mydb
```

### 2. Direct Fragment Processing

```bash
# Generate fragments and add directly to outgoing queue
python -m partitioncache.cli.add_to_cache \
    --query "SELECT * FROM complex_query" \
    --queue \
    --partition-key "custom_partition_key"
```

### 3. Priority System (PostgreSQL only)

```bash
# Same query added multiple times gets higher priority automatically
python -m partitioncache.cli.add_to_cache \
    --query "SELECT * FROM high_priority_table" \
    --queue \
    --partition-key "important_key"

# Run again - priority will increment automatically
python -m partitioncache.cli.add_to_cache \
    --query "SELECT * FROM high_priority_table" \
    --queue \
    --partition-key "important_key"
```

### 4. Queue Management

```bash
# Clear all queues
python -m partitioncache.cli.manage_cache --clear-queue

# Clear specific queues
python -m partitioncache.cli.manage_cache --clear-incoming-queue
python -m partitioncache.cli.manage_cache --clear-outgoing-queue
```

## Environment Variables

### PostgreSQL Queue Provider (Recommended)

```bash
# PostgreSQL queue configuration
QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5432
PG_QUEUE_USER=queue_user
PG_QUEUE_PASSWORD=queue_password
PG_QUEUE_DB=partition_cache_queues
```

### Redis Queue Provider

```bash
# Redis queue configuration
QUERY_QUEUE_PROVIDER=redis
REDIS_HOST=localhost
REDIS_PORT=6379
QUERY_QUEUE_REDIS_DB=1
QUERY_QUEUE_REDIS_QUEUE_KEY=partition_cache_queue

# Optional Redis password
REDIS_PASSWORD=your_password
```

## Benefits

### 1. Partition Key Management
- Support for multiple partition keys in single queue system
- Automatic partition key tracking through the entire pipeline
- Better organization and filtering capabilities

### 2. Priority Processing (PostgreSQL)
- Automatic priority increment for duplicate queries
- Ensures important queries are processed first
- Configurable priority-based retrieval

### 3. Flexibility
- Clients can choose PostgreSQL (with priorities) or Redis (lightweight)
- Different processing strategies for different use cases
- Backward compatibility maintained

### 4. Performance
- Separation of concerns between query processing and execution
- Independent scaling of processing and execution components
- Optimized database indexes for priority-based access

### 5. Monitoring
- Clear visibility into both queue states with partition key information
- Better debugging and operational insights
- Enhanced logging with partition key tracking

## Database Schema (PostgreSQL Provider)

### Original Query Queue Table
```sql
CREATE TABLE original_query_queue (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(query, partition_key)
);
```

### Query Fragment Queue Table
```sql
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

## Testing

### Unit Tests

Run the comprehensive test suite:

```bash
# Test queue functionality with partition keys
pytest tests/test_queue.py -v

# Test monitor cache queue functionality
pytest tests/test_monitor_cache_queue.py -v
```

### Demo Script

Run the interactive demo:

```bash
python examples/scripts/two_queue_demo.py
```

## Migration Guide

### From Previous Version

1. **Queue Interface Changes**: 
   - `pop_from_incoming_queue()` now returns `(query, partition_key)`
   - `pop_from_outgoing_queue()` now returns `(query, hash, partition_key)`
   - All push functions now require partition_key parameter

2. **Monitor Changes**: 
   - Remove `--partition-key` argument from monitor_cache_queue
   - Partition keys now come from the queue automatically

3. **Database Updates**: 
   - PostgreSQL tables will be automatically updated with new columns
   - Redis keys remain compatible with JSON structure

### Environment Updates

**PostgreSQL Provider (New):**
```bash
QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5432
PG_QUEUE_USER=queue_user
PG_QUEUE_PASSWORD=queue_password
PG_QUEUE_DB=partition_cache_queues
```

**Redis Provider (Updated):**
- Queue structure remains the same but now includes partition_key in JSON

## Error Handling

The system includes comprehensive error handling:

- **Database connection failures**: Graceful degradation with error logging
- **Invalid data in queues**: Automatic skipping with error logging
- **Queue timeouts**: Non-blocking operations with configurable timeouts
- **Fragment processing errors**: Isolated error handling per fragment
- **Priority conflicts**: Automatic resolution with increment logic

## Monitoring and Debugging

### Queue Status

```bash
# Get detailed queue statistics
python -m partitioncache.cli.manage_cache --count-queue
```

### Priority Monitoring (PostgreSQL)

Monitor high-priority queries:
```sql
-- Check current priorities in original queue
SELECT partition_key, priority, COUNT(*) 
FROM original_query_queue 
GROUP BY partition_key, priority 
ORDER BY priority DESC;

-- Check fragment priorities
SELECT partition_key, priority, COUNT(*) 
FROM query_fragment_queue 
GROUP BY partition_key, priority 
ORDER BY priority DESC;
```

## Best Practices

1. **Partition Key Strategy**: Use meaningful partition keys that align with your data partitioning
2. **Priority Management**: Leverage automatic priority increment for important recurring queries
3. **Provider Selection**: Use PostgreSQL for priority features, Redis for high-throughput scenarios
4. **Queue Sizing**: Monitor queue lengths to prevent memory/storage issues
5. **Resource Management**: Scale worker processes based on queue depth and priority distribution

## Troubleshooting

### Common Issues

1. **Queues not working**: Check database/Redis connection and environment variables
2. **Fragments not processing**: Verify monitor_cache_queue is running and has correct permissions
3. **Priority not working**: Ensure you're using PostgreSQL provider
4. **Partition key issues**: Verify partition keys are being passed correctly through the pipeline

### Debug Commands

**PostgreSQL:**
```sql
-- Check queue contents
SELECT * FROM original_query_queue ORDER BY priority DESC, created_at ASC LIMIT 10;
SELECT * FROM query_fragment_queue ORDER BY priority DESC, created_at ASC LIMIT 10;
```

**Redis:**
```bash
# Check Redis connectivity
redis-cli -h localhost -p 6379 ping

# Check queue keys
redis-cli -h localhost -p 6379 keys "*queue*"

# Inspect queue contents
redis-cli -h localhost -p 6379 lrange partition_cache_queue_original_query 0 -1
``` 
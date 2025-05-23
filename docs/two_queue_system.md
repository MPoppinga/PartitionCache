# Two-Queue System Implementation

This document describes the implementation of the two-queue system for PartitionCache, which separates query processing into incoming and outgoing queues for improved flexibility and performance.

## Overview

The two-queue system replaces the single queue approach with:

1. **Incoming Queue**: Accepts original queries for asynchronous processing
2. **Outgoing Queue**: Contains pre-processed query fragments ready for execution

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client        │    │  Fragment       │    │  Execution      │
│   Applications  │    │  Processor      │    │  Workers        │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │ Original  │  │───▶│  │ Query     │  │───▶│  │ Database  │  │
│  │ Queries   │  │    │  │ Breakdown │  │    │  │ Execution │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Incoming Queue  │    │ Processing      │    │ Outgoing Queue  │
│ (Redis)         │    │ Thread          │    │ (Redis)         │
│                 │    │                 │    │                 │
│ - Original SQL  │    │ - Fragments     │    │ - Query Hash    │
│ - Full queries  │    │ - Optimization  │    │ - Ready to run  │
│ - User input    │    │ - Validation    │    │ - Pre-processed │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Components

### 1. Queue Module (`src/partitioncache/queue.py`)

#### New Functions:

- `push_to_incoming_queue(query)`: Push original queries to the incoming queue
- `push_to_outgoing_queue(query_hash_pairs)`: Push query fragments to the outgoing queue
- `pop_from_incoming_queue()`: Retrieve queries from the incoming queue
- `pop_from_outgoing_queue()`: Retrieve fragments from the outgoing queue
- `get_queue_lengths()`: Get lengths of both queues

#### Redis Key Structure:

- Incoming queue: `{base_key}_incoming`
- Outgoing queue: `{base_key}_outgoing`

### 2. Monitor Cache Queue (`src/partitioncache/cli/monitor_cache_queue.py`)

#### Two-Threaded Architecture:

**Thread 1: Fragment Processor**
- Consumes from incoming queue
- Breaks down queries using `generate_all_query_hash_pairs()`
- Pushes fragments to outgoing queue

**Thread 2: Execution Pool**
- Consumes from outgoing queue
- Executes fragments against database
- Stores results in cache

### 3. CLI Tools

#### Updated `add_to_cache.py`:
- `--queue`: Add to incoming queue (existing behavior)
- `--queue-fragments`: Generate fragments and add directly to outgoing queue

#### Updated `manage_cache.py`:
- `--count-queue`: Show lengths of both queues
- `--clear-queue`: Clear both queues
- `--clear-incoming-queue`: Clear only incoming queue
- `--clear-outgoing-queue`: Clear only outgoing queue

## Usage Examples

### 1. Basic Workflow

```bash
# Add original query to incoming queue
python -m partitioncache.cli.add_to_cache \
    --query "SELECT * FROM users WHERE age > 25" \
    --queue \
    --partition-key "partition_key"

# Monitor queue processing
python -m partitioncache.cli.monitor_cache_queue \
    --db-backend sqlite \
    --cache-backend rocksdb \
    --partition-key "partition_key"

# Check queue status
python -m partitioncache.cli.manage_cache --count-queue
```

### 2. Direct Fragment Processing

```bash
# Generate fragments and add directly to outgoing queue
python -m partitioncache.cli.add_to_cache \
    --query "SELECT * FROM complex_query" \
    --queue-fragments \
    --partition-key "partition_key"
```

### 3. Queue Management

```bash
# Clear all queues
python -m partitioncache.cli.manage_cache --clear-queue

# Clear specific queues
python -m partitioncache.cli.manage_cache --clear-incoming-queue
python -m partitioncache.cli.manage_cache --clear-outgoing-queue
```

## Environment Variables

```bash
# Required for Redis queue provider
QUERY_QUEUE_PROVIDER=redis
REDIS_HOST=localhost
REDIS_PORT=6379
QUERY_QUEUE_REDIS_DB=1
QUERY_QUEUE_REDIS_QUEUE_KEY=partition_cache_queue

# Optional Redis password
REDIS_PASSWORD=your_password
```

## Benefits

### 1. Flexibility
- Clients can choose to send original queries or pre-processed fragments
- Different processing strategies for different use cases

### 2. Performance
- Separation of concerns between query processing and execution
- Independent scaling of processing and execution components

### 3. Monitoring
- Clear visibility into both queue states
- Better debugging and operational insights

### 4. Backwards Compatibility
- Legacy `push_to_queue()` function maintained for compatibility
- Existing code continues to work without changes

## Testing

### Unit Tests

Run the comprehensive test suite:

```bash
# Test queue functionality
pytest tests/test_queue.py -v

# Test monitor cache queue functionality
pytest tests/test_monitor_cache_queue.py -v
```

### Demo Script

Run the interactive demo:

```bash
python examples/two_queue_demo.py
```

## Migration Guide

### From Single Queue

1. **No immediate action required**: The legacy `push_to_queue()` function automatically uses the incoming queue
2. **Update monitoring**: Use new queue management commands for better visibility
3. **Optimize workflows**: Consider using `--queue-fragments` for pre-processed queries

### Environment Updates

Update your Redis queue key structure:
- Old: `QUERY_QUEUE_REDIS_QUEUE_KEY=my_queue`
- New: Incoming will be `my_queue_incoming`, outgoing will be `my_queue_outgoing`

## Error Handling

The system includes comprehensive error handling:

- **Redis connection failures**: Graceful degradation with error logging
- **Invalid JSON in outgoing queue**: Automatic skipping with error logging
- **Queue timeouts**: Non-blocking operations with configurable timeouts
- **Fragment processing errors**: Isolated error handling per fragment

## Monitoring and Debugging

### Queue Status

```bash
# Get detailed queue statistics
python -m partitioncache.cli.manage_cache --count-queue
```

### Log Output

The monitor process provides detailed logging:
- Fragment generation progress
- Queue length monitoring
- Processing time statistics
- Error reporting

### Performance Metrics

Monitor these key metrics:
- Incoming queue depth
- Outgoing queue depth
- Fragment processing rate
- Cache hit/miss ratios
- Query execution times

## Best Practices

1. **Queue Sizing**: Monitor queue lengths to prevent memory issues
2. **Error Handling**: Implement proper retry logic for failed fragments
3. **Monitoring**: Set up alerts for queue depth and processing rates
4. **Resource Management**: Scale worker processes based on queue depth
5. **Testing**: Use the demo script to validate configuration changes

## Troubleshooting

### Common Issues

1. **Queues not working**: Check Redis connection and environment variables
2. **Fragments not processing**: Verify monitor_cache_queue is running
3. **High memory usage**: Monitor and clear queues regularly
4. **Performance issues**: Scale worker processes or optimize fragment size

### Debug Commands

```bash
# Check Redis connectivity
redis-cli -h localhost -p 6379 ping

# Monitor Redis operations
redis-cli -h localhost -p 6379 monitor

# Check queue keys
redis-cli -h localhost -p 6379 keys "*queue*"
``` 
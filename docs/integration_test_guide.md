# PartitionCache Integration Test Guide

## Overview

PartitionCache has a comprehensive integration test suite that validates the functionality of all cache backends, queue processing, CLI tools, and end-to-end workflows. The tests are designed to run both locally and in CI/CD environments.

## Test Structure

```
tests/
├── integration/          # Integration tests
│   ├── conftest.py      # Shared fixtures and configuration
│   ├── test_cache_backends.py     # Core cache functionality tests
│   ├── test_queue_processor.py    # Queue processing and pg_cron tests
│   ├── test_cli.py               # CLI command tests
│   ├── test_cache_performance.py  # Performance and concurrency tests
│   ├── test_error_recovery.py     # Error handling tests
│   ├── test_end_to_end_workflows.py # Complete workflow tests
│   └── test_spatial_cache.py      # Spatial data caching tests
└── pytest/              # Unit tests
```

## What Each Test Module Covers

### test_cache_backends.py
- Basic cache operations (set/get/exists/delete)
- All supported datatypes (integer, float, text, timestamp)
- Multi-partition key support
- Cache invalidation and updates
- Edge cases and boundary conditions

### test_queue_processor.py
- Asynchronous queue processing
- pg_cron job scheduling and execution
- Queue error handling and retries
- Two-queue system workflow
- Concurrent queue processing

### test_cli.py
- All CLI commands (pcache-manage, pcache-add, pcache-read, etc.)
- Command-line argument parsing
- Error messages and exit codes
- Integration with cache backends

### test_cache_performance.py
- Throughput benchmarks
- Latency measurements
- Concurrent access patterns
- Memory usage
- Large dataset handling

### test_error_recovery.py
- Connection failures
- Transaction rollbacks
- Corrupted data handling
- Resource exhaustion
- Graceful degradation

### test_end_to_end_workflows.py
- Complete OSM-style workflows
- Multi-step data processing
- Cache warming strategies
- Real-world usage patterns

## External Dependencies

### PostgreSQL
- **Required Version**: 14+
- **Extensions**: pg_cron (required for queue tests), roaringbitmap (required for roaringbitmap backend)
- **Databases**: Separate databases for each test job to ensure isolation
- **Tables**: Automatically created during test setup

### Redis
- **Required Version**: 7+
- **Databases**: Separate DB numbers for each test job (0-4 for cache, 10-14 for bit operations)
- **Memory**: At least 100MB available

### RocksDB
- **Optional**: Only required if testing RocksDB backends
- **Storage**: Temporary directories created per test run
- **Cleanup**: Automatic cleanup after tests

## Running Tests Locally

### Setup

1. **Start test services**:
```bash
./scripts/setup_integration_testing.sh --service "postgres-integration redis-integration"
```

2. **Load environment variables**:
```bash
source scripts/load_env.sh
```

3. **Run all integration tests**:
```bash
python -m pytest tests/integration/ -v
```

### Running Specific Tests

```bash
# Test specific backend
python -m pytest tests/integration/ -k postgresql_array

# Test specific module
python -m pytest tests/integration/test_cli.py

# Run with coverage
python -m pytest tests/integration/ --cov=partitioncache

# Run in parallel
python -m pytest tests/integration/ -n auto
```

### Cleanup

```bash
./scripts/stop_integration_testing.sh --clean
```

## GitHub Actions Execution

The CI/CD pipeline runs tests in a matrix configuration:

### Job Matrix
- **PostgreSQL Backends**: Tests all PostgreSQL-based cache handlers
- **Redis Backends**: Tests Redis-based implementations
- **RocksDB Backends**: Tests file-based storage
- **Queue Processing**: Tests async queue and pg_cron
- **CLI Tools**: Tests command-line interface

### Key Differences from Local Execution

1. **Database Isolation**: Each job gets a unique database name with run ID
2. **Service Containers**: Uses Docker services instead of docker-compose
3. **Parallel Execution**: Jobs run concurrently with resource isolation

### Environment Variables

```yaml
# Unique per job
UNIQUE_DB_NAME: partitioncache_integration_${suffix}_${run_id}
REDIS_CACHE_DB: ${job_index}
REDIS_BIT_DB: ${job_index + 10}

# Table prefixes include job suffix
PG_ARRAY_CACHE_TABLE_PREFIX: ci_array_cache_${suffix}

# File paths include run ID
ROCKSDB_PATH: /tmp/ci_rocksdb_${suffix}_${run_id}
```

## Test Markers

```python
@pytest.mark.slow         # Long-running tests
@pytest.mark.performance  # Performance benchmarks
@pytest.mark.stress      # High-load tests
@pytest.mark.concurrent  # Concurrency tests
@pytest.mark.serial      # Must run sequentially
```

## Troubleshooting

### Common Issues

1. **Tests Skip**: Check service connectivity and environment variables
2. **Database Errors**: Ensure PostgreSQL has required extensions
3. **Redis Errors**: Verify Redis is running and accessible
4. **Timeout Errors**: Increase test timeouts for slow systems

### Debug Commands

```bash
# Check service status
docker-compose -f docker-compose.integration.yml ps

# View PostgreSQL logs
docker-compose -f docker-compose.integration.yml logs postgres-integration

# Test database connection
psql -h localhost -p 5434 -U integration_user -d partitioncache_integration

# Test Redis connection
redis-cli -h localhost -p 6381 ping
```

## Queue Processing Testing

### Manual vs pg_cron Processing

Integration tests use two different approaches for queue processing:

1. **Manual Processing (Default)**
   - Used by most integration tests to avoid concurrency issues
   - Calls `partitioncache_manual_process_queue()` directly
   - Provides deterministic, immediate processing
   - No job name conflicts in parallel CI runs

2. **pg_cron Processing (Production)**
   - Used by dedicated `test_pg_cron_integration.py`
   - Tests real pg_cron scheduling and execution
   - Uses database-specific job name prefixes for isolation
   - Validates production queue processing behavior

### Manual Processing Usage

```python
# In tests, process queue manually
with db_session.cursor() as cur:
    cur.execute("SELECT * FROM partitioncache_manual_process_queue(10)")
    result = cur.fetchone()
    processed_count, message = result
```

### pg_cron Isolation Strategy

For tests that must use pg_cron:
- Job names include database name prefix: `{db_name}_test_{timestamp}`
- Each test creates unique job names to avoid conflicts
- Tests clean up their jobs after completion
- Only one test class uses pg_cron to minimize conflicts



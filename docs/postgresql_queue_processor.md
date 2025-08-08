# PostgreSQL Queue Processor

## Overview

The PostgreSQL Queue Processor provides a sophisticated database-native approach to automated query processing and caching. Instead of relying on external Python processes, this system leverages PostgreSQL's built-in `pg_cron` extension to automatically process queued queries and populate partition caches entirely within the database.

This approach offers significant advantages in reliability, performance, and operational simplicity by eliminating external dependencies and providing comprehensive monitoring and logging capabilities.

### Cross-Database Architecture Support

PartitionCache supports cross-database pg_cron configurations where:
- **pg_cron** is installed in a central database (typically `postgres`)
- **Work databases** contain PartitionCache tables and business logic
- **Jobs execute** in work databases via `cron.schedule_in_database()`

This architecture provides better isolation, security, and scalability.

#### Cross-Database Setup Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Instance                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐                    ┌─────────────────────────┐    │
│  │  pg_cron DB     │                    │     Cache Database      │    │
│  │  (postgres)     │                    │    (geominedb)          │    │
│  │                 │                    │                         │    │
│  │ ┌─────────────┐ │ schedule_in_       │ ┌─────────────────────┐ │    │
│  │ │  pg_cron    │ │ database()         │ │  Queue Tables       │ │    │
│  │ │  Extension  │ │ ─────────────────→ │ │  Cache Tables       │ │    │
│  │ │  Config     │ │                    │ │  Processor Functions│ │    │
│  │ │  Tables     │ │                    │ │  Business Logic     │ │    │
│  │ └─────────────┘ │                    │ └─────────────────────┘ │    │
│  └─────────────────┘                    └─────────────────────────┘    │
│                                         └─────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Environment Configuration

```bash
# Cache Database (where queues and cache are stored)
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=geominedb

# Queue Database (must be same as cache database)
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5432
PG_QUEUE_USER=your_username
PG_QUEUE_PASSWORD=your_password
PG_QUEUE_DB=geominedb

# pg_cron Database (can be different from cache database)
PG_CRON_HOST=localhost                   # Default: same as DB_HOST
PG_CRON_PORT=5432                       # Default: same as DB_PORT
PG_CRON_USER=your_username              # Default: same as DB_USER
PG_CRON_PASSWORD=your_password          # Default: same as DB_PASSWORD
PG_CRON_DATABASE=postgres               # Default: postgres

# Cache Backend Configuration
CACHE_BACKEND=postgresql_array
PG_ARRAY_CACHE_TABLE_PREFIX=partitioncache
```

#### Setup Steps

1. **Install pg_cron Extension**:
   ```sql
   -- Connect to the pg_cron database (usually postgres)
   psql -U your_username -d postgres
   
   -- Install pg_cron extension
   CREATE EXTENSION IF NOT EXISTS pg_cron;
   ```

2. **Grant Permissions**:
   ```sql
   -- Grant necessary permissions to your user
   GRANT USAGE ON SCHEMA cron TO your_username;
   GRANT EXECUTE ON FUNCTION cron.schedule_in_database TO your_username;
   GRANT EXECUTE ON FUNCTION cron.unschedule TO your_username;
   GRANT EXECUTE ON FUNCTION cron.alter_job TO your_username;
   GRANT SELECT ON cron.job TO your_username;
   ```

3. **Setup PartitionCache**:
   ```bash
   # Setup the PostgreSQL queue processor
   pcache-postgresql-queue-processor setup --enable-after-setup
   
   # Verify setup
   pcache-postgresql-queue-processor status
   ```

#### How Cross-Database Processing Works

1. **Setup Phase**: 
   - Script connects to `postgres` database to install pg_cron configuration objects
   - Creates processor functions and queue tables in `geominedb`

2. **Job Scheduling**:
   - pg_cron jobs are stored in `postgres` database  
   - Jobs use `cron.schedule_in_database()` to execute in `geominedb`

3. **Processing**:
   - Jobs run in `geominedb` where queues and cache tables exist
   - Configuration is retrieved from `postgres` database via dblink
   - All business logic executes in the cache database

#### Validation

The system validates that:
- ✅ Queue and cache databases are the same instance
- ✅ pg_cron database can be different from cache database
- ✅ All required environment variables are set
- ✅ dblink extension is available for cross-database queries
- ✅ Cross-database permissions are properly configured

#### Benefits

1. **Isolation**: pg_cron infrastructure separate from business data
2. **Scalability**: Single pg_cron can serve multiple work databases
3. **Security**: Different permission models for scheduling vs. execution
4. **Maintenance**: Easier to manage pg_cron without affecting business databases

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PostgreSQL Database                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────┐  │
│  │   pg_cron           │    │  Queue & Control    │    │  Cache Tables   │  │
│  │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────┐ │  │
│  │ │ cron.job        │ │───>│ │ query_fragment  │ │    │ │ {prefix}_   │ │  │
│  │ │ - jobname       │ │    │ │ _queue          │ │    │ │ cache_{key} │ │  │
│  │ │ - schedule      │ │    │ │ - id            │ │    │ │ - query_hash│ │  │
│  │ │ - command       │ │    │ │ - query         │ │    │ │ - value[]   │ │  │
│  │ └─────────────────┘ │    │ │ - hash          │ │    │ └─────────────┘ │  │
│  └─────────────────────┘    │ │ - partition_key │ │    └─────────────────┘  │
│                             │ │ - priority      │ │                         │
│                             │ └─────────────────┘ │                         │
│                             │                     │                         │
│                             │ ┌─────────────────┐ │    ┌─────────────────┐  │
│                             │ │ processor_      │ │    │ {prefix}_       │  │
│                             │ │ config          │ │    │ queries         │  │
│                             │ │ - enabled       │ │    │ - query_hash    │  │
│                             │ │ - max_jobs      │ │    │ - partition_key │  │
│                             │ │ - frequency     │ │    │ - query         │  │
│                             │ │ - last_seen     │ │    │ - last_seen     │  │
│                             │ └─────────────────┘ │    │ - last_seen     │  │
│                             │                     │    └─────────────────┘  │
│                             │ ┌─────────────────┐ │                         │
│                             │ │ processor_log   │ │    ┌─────────────────┐  │
│                             │ │ - job_id        │ │    │ {prefix}_       │  │
│                             │ │ - query_hash    │ │    │ partition_      │  │
│                             │ │ - status        │ │    │ metadata        │  │
│                             │ │ - error_msg     │ │    │ - partition_key │  │
│                             │ │ - exec_time_ms  │ │    │ - datatype      │  │
│                             │ └─────────────────┘ │    └─────────────────┘  │
│                             │                     │                         │
│                             │ ┌─────────────────┐ │                         │
│                             │ │ active_jobs     │ │                         │
│                             │ │ - query_hash    │ │                         │
│                             │ │ - partition_key │ │                         │
│                             │ │ - job_id        │ │                         │
│                             │ │ - started_at    │ │                         │
│                             │ └─────────────────┘ │                         │
│                             └─────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Function Call Hierarchy

```
pg_cron scheduler (every N seconds)
├── partitioncache_process_queue(table_prefix)
    ├── cleanup_stale_jobs()
    │   ├── DELETE stale jobs from active_jobs
    │   └── LOG timeout entries
    │
    ├── can_process_more_jobs()
    │   ├── CHECK processor_config.enabled
    │   └── COUNT active_jobs < max_parallel_jobs
    │
    └── FOR EACH available slot:
        └── process_queue_item(table_prefix)
            ├── SELECT queue item (FOR UPDATE SKIP LOCKED)
            ├── INSERT into active_jobs
            ├── LOG 'started' status
            ├── get_cache_table_name(prefix, partition_key)
            ├── EXECUTE user query → cache table
            ├── UPDATE queries table
            ├── DELETE from queue
            ├── DELETE from active_jobs
            └── LOG 'success'/'failed' status

Utility Functions:
├── get_processor_status()
├── set_processor_enabled(boolean)
├── update_processor_config(...)
└── manual_process_queue(count, prefix)
```

## Key Features

### Database-Native Processing
- **pg_cron Integration**: Utilizes PostgreSQL's `pg_cron` extension for reliable job scheduling
- **No External Dependencies**: Eliminates need for external Python processes or cron jobs
- **Atomic Operations**: All operations are database transactions ensuring consistency
- **Built-in Resilience**: Automatic recovery from failures and comprehensive error handling

### Automated Cache Backend Detection
- **Schema Inspection**: Automatically detects cache table structure to determine backend type
- **Array Backend Support**: Full support for PostgreSQL array-based cache tables
- **Future-Proof Design**: Extensible architecture for additional backend types
- **Seamless Integration**: Works with existing cache table structures

### Advanced Monitoring and Logging
- **Comprehensive Audit Trail**: Every operation is logged with execution timing
- **Job Status Tracking**: Real-time monitoring of active and completed jobs
- **Error Recovery**: Automatic cleanup of stale jobs and detailed error logging
- **Performance Metrics**: Millisecond-precision execution timing

### Intelligent Concurrency Control
- **Lock-Free Design**: Uses `SELECT FOR UPDATE SKIP LOCKED` for optimal concurrency
- **Parallel Processing**: Configurable number of parallel jobs
- **Deadlock Prevention**: Advanced locking strategy prevents database deadlocks
- **Resource Management**: Automatic cleanup of abandoned or timed-out jobs

## System Components

### Core Processing Functions

#### `partitioncache_run_single_job(p_job_name)`
Main processing function called by pg_cron workers:

```sql
SELECT partitioncache_run_single_job('partitioncache_process_queue_1');
```

**Function Flow:**
1. **Cleanup Phase**: Remove stale jobs that have exceeded timeout
2. **Capacity Check**: Verify system can accept more jobs based on configuration
3. **Job Processing**: Process available queue items up to configured limit
4. **Logging**: Record all operations and timing information

#### `process_queue_item(table_prefix)`
Processes individual queue items:

```sql
-- Internal function called by main processor
-- Handles single query execution and cache population
```

**Processing Steps:**
1. **Job Acquisition**: Use `SELECT FOR UPDATE SKIP LOCKED` to claim queue item
2. **Active Job Registration**: Insert record into active_jobs table
3. **Cache Backend Detection**: Inspect table schema to determine cache type
4. **Query Execution**: Execute user query against database
5. **Cache Population**: Store results in appropriate cache table
6. **Cleanup**: Remove processed item and update metadata

### Monitoring Functions

#### `partitioncache_get_processor_status(p_queue_prefix, p_job_name)`
Returns comprehensive processor status:

```sql
-- Use the base job name including the database suffix
SELECT * FROM partitioncache_get_processor_status('partitioncache_queue', 'partitioncache_process_queue_<db_name>');
```

**Returns:**
- **enabled**: Whether processor is currently enabled
- **max_parallel_jobs**: Maximum concurrent jobs allowed
- **frequency_seconds**: Processing frequency in seconds
- **active_jobs**: Number of currently running jobs
- **queue_length**: Number of items waiting in queue
- **last_seen**: Timestamp of last processor activity

#### `partitioncache_set_processor_enabled_cron(p_enabled, p_queue_prefix, p_job_name)`
Enable or disable the processor:

```sql
-- Enable processor
SELECT partitioncache_set_processor_enabled_cron(true, 'partitioncache_queue', 'partitioncache_process_queue_<db_name>');

-- Disable processor  
SELECT partitioncache_set_processor_enabled_cron(false, 'partitioncache_queue', 'partitioncache_process_queue_<db_name>');
```

### Configuration Management

#### `partitioncache_update_processor_config_cron(...)`
Update processor configuration:

```sql
-- Set max parallel jobs to 3, frequency to 5 seconds
SELECT partitioncache_update_processor_config_cron(
    'partitioncache_process_queue_<db_name>', 
    NULL, -- enabled (keep current)
    3,    -- max_parallel_jobs
    5,    -- frequency_seconds
    1800, -- timeout_seconds
    'partitioncache', -- table_prefix
    'partitioncache_queue' -- queue_prefix
);
```

#### `partitioncache_manual_process_queue(p_count)`
Manually process queue items:

```sql
-- Process up to 5 queue items immediately
SELECT partitioncache_manual_process_queue(5);
```

## Processing State Machine

```
┌─────────────┐      ┌──────────────┐     ┌─────────────┐
│ Query Added │────▶│ Queue Item   │────▶│ Available   │
│ to Queue    │      │ Created      │     │ for Process │
└─────────────┘      └──────────────┘     └─────────────┘
                                                │
                ┌─────────────────────────────────┘
                ▼
        ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
        │ Job Started │────▶│ Query        │────▶│ Cache       │
        │ (logged)    │     │ Executing    │     │ Updated     │
        └─────────────┘     └──────────────┘     └─────────────┘
                                  │                   │
                                  ▼                   │
                          ┌──────────────┐            │
                          │ Error/       │            │
                          │ Exception    │            │
                          └──────────────┘            │
                                  │                   │
                                  ▼                   ▼
                          ┌──────────────┐     ┌─────────────┐
                          │ Job Cleaned  │◀────│ Job Success │
                          │ (failed)     │     │ (completed) │
                          └──────────────┘     └─────────────┘
                                  │                   │
                                  ▼                   ▼
                          ┌──────────────┐     ┌─────────────┐
                          │ Logged as    │     │ Logged as   │
                          │ 'failed'     │     │ 'success'   │
                          └──────────────┘     └─────────────┘
```

## Installation and Setup

### Prerequisites

1. **PostgreSQL 12+** with `pg_cron` extension available (see [pg_cron installation](https://github.com/citusdata/pg_cron#installation))
2. **pg_cron Extension Configurateion**:
   ```
   shared_preload_libraries=pg_cron // Load pg_cron extension
   cron.database_name=${POSTGRES_DB} // Database name to use for pg_cron
   ```

3. **Permissions**: Database user needs:
   - CREATE/DROP TABLE permissions
   - INSERT/UPDATE/DELETE on all tables
   - EXECUTE permissions on functions

### Setup Process

#### 1. Initialize PostgreSQL Queue Processor

Using the CLI tool:

```bash
pcache-postgresql-queue-processor setup \
    --table-prefix partitioncache \
    --frequency 1 \
    --max-parallel-jobs 2 \
    --timeout 1800 \
    --enable-after-setup
```

## Configuration

### Environment Variables

The system automatically detects table prefixes from environment variables:

```bash
# For PostgreSQL Array cache backend
CACHE_BACKEND=postgresql_array
PG_ARRAY_CACHE_TABLE_PREFIX=my_custom_prefix

# For PostgreSQL Bit cache backend  
CACHE_BACKEND=postgresql_bit
PG_BIT_CACHE_TABLE_PREFIX=my_custom_prefix

# Default if not specified
# Uses 'partitioncache' as prefix
```


### Processor Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| **enabled** | `true` | Whether processor is active |
| **max_parallel_jobs** | `2` | Maximum concurrent jobs |
| **frequency_seconds** | `1` | Processing frequency |
| **job_timeout_minutes** | `5` | Timeout for individual jobs |

Update configuration:

```sql
-- Disable processor temporarily (optional)
SELECT set_processor_enabled(false);

-- Increase parallel processing
SELECT update_processor_config(5, 2);

-- Re-enable processor
SELECT set_processor_enabled(true);
```

## Concurrency Control

### Lock-Free Queue Processing

The system uses PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` pattern:

```sql
-- Multiple processors can run simultaneously
SELECT id, query, hash, partition_key 
FROM query_fragment_queue 
ORDER BY priority DESC, created_at ASC 
FOR UPDATE SKIP LOCKED 
LIMIT 1;
```

**Benefits:**
- **No Blocking**: Processes skip locked rows instead of waiting
- **High Concurrency**: Multiple processors can run simultaneously
- **Optimal Resource Usage**: No wasted CPU cycles on blocking
- **Automatic Load Distribution**: Work is naturally distributed

### Active Job Tracking

```sql
-- Register job start
INSERT INTO active_jobs (query_hash, partition_key, job_id, started_at)
VALUES (query_hash, partition_key, job_id, CURRENT_TIMESTAMP);

-- Job completion cleanup
DELETE FROM active_jobs 
WHERE query_hash = query_hash AND partition_key = partition_key;
```

### Stale Job Recovery

Automatic cleanup of jobs that exceed timeout:

```sql
-- Clean up jobs older than 5 minutes
DELETE FROM active_jobs 
WHERE started_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes';
```

## Cache Backend Detection

### Automatic Schema Inspection

The processor automatically detects cache backend type:

```sql
-- Inspect table structure
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = cache_table_name;

-- Determine backend type
IF has_column('value') AND data_type = 'ARRAY' THEN
    -- PostgreSQL Array backend
    INSERT INTO cache_table (query_hash, value) 
    VALUES (hash, ARRAY(SELECT DISTINCT result_column FROM query_results));
    
ELSIF has_column('partition_keys') AND data_type = 'BIT' THEN
    -- PostgreSQL Bit backend (future implementation)
    RAISE EXCEPTION 'Bit backend not yet supported in PostgreSQL queue processor';
    
ELSE
    RAISE EXCEPTION 'Unknown cache backend type for table %', cache_table_name;
END IF;
```

### Supported Backends

**Currently Supported:**
- ✅ **PostgreSQL Array**: Full support with automatic detection
- ✅ **PostgreSQL Bit**: Full support with automatic detection
- ✅ **PostgreSQL Roaring Bit**: Full support with automatic detection



## Monitoring and Logging

### Comprehensive Audit Trail

Every operation is logged with detailed information:

```sql
-- View recent processing activity
SELECT job_id, query_hash, partition_key, status, execution_time_ms, created_at
FROM partitioncache_processor_log 
ORDER BY created_at DESC 
LIMIT 10;

-- Performance analysis
SELECT 
    status,
    COUNT(*) as job_count,
    AVG(execution_time_ms) as avg_time_ms,
    MAX(execution_time_ms) as max_time_ms
FROM partitioncache_processor_log 
WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY status;
```

### Real-Time Status Monitoring

```sql
-- Get current system status
SELECT * FROM get_processor_status();

-- Monitor active jobs
SELECT 
    query_hash,
    partition_key, 
    job_id,
    started_at,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) as duration_seconds
FROM partitioncache_active_jobs;

-- Check queue backlog
SELECT 
    partition_key,
    priority,
    COUNT(*) as queue_items
FROM query_fragment_queue 
GROUP BY partition_key, priority 
ORDER BY priority DESC;
```

### Error Analysis

```sql
-- Recent errors
SELECT query_hash, partition_key, error_msg, created_at
FROM partitioncache_processor_log 
WHERE status = 'failed' 
AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
ORDER BY created_at DESC;

-- Error patterns
SELECT 
    SUBSTRING(error_msg FROM 1 FOR 50) as error_type,
    COUNT(*) as frequency
FROM partitioncache_processor_log 
WHERE status = 'failed'
GROUP BY SUBSTRING(error_msg FROM 1 FOR 50)
ORDER BY frequency DESC;
```

## Performance Optimization

### Frequency Tuning

Choose processing frequency based on workload:

```sql
-- High-frequency for real-time processing
SELECT update_processor_config(2, 1);  -- 1 second intervals

-- Medium-frequency for balanced load
SELECT update_processor_config(3, 5);  -- 5 second intervals

-- Low-frequency for batch processing  
SELECT update_processor_config(5, 30); -- 30 second intervals
```

### Parallel Job Configuration

Balance parallelism with system resources:

```sql
-- Conservative (low-resource systems)
SELECT update_processor_config(1, 2);

-- Moderate (typical production)
SELECT update_processor_config(3, 1);

-- Aggressive (high-performance systems)
SELECT update_processor_config(8, 1);
```

### Index Optimization

Ensure optimal database performance:

```sql
-- Queue processing indexes
CREATE INDEX IF NOT EXISTS idx_query_fragment_queue_priority 
ON query_fragment_queue (priority DESC, created_at ASC);

-- Monitoring indexes
CREATE INDEX IF NOT EXISTS idx_processor_log_created_at 
ON partitioncache_processor_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_processor_log_status 
ON partitioncache_processor_log (status, created_at DESC);
```

## Data Flow Architecture

```
External Application
        │
        ▼
┌─────────────────┐
│ pcache-add      │
│ --queue         │
│ --query "..."   │
│ --partition-key │
└─────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│                PostgreSQL Database                     │
│                                                         │
│  ┌─────────────────┐    ┌─────────────────┐           │
│  │ query_fragment  │    │ Direct          │           │
│  │ _queue          │───▶│ Processor       │           │
│  │                 │    │ Functions       │           │
│  │ ┌─────────────┐ │    │                 │           │
│  │ │ NEW QUERY   │ │    │ ┌─────────────┐ │           │
│  │ │ ITEM        │ │    │ │ AUTOMATIC   │ │           │
│  │ └─────────────┘ │    │ │ PROCESSING  │ │           │
│  └─────────────────┘    │ └─────────────┘ │           │
│                         │        │        │           │
│                         └────────┼────────┘           │
│                                  ▼                    │
│  ┌─────────────────┐    ┌─────────────────┐           │
│  │ Cache Tables    │◀───│ Query Execution │           │
│  │                 │    │ & Result        │           │
│  │ ┌─────────────┐ │    │ Storage         │           │
│  │ │ CACHED      │ │    │                 │           │
│  │ │ RESULTS     │ │    └─────────────────┘           │
│  │ └─────────────┘ │                                  │
│  └─────────────────┘                                  │
│                                                        │
│  ┌─────────────────┐    ┌─────────────────┐           │
│  │ Monitoring &    │    │ pg_cron         │           │
│  │ Logging         │    │ Scheduler       │           │
│  │                 │    │                 │           │
│  │ ┌─────────────┐ │    │ ┌─────────────┐ │           │
│  │ │ EXECUTION   │ │    │ │ TRIGGERS    │ │           │
│  │ │ HISTORY     │ │    │ │ EVERY 1s    │ │           │
│  │ └─────────────┘ │    │ └─────────────┘ │           │
│  └─────────────────┘    └─────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

## Usage Examples

### Basic Workflow

```bash
# 1. Add query to processing queue
pcache-add \
    --queue \
    --query "SELECT DISTINCT city_id FROM pois WHERE type='restaurant'" \
    --partition-key "city_id"

# 2. Monitor processing 
pcache-postgresql-queue-processor status

# 3. View processing logs
pcache-postgresql-queue-processor logs
```

### Advanced Configuration

```sql
-- High-performance setup for busy system
SELECT update_processor_config(5, 1);  -- 5 parallel jobs, 1-second intervals

-- Monitor performance
SELECT 
    AVG(execution_time_ms) as avg_ms,
    MAX(execution_time_ms) as max_ms,
    COUNT(*) as total_jobs
FROM partitioncache_processor_log 
WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
AND status = 'success';

-- Check for errors
SELECT COUNT(*) as error_count
FROM partitioncache_processor_log 
WHERE status = 'failed'
AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour';
```

### Maintenance Operations (SQL)

```sql
-- Pause processing for maintenance
SELECT set_processor_enabled(false);

-- Clear old logs (keep last 7 days)
DELETE FROM partitioncache_processor_log 
WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '7 days';

-- Manually process pending items
SELECT manual_process_queue(10, 'partitioncache');

-- Resume processing
SELECT set_processor_enabled(true);
```

## Troubleshooting

### Common Issues

**1. Processor Not Running**
```sql
-- Check pg_cron job status
SELECT jobname, schedule, active, last_run_status 
FROM cron.job 
WHERE jobname = 'partitioncache_process_queue';

-- Check if processor is enabled
SELECT * FROM get_processor_status();
```

**2. Jobs Timing Out**
```sql
-- Check for stale jobs
SELECT * FROM partitioncache_active_jobs 
WHERE started_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes';

-- Clean up manually if needed
DELETE FROM partitioncache_active_jobs 
WHERE started_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes';
```

**3. Cache Backend Detection Issues**
```sql
-- Check if cache tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_name LIKE 'partitioncache_cache_%';

-- Verify table structure
\d partitioncache_cache_partition_key
```

### Debug Mode

Enable detailed logging:

```sql
-- Check recent activity
SELECT job_id, status, execution_time_ms, error_msg
FROM partitioncache_processor_log 
ORDER BY created_at DESC 
LIMIT 20;

-- Monitor queue processing
SELECT 
    COUNT(*) as queue_length,
    MAX(created_at) as newest_item,
    MIN(created_at) as oldest_item
FROM query_fragment_queue;
```

### Performance Issues

**High CPU Usage:**
```sql
-- Reduce frequency and parallelism
SELECT update_processor_config(1, 5);
```

**Slow Processing:**
```sql
-- Check for long-running queries
SELECT query_hash, partition_key, error_msg
FROM partitioncache_processor_log 
WHERE status = 'failed' 
AND error_msg LIKE '%timeout%';

-- Increase timeout if needed (modify configuration)
```

**Memory Issues:**
```sql
-- Reduce parallel jobs
SELECT update_processor_config(1, 2);

-- Monitor query complexity
SELECT 
    LENGTH(query) as query_length,
    execution_time_ms
FROM partitioncache_processor_log 
WHERE status = 'success'
ORDER BY execution_time_ms DESC 
LIMIT 10;
```

## Migration from External Processing

### From Python Observer Scripts

**Before (External Python):**
```bash
# Manual monitoring process
pcache-monitor \
    --db-backend postgresql \
    --cache-backend postgresql_array
```

**After (PostgreSQL Queue Processor):**
```bash
pcache-postgresql-queue-processor setup
pcache-postgresql-queue-processor enable
```

## Best Practices

### Configuration Guidelines

1. **Start Conservative**: Begin with 1-2 parallel jobs and 5-second frequency
2. **Monitor Performance**: Use built-in logging to track execution times
3. **Scale Gradually**: Increase parallelism based on system capacity
4. **Plan for Peaks**: Configure for typical load, not peak load

### Operational Best Practices

1. **Regular Monitoring**: Check processor status and logs daily
2. **Log Retention**: Clean up old logs weekly to prevent table growth
3. **Backup Strategy**: Include processor tables in database backups
4. **Alerting**: Monitor for failed jobs and system errors
5. **Capacity Planning**: Track queue lengths and processing rates

### Security Considerations

1. **Database Permissions**: Grant minimal required permissions
2. **SQL Injection**: All queries are parameterized and validated
3. **Resource Limits**: Configure appropriate timeouts and job limits
4. **Access Control**: Restrict access to processor management functions
5. **Ensure Trusted Code**: Only use trusted code and trusted environments, as malicious SQL could be executed if provided in the queue

This comprehensive PostgreSQL queue processor implementation provides a robust, scalable, and maintainable solution for automated partition cache management entirely within PostgreSQL. 


# Misc


## Concurrency Control Flow

```
Multiple pg_cron calls (frequency=1s)
        │
        ▼
┌───────────────────┐
│ Each Call:        │
│ cleanup_stale()   │───┐
│ can_process()?    │   │ Parallel execution prevented by:
│ process_items()   │   │ • SELECT FOR UPDATE SKIP LOCKED
└───────────────────┘   │ • active_jobs table constraints
        │               │ • max_parallel_jobs limit
        ▼               │
┌───────────────────┐   │
│ SELECT queue item │◀──┘
│ FOR UPDATE        │
│ SKIP LOCKED       │
└───────────────────┘
        │
        ▼
┌───────────────────┐    ┌─────────────────┐
│ INSERT active_job │───▶│ Other processes │
│ (query_hash,      │    │ skip this item  │
│  partition_key)   │    │ automatically   │
└───────────────────┘    └─────────────────┘
        │
        ▼
┌───────────────────┐
│ Execute query &   │
│ update cache      │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ DELETE active_job │
│ (frees slot)      │
└───────────────────┘
```

## Cache Backend Detection Logic

```
process_queue_item()
        │
        ▼
┌─────────────────────────────────────────┐
│ Inspect cache table schema:             │
│                                         │
│ SELECT column_name                      │
│ FROM information_schema.columns         │
│ WHERE table_name = cache_table          │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────┐    ┌─────────────────┐
│ Has 'value'     │    │ Has 'partition_ │
│ column?         │    │ keys' column?   │
│                 │    │                 │
│ YES: Array      │    │ YES: Bit        │
│ Backend         │    │ Backend         │
└─────────────────┘    └─────────────────┘
        │                        │
        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐
│ INSERT INTO     │    │ [Not yet        │
│ cache_table     │    │ implemented]    │
│ (query_hash,    │    │                 │
│  ARRAY(query))  │    │ RAISE EXCEPTION │
└─────────────────┘    └─────────────────┘
```

## Error Handling & Recovery

```
┌─────────────────┐
│ Job Starts      │
│ (INSERT active) │
│ (LOG started)   │
└─────────────────┘
        │
        ▼
┌─────────────────┐    ┌─────────────────┐
│ Query Execution │───▶│ SUCCESS         │
│                 │    │ - Update cache  │
│                 │    │ - Update queries│
│                 │    │ - Delete queue  │
│                 │    │ - Delete active │
│                 │    │ - LOG success   │
└─────────────────┘    └─────────────────┘
        │
        ▼
┌─────────────────┐    ┌─────────────────┐
│ EXCEPTION       │───▶│ RECOVERY        │
│ - SQL Error     │    │ - Delete active │
│ - Timeout       │    │ - LOG failed    │
│ - System Error  │    │ - Keep in queue │
└─────────────────┘    └─────────────────┘
        │
        ▼
┌─────────────────┐
│ STALE CLEANUP   │
│ (after 5 min)   │
│ - Delete active │
│ - LOG timeout   │
└─────────────────┘
```

## pg_cron Job Configuration

```
Modern pg_cron 1.5+ Syntax:

┌─────────────────────────────────────────┐
│ INSERT INTO cron.job (                 │
│     jobname,                           │
│     schedule,                          │
│     command                            │
│ ) VALUES (                             │
│     'partitioncache_process_queue',    │
│     '1 seconds',  ← Modern syntax      │
│     'SELECT partitioncache_process...' │
│ );                                     │
└─────────────────────────────────────────┘

Frequency Options:
├── '1 seconds'  ← High frequency
├── '5 seconds'  ← Medium frequency  
├── '30 seconds' ← Low frequency
└── '* * * * *'  ← Traditional cron (minutes)

Job Execution Flow:
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ pg_cron     │─▶│ SQL Command │─▶│ Function    │
│ scheduler   │  │ executes    │  │ processes   │
│ triggers    │  │ in database │  │ queue items │
└─────────────┘  └─────────────┘  └─────────────┘
```



## Parallel Processing Architecture

To prevent long-running queries from blocking the entire queue (head-of-line blocking), the direct processor uses a parallel architecture powered by multiple `pg_cron` jobs.

### Key Components

1.  **Worker Entrypoint (`partitioncache_run_single_job`)**: This is the main function executed by each cron worker. Its job is to dequeue and process exactly one item from the queue.

2.  **Parallel Cron Jobs**: Instead of a single cron job, the system creates multiple, independent jobs (e.g., `partitioncache_process_queue_1`, `partitioncache_process_queue_2`, etc.), up to the `max_parallel_jobs` limit. `pg_cron` ensures that these jobs can run concurrently, providing true parallelism. Because each worker runs in its own session, a slow query for one partition will not delay the processing of other items.

3.  **Active Jobs Table (`..._active_jobs`)**: This table tracks which jobs are currently being processed. This is crucial for preventing the same query (for the same partition) from being processed by multiple workers simultaneously and for monitoring purposes.

4.  **Timeout & Cleanup Job**: A separate, dedicated cron job (`..._timeouts`) runs on the same schedule. Its sole responsibility is to find and clean up any jobs that have been running for too long, preventing orphaned jobs from blocking the system.

### Requirements

-   **`pg_cron`**: Must be installed and enabled in your PostgreSQL instance. This is the only extension required for scheduling and parallelism.

### Installation

The provided `Dockerfile` in the `examples/openstreetmap_poi/` directory includes the necessary steps to install `pg_cron`.

```dockerfile
# The Dockerfile installs pg_cron and other necessary build tools.
# Example:
RUN apt-get update && apt-get install -y postgresql-16-cron
```

When you run the `pcache-postgresql-queue-processor setup` command, it creates all necessary tables and a trigger on the configuration table. This trigger automatically creates and manages the multiple `pg_cron` jobs, abstracting the scheduling logic away from the user.

## How it Works

1.  **Setup**: Running `pcache-postgresql-queue-processor setup` creates:
    -   A central configuration table (`..._processor_config`).
    -   A trigger on this table that automatically creates:
        -   `N` worker jobs in `cron.job`, where `N` is `max_parallel_jobs`.
        -   One dedicated timeout/cleanup job in `cron.job`.
2.  **Execution**:
    -   Every `frequency_seconds`, `pg_cron` attempts to start all `N+1` jobs.
    -   Each worker job calls `partitioncache_run_single_job`, which tries to pull one item from the queue and process it.
    -   The timeout job calls `partitioncache_handle_timeouts` to clean up any stuck workers.
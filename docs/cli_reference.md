# PartitionCache CLI Reference Guide

This comprehensive reference covers all CLI tools and their complete parameter sets.

## Overview

PartitionCache provides six main CLI tools:

| Tool | Purpose | Key Features |
|------|---------|--------------|
| `pcache-manage` | Central management tool | Setup, status, cache operations, maintenance |
| `pcache-add` | Add queries to cache | Direct execution or queue-based processing |
| `pcache-read` | Read cached partition keys | Query cache with flexible output formats |
| `pcache-monitor` | Monitor and process queues | Multi-threaded queue processing with optimization |
| `pcache-postgresql-queue-processor` | PostgreSQL-native queue processor | pg_cron integration, database-native processing |
| `pcache-postgresql-eviction-manager` | PostgreSQL cache eviction | Automatic cache cleanup and management |

---

## pcache-manage

**Central management tool for PartitionCache operations**

### Basic Usage
```bash
pcache-manage [--env ENV_FILE] {setup,status,cache,queue,maintenance} ...
```

### Global Options
- `--env ENV_FILE` - Environment file path (default: `.env`)
  - Loads database credentials and configuration
  - Supports custom environment files for different deployments

### Subcommands

#### setup - Setup PartitionCache Infrastructure
```bash
pcache-manage setup {all,queue,cache}
```

**Subcommands:**
- `all` - Set up all tables (recommended for new projects)
  - Creates cache metadata tables
  - Creates queue tables
  - Sets up proper indexes and constraints
- `queue` - Set up queue tables only
  - Creates `original_query_queue` and `query_fragment_queue`
  - Sets up queue-related indexes
- `cache` - Set up cache metadata tables only
  - Creates partition metadata tables
  - Sets up cache-related indexes

#### status - Check System Status
```bash
pcache-manage status [{all,env,tables}]
```

**Subcommands:**
- *(default/no subcommand)* - Show comprehensive status overview
  - Displays complete system status including queue and cache statistics
  - Shows system health checks and recommendations
  - Same as `status all`
- `all` - Show comprehensive status overview (explicit)
  - Configuration details (cache backend, queue provider)
  - Queue statistics (original and fragment queue counts)
  - Cache statistics (total entries, partitions, top partitions by size)
  - System health checks (pg_cron status, eviction manager status)
- `env` - Validate environment configuration only
  - Checks all required environment variables
  - Tests database connectivity
  - Validates cache backend configuration
- `tables` - Check table status and accessibility only
  - Verifies table existence
  - Checks table permissions
  - Reports table statistics

#### cache - Cache Operations
```bash
pcache-manage cache {count,overview,copy,export,import,delete} [options]
```

**Subcommands:**

**`count` - Count cache entries**
```bash
pcache-manage cache count [--type CACHE_TYPE] [--all]
```
- `--type CACHE_TYPE` - Specific cache type (default: from `CACHE_BACKEND` env var)
- `--all` - Count all cache types simultaneously

**`overview` - Show detailed partition overview**
```bash
pcache-manage cache overview
```
- Displays partition keys, datatypes, and entry counts
- Shows cache utilization statistics

**`copy` - Copy cache between backends**
```bash
pcache-manage cache copy --from SOURCE_TYPE --to TARGET_TYPE
```
- Supports copying between any compatible cache backends
- Preserves partition key mappings and datatypes

**`export` - Export cache to file**
```bash
pcache-manage cache export [--type CACHE_TYPE] --file ARCHIVE_FILE
```
- `--type CACHE_TYPE` - Cache type to export (default: from env)
- `--file ARCHIVE_FILE` - Output archive file path (pickle format)

**`import` - Import cache from file**
```bash
pcache-manage cache import [--type CACHE_TYPE] --file ARCHIVE_FILE
```
- `--type CACHE_TYPE` - Target cache type (default: from env)
- `--file ARCHIVE_FILE` - Input archive file path

**`delete` - Delete cache**
```bash
pcache-manage cache delete [--type CACHE_TYPE] [--partition PARTITION_KEY]
```
- `--type CACHE_TYPE` - Cache type to delete from
- `--partition PARTITION_KEY` - Delete specific partition only

#### queue - Queue Operations
```bash
pcache-manage queue {count,clear}
```

**Subcommands:**
- `count` - Count queue entries
  - Shows original and fragment queue depths
  - Displays processing statistics
- `clear` - Clear queue entries
  - Removes all pending queue items
  - Useful for resetting during development

#### maintenance - Maintenance Operations
```bash
pcache-manage maintenance {prune,evict,cleanup,partition} [options]
```

**Subcommands:**

**`prune` - Remove old queries**
```bash
pcache-manage maintenance prune [--days DAYS] [--type CACHE_TYPE]
```
- `--days DAYS` - Remove queries older than specified days
- `--type CACHE_TYPE` - Specific cache type to prune (default: all)

**`evict` - Evict cache entries based on strategy**
```bash
pcache-manage maintenance evict [--strategy STRATEGY] [--type CACHE_TYPE]
```
- Uses configurable eviction strategies (LRU, size-based, etc.)

**`cleanup` - Clean up cache entries**
```bash
pcache-manage maintenance cleanup [--remove-termination] [--remove-large SIZE]
```
- `--remove-termination` - Remove terminated/failed entries
- `--remove-large SIZE` - Remove entries larger than specified size

**`partition` - Partition management**
```bash
pcache-manage maintenance partition --delete PARTITION_KEY
```
- `--delete PARTITION_KEY` - Delete entire partition and all its data

---

## pcache-add

**Add queries to cache via direct execution or queue processing**

### Usage
```bash
pcache-add [options] --partition-key PARTITION_KEY {--direct|--queue|--queue-original}
```

### Query Input Options (Mutually Exclusive)
- `--query QUERY` - SQL query string to cache
- `--query-file QUERY_FILE` - Path to file containing SQL query

### Query Processing Options
- `--no-recompose` - Add query as-is without decomposition
  - **Default behavior**: Decomposes queries into subquery fragments
  - **With flag**: Adds the complete query directly to cache/queue
  - **Use case**: When you want to cache the exact query without optimization

### Partition Configuration (Required)
- `--partition-key PARTITION_KEY` - Name of the partition key column (**Required**)
- `--partition-datatype {integer,float,text,timestamp}` - Partition key datatype
  - **Default**: Inferred from query analysis
  - **When to specify**: For explicit type validation or when inference fails

### Execution Mode (Mutually Exclusive - Required)

#### Direct Execution
```bash
--direct
```
- Executes query immediately and stores results in cache
- **Best for**: Development, testing, small queries
- **Warning**: Can be slow for complex queries with many fragments

#### Queue Processing
```bash
--queue
```
- Adds query fragments to `query_fragment_queue` for async processing
- **Best for**: Production workloads, complex queries
- **Processing**: Requires queue processor to be running

#### Original Queue Processing
```bash
--queue-original
```
- Adds complete query to `original_query_queue`
- **Best for**: When you want the queue processor to handle decomposition
- **Processing**: Queue processor will fragment and process automatically

### Backend Configuration
- `--queue-provider QUEUE_PROVIDER` - Queue provider (`postgresql`, `redis`)
  - **Default**: `postgresql`
- `--cache-backend CACHE_BACKEND` - Cache backend type
  - **Default**: `postgresql_bit`
  - **Options**: `postgresql_array`, `postgresql_bit`, `redis_set`, `redis_bit`, etc.
- `--db-backend DB_BACKEND` - Database backend
  - **Default**: `postgresql`
  - **Options**: `postgresql`, `mysql`, `sqlite`

### Database Configuration
- `--db-name DB_NAME` - Target database name
  - **Default**: Read from environment (`DB_NAME`)
  - **Required**: Must be specified via parameter or environment
- `--env ENV` - Environment file path
  - **Default**: `.env`
  - **Purpose**: Loads database credentials and configuration

### Examples

**Direct cache population:**
```bash
pcache-add --direct \
  --query "SELECT DISTINCT city_id FROM restaurants WHERE rating > 4.0" \
  --partition-key "city_id" \
  --partition-datatype "integer" \
  --cache-backend "postgresql_array"
```

**Queue-based processing:**
```bash
pcache-add --queue-original \
  --query-file "complex_analytics_query.sql" \
  --partition-key "region_id" \
  --partition-datatype "text"
```

**No decomposition (exact query caching):**
```bash
pcache-add --queue --no-recompose \
  --query "SELECT * FROM users WHERE city_id IN (1,2,3)" \
  --partition-key "city_id"
```

---

## pcache-read

**Read partition keys from cache for a given query**

### Usage
```bash
pcache-read --query QUERY [options]
```

### Required Parameters
- `--query QUERY` - SQL query to look up in cache (**Required**)

### Cache Configuration
- `--cache-backend CACHE_BACKEND` - Cache backend to query
  - **Default**: From `CACHE_BACKEND` environment variable
- `--partition-key PARTITION_KEY` - Partition key column name
  - **Default**: From cache metadata or `partition_key`
- `--partition-datatype {integer,float,text,timestamp}` - Partition key datatype
  - **Default**: `integer`

### Output Options
- `--output-format {list,json,lines}` - Output format for partition keys
  - **`list`**: Comma-separated values (e.g., `1,2,3,4,5`)
  - **`json`**: JSON array format (e.g., `[1,2,3,4,5]`)
  - **`lines`**: One partition key per line
- `--output-file OUTPUT_FILE` - Write output to file instead of console
  - **Default**: Print to stdout

### Configuration
- `--env-file ENV_FILE` - Environment file with cache configuration
  - **Default**: Uses environment variables

### Examples

**Basic cache lookup:**
```bash
pcache-read \
  --query "SELECT * FROM restaurants WHERE rating > 4.0" \
  --partition-key "city_id" \
  --partition-datatype "integer"
```

**JSON output to file:**
```bash
pcache-read \
  --query "SELECT * FROM events WHERE event_type = 'click'" \
  --partition-key "user_id" \
  --output-format json \
  --output-file "cached_users.json"
```

**Using custom cache backend:**
```bash
pcache-read \
  --query "SELECT * FROM products WHERE category = 'electronics'" \
  --cache-backend "redis_set" \
  --partition-key "store_id" \
  --partition-datatype "text" \
  --output-format lines
```

---

## pcache-monitor

**Monitor and process cache queues with multi-threaded execution**

### Usage
```bash
pcache-monitor [options]
```

### Backend Configuration
- `--db-backend {postgresql,mysql,sqlite}` - Database backend type
  - **Default**: `postgresql`
- `--cache-backend CACHE_BACKEND` - Cache backend type
  - **Default**: From environment
- `--db-name DB_NAME` - Database name to use
  - **Default**: From environment (`DB_NAME`)
- `--db-dir DB_DIR` - Database directory (for SQLite)

### Processing Configuration
- `--max-processes MAX_PROCESSES` - Maximum number of worker processes
  - **Default**: CPU count
  - **Use case**: Control resource usage and concurrency
- `--long-running-query-timeout LONG_RUNNING_QUERY_TIMEOUT` - Query timeout in seconds
  - **Default**: 300 seconds
  - **Purpose**: Prevent hanging queries from blocking processing

### Output Control
- `--limit LIMIT` - Limit number of returned partition keys per query
  - **Purpose**: Control memory usage for large result sets
- `--status-log-interval STATUS_LOG_INTERVAL` - Status logging interval when idle
  - **Default**: 10 seconds
  - **Purpose**: Reduce log noise during quiet periods

### Performance Optimization
- `--disable-optimized-polling` - Disable optimized polling and use simple polling
  - **Default**: Optimized polling enabled
  - **Optimized**: Uses LISTEN/NOTIFY for PostgreSQL, native blocking for Redis
  - **Simple**: Uses regular polling with sleep intervals
  - **Use case**: Troubleshooting or compatibility issues

### Configuration
- `--env ENV` - Environment file path
  - **Default**: `.env`
- `--close` - Close cache connections after operation
  - **Purpose**: Ensure clean shutdown

### Examples

**Basic queue monitoring:**
```bash
pcache-monitor \
  --cache-backend "postgresql_array" \
  --max-processes 4
```

**High-performance setup:**
```bash
pcache-monitor \
  --cache-backend "redis_set" \
  --max-processes 8 \
  --long-running-query-timeout 600 \
  --status-log-interval 30
```

**Development/debugging setup:**
```bash
pcache-monitor \
  --cache-backend "postgresql_array" \
  --max-processes 1 \
  --disable-optimized-polling \
  --status-log-interval 5
```

---

## pcache-postgresql-queue-processor

**PostgreSQL-native queue processor with pg_cron integration**

### Usage
```bash
pcache-postgresql-queue-processor [--env ENV] {command} [options]
```

### Global Options
- `--env ENV` - Environment file path
  - **Default**: `.env`

### Commands

#### setup - Initial Setup
```bash
pcache-postgresql-queue-processor setup
```
- Creates database objects (functions, tables, procedures)
- Sets up pg_cron job configuration
- **Prerequisites**: pg_cron extension must be installed

#### remove - Complete Removal
```bash
pcache-postgresql-queue-processor remove
```
- Removes pg_cron job
- Drops all processor-related database objects
- **Use case**: Clean uninstall

#### enable - Enable Processing
```bash
pcache-postgresql-queue-processor enable
```
- Activates the pg_cron job
- Starts automatic queue processing
- **Default schedule**: Every second

#### disable - Disable Processing
```bash
pcache-postgresql-queue-processor disable
```
- Deactivates the pg_cron job
- Stops automatic processing (manual processing still possible)

#### update-config - Update Configuration
```bash
pcache-postgresql-queue-processor update-config [--frequency SECONDS] [--max-jobs COUNT]
```
- `--frequency SECONDS` - Processing frequency (1-59 seconds)
- `--max-jobs COUNT` - Maximum concurrent jobs
- **Purpose**: Tune performance for your workload

#### status - Basic Status
```bash
pcache-postgresql-queue-processor status
```
- Shows job status (enabled/disabled)
- Reports basic queue statistics

#### status-detailed - Detailed Status
```bash
pcache-postgresql-queue-processor status-detailed
```
- Comprehensive status report
- Queue depths, processing rates, error statistics
- Recent job execution history

#### queue-info - Queue and Cache Information
```bash
pcache-postgresql-queue-processor queue-info
```
- Detailed information per partition key
- Cache hit rates and statistics
- Queue processing metrics

#### logs - View Processing Logs
```bash
pcache-postgresql-queue-processor logs [--limit LIMIT] [--status STATUS]
```
- `--limit LIMIT` (or `-n LIMIT`) - Number of log entries
  - **Default**: 50
- `--status STATUS` - Filter by status (`success`, `failed`, etc.)

#### manual-process - Manual Processing
```bash
pcache-postgresql-queue-processor manual-process
```
- Manually trigger one processing cycle
- **Use case**: Testing, development, troubleshooting

### Examples

**Complete setup for production:**
```bash
# Initial setup
pcache-postgresql-queue-processor setup

# Configure for high-throughput
pcache-postgresql-queue-processor update-config --frequency 1 --max-jobs 10

# Enable processing
pcache-postgresql-queue-processor enable

# Monitor status
pcache-postgresql-queue-processor status-detailed
```

**Development and testing:**
```bash
# Setup without auto-processing
pcache-postgresql-queue-processor setup

# Manual processing for testing
pcache-postgresql-queue-processor manual-process

# Check logs for issues
pcache-postgresql-queue-processor logs --limit 20 --status failed
```

---

## pcache-postgresql-eviction-manager

**PostgreSQL cache eviction management with pg_cron integration**

### Usage
```bash
pcache-postgresql-eviction-manager [--env ENV] [--table-prefix PREFIX] {command}
```

### Global Options
- `--env ENV` - Environment file path
  - **Default**: `.env`
- `--table-prefix TABLE_PREFIX` - Cache table prefix
  - **Default**: From environment variables
  - **Purpose**: Support multiple cache instances

### Commands

#### setup - Setup Eviction System
```bash
pcache-postgresql-eviction-manager setup
```
- Creates eviction procedures and tables
- Sets up pg_cron job for automatic eviction
- **Prerequisites**: pg_cron extension

#### remove - Remove Eviction System
```bash
pcache-postgresql-eviction-manager remove
```
- Removes pg_cron job
- Drops eviction-related database objects

#### enable - Enable Automatic Eviction
```bash
pcache-postgresql-eviction-manager enable
```
- Activates automatic cache cleanup
- **Default schedule**: Configurable interval

#### disable - Disable Automatic Eviction
```bash
pcache-postgresql-eviction-manager disable
```
- Stops automatic eviction
- Manual eviction still available

#### update-config - Update Eviction Configuration
```bash
pcache-postgresql-eviction-manager update-config [options]
```
- Configure eviction policies and schedules
- **Options**: TTL, size limits, frequency

#### status - Eviction Status
```bash
pcache-postgresql-eviction-manager status
```
- Shows eviction job status
- Reports recent eviction statistics

#### logs - View Eviction Logs
```bash
pcache-postgresql-eviction-manager logs [--limit LIMIT]
```
- `--limit LIMIT` - Number of log entries to show

#### manual-run - Manual Eviction
```bash
pcache-postgresql-eviction-manager manual-run
```
- Manually trigger eviction process
- **Use case**: Testing eviction policies

### Examples

**Production eviction setup:**
```bash
pcache-postgresql-eviction-manager setup
pcache-postgresql-eviction-manager enable
pcache-postgresql-eviction-manager status
```

**Multiple cache instances:**
```bash
pcache-postgresql-eviction-manager --table-prefix "app1_cache" setup
pcache-postgresql-eviction-manager --table-prefix "app2_cache" setup
```

---

## Best Practices

### Environment Management
- Use separate `.env` files for different environments (dev, staging, prod)
- Test environment configuration with `pcache-manage status env`
- Use `--env` parameter to switch between configurations

### Queue Processing
- **Development**: Use `pcache-monitor` for flexibility and debugging
- **Production**: Use `pcache-postgresql-queue-processor` for reliability and performance
- Monitor queue depths regularly to prevent backlogs

### Cache Management
- Set up regular maintenance with `pcache-manage maintenance prune`
- Use `pcache-manage cache overview` to monitor cache utilization
- Export cache before major changes: `pcache-manage cache export`

### Performance Optimization
- Tune `--max-processes` based on your database capacity
- Use `--disable-optimized-polling` only for troubleshooting
- Monitor long-running queries with appropriate timeouts

### Error Handling
- Regularly check logs: `pcache-postgresql-queue-processor logs --status failed`
- Use manual processing for debugging: `manual-process`
- Keep backups of working configurations

This comprehensive reference ensures you can effectively use all PartitionCache CLI features with full understanding of each parameter's purpose and impact.
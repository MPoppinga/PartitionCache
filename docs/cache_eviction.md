# Cache Eviction

PartitionCache provides comprehensive cache eviction capabilities to manage cache size and optimize performance. There are two main approaches: automatic eviction via PostgreSQL scheduling and manual eviction via CLI commands.

## Overview

Cache eviction helps prevent unbounded cache growth by removing entries based on configurable strategies:

- **Oldest Strategy**: Remove entries with the oldest `last_seen` timestamps
- **Largest Strategy**: Remove entries with the highest partition key counts

## PostgreSQL Eviction Manager (`pcache-postgresql-eviction-manager`)

The PostgreSQL Eviction Manager provides automatic, scheduled cache eviction using `pg_cron` for hands-off cache management.

### Features

- **Automatic Scheduling**: Uses `pg_cron` for reliable, scheduled eviction
- **Multiple Strategies**: Supports both `oldest` and `largest` eviction strategies
- **Independent Operation**: Operates independently of the queue processor
- **Comprehensive Logging**: Tracks all eviction activities with detailed logs
- **Configuration Management**: Database-stored configuration with hot updates
- **Cross-Database Support**: Works with pg_cron in separate database from cache data

### Cross-Database Architecture Support

The eviction manager supports cross-database pg_cron configurations where:
- **pg_cron** is installed in a central database (typically `postgres`)
- **Cache databases** contain PartitionCache tables and eviction logic
- **Jobs execute** in cache databases via `cron.schedule_in_database()`

This architecture provides better isolation, security, and scalability by separating scheduling infrastructure from business data.

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
│  │ │  pg_cron    │ │ database()         │ │  Cache Tables       │ │    │
│  │ │  Extension  │ │ ─────────────────→ │ │  Eviction Logic     │ │    │
│  │ │  Eviction   │ │                    │ │  Log Tables         │ │    │
│  │ │  Config     │ │                    │ │  Worker Functions   │ │    │
│  │ └─────────────┘ │                    │ └─────────────────────┘ │    │
│  └─────────────────┘                    └─────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Prerequisites

- PostgreSQL database with `pg_cron` extension
- PostgreSQL cache backend (array, bit, or roaringbit)
- Appropriate database permissions for creating functions and cron jobs
- For cross-database setups: permissions to use `cron.schedule_in_database()`

### Quick Start

```bash
# Setup eviction manager for a cache with table prefix 'my_cache'
pcache-postgresql-eviction-manager setup --table-prefix my_cache

# Enable automatic eviction
pcache-postgresql-eviction-manager enable --table-prefix my_cache

# Check status
pcache-postgresql-eviction-manager status --table-prefix my_cache
```

### Configuration

The eviction manager uses environment variables for database connection:

```bash
# Cache Database Configuration (where cache tables and eviction logic live)
DB_HOST=localhost
DB_PORT=5432
DB_USER=cache_user
DB_PASSWORD=cache_password
DB_NAME=cache_database

# pg_cron Database Configuration (can be different from cache database)
PG_CRON_HOST=localhost                   # Default: same as DB_HOST
PG_CRON_PORT=5432                       # Default: same as DB_PORT  
PG_CRON_USER=cache_user                 # Default: same as DB_USER
PG_CRON_PASSWORD=cache_password         # Default: same as DB_PASSWORD
PG_CRON_DATABASE=postgres               # Default: postgres

# Cache table prefix (auto-detected from environment if not specified)
PG_ARRAY_CACHE_TABLE_PREFIX=my_cache
# or PG_BIT_CACHE_TABLE_PREFIX=my_cache
# or PG_ROARINGBIT_CACHE_TABLE_PREFIX=my_cache
```

#### Cross-Database Setup Process

1. **Grant pg_cron Permissions**:
   ```sql
   -- Connect to the pg_cron database (usually postgres)
   psql -U your_username -d postgres
   
   -- Grant necessary permissions
   GRANT USAGE ON SCHEMA cron TO your_username;
   GRANT EXECUTE ON FUNCTION cron.schedule_in_database TO your_username;
   GRANT EXECUTE ON FUNCTION cron.unschedule TO your_username;
   GRANT SELECT ON cron.job TO your_username;
   ```

2. **Setup Eviction Manager**:
   ```bash
   # The CLI tool automatically handles cross-database setup
   pcache-postgresql-eviction-manager setup --table-prefix my_cache
   ```

#### How Cross-Database Eviction Works

1. **Setup Phase**: 
   - Script connects to `postgres` database to install pg_cron configuration objects
   - Creates eviction functions and log tables in `cache_database`

2. **Job Scheduling**:
   - Eviction jobs are stored in `postgres` database
   - Jobs use `cron.schedule_in_database()` to execute in `cache_database`

3. **Eviction Processing**:
   - Jobs run in `cache_database` where cache tables exist
   - Configuration is retrieved from `postgres` database via dblink

### Commands

#### Setup and Management

```bash
# Setup with custom configuration
pcache-postgresql-eviction-manager setup \
    --table-prefix my_cache \
    --frequency 120 \
    --threshold 5000 \
    --strategy largest \
    --enable-after-setup

# Remove eviction infrastructure
pcache-postgresql-eviction-manager remove --table-prefix my_cache

# Enable/disable eviction
pcache-postgresql-eviction-manager enable --table-prefix my_cache
pcache-postgresql-eviction-manager disable --table-prefix my_cache
```

#### Configuration Updates

```bash
# Update eviction strategy
pcache-postgresql-eviction-manager update-config \
    --table-prefix my_cache \
    --strategy oldest \
    --threshold 1000

# Change frequency to run every 30 minutes
pcache-postgresql-eviction-manager update-config \
    --table-prefix my_cache \
    --frequency 30
```

#### Monitoring

```bash
# Check eviction status
pcache-postgresql-eviction-manager status --table-prefix my_cache

# View eviction logs
pcache-postgresql-eviction-manager logs --table-prefix my_cache --limit 50

# Manually trigger eviction for testing
pcache-postgresql-eviction-manager manual-run --table-prefix my_cache
```

### Eviction Strategies

#### Oldest Strategy
Removes entries based on `last_seen` timestamps in the queries table:

```sql
-- Finds oldest entries across all partitions
SELECT query_hash FROM queries_table 
WHERE partition_key = 'target_partition' AND status = 'ok'
ORDER BY last_seen ASC 
LIMIT number_to_remove;
```

#### Largest Strategy
Removes entries with the highest partition key counts:

```sql
-- Finds largest entries by count column
SELECT query_hash FROM cache_table 
JOIN queries_table ON cache_table.query_hash = queries_table.query_hash
WHERE queries_table.partition_key = 'target_partition' AND status = 'ok'
ORDER BY cache_table.partition_keys_count DESC 
LIMIT number_to_remove;
```

### Logging and Monitoring

The eviction manager provides comprehensive logging:

```bash
# View recent eviction activity
pcache-postgresql-eviction-manager logs --table-prefix my_cache

# Example log output:
# [2024-01-15 14:23:45] Job: partitioncache_evict_my_cache | 
# Partition: user_region | Status: success | Removed: 150 | 
# Message: Evicted oldest queries.
```

## Manual Eviction (`pcache-manage maintenance evict`)

For one-time eviction operations or non-PostgreSQL cache backends, use the manual eviction command.

### Features

- **Cross-Backend Support**: Works with PostgreSQL, Redis, and RocksDB caches
- **Command-Line Configuration**: All settings specified via command-line arguments
- **Independent Operation**: Does not require database-stored configuration
- **Immediate Execution**: Runs immediately, not scheduled

### Usage

```bash
# Evict using oldest strategy (PostgreSQL only)
pcache-manage maintenance evict \
    --strategy oldest \
    --threshold 1000

# Evict using largest strategy (all cache types)
pcache-manage maintenance evict \
    --type redis \
    --strategy largest \
    --threshold 500

# Evict from specific cache type
pcache-manage maintenance evict \
    --type postgresql_array \
    --strategy oldest \
    --threshold 2000
```

### Strategy Support by Cache Type

| Cache Type | Oldest Strategy | Largest Strategy |
|------------|----------------|------------------|
| **postgresql_array** | ✅ Yes | ✅ Yes |
| **postgresql_bit** | ✅ Yes | ✅ Yes |
| **postgresql_roaringbit** | ✅ Yes | ✅ Yes |
| **redis_set** | ❌ No | ✅ Yes |
| **redis_bit** | ❌ No | ✅ Yes |
| **rocksdb_set** | ❌ No | ✅ Yes |
| **rocksdb_bit** | ❌ No | ✅ Yes |

**Note:** The `oldest` strategy requires access to query timestamps, which is currently only available in PostgreSQL-based caches.

## Best Practices

### Choosing Between Automatic and Manual Eviction

**Use Automatic Eviction (pcache-postgresql-eviction-manager) when:**
- Using PostgreSQL cache backends
- Need consistent, scheduled cache maintenance
- Want comprehensive logging and monitoring
- Prefer hands-off cache management

**Use Manual Eviction (pcache-manage) when:**
- Using non-PostgreSQL cache backends (Redis, RocksDB)
- Need one-time cache cleanup
- Want command-line control over eviction parameters
- Integrating with external monitoring/automation systems

### Threshold Selection

- **Conservative**: Set threshold to 80% of desired maximum cache size
- **Aggressive**: Set threshold to 60% of desired maximum cache size
- **Monitor and Adjust**: Review eviction logs to fine-tune thresholds

### Strategy Selection

- **Oldest Strategy**: Best for time-sensitive data where older queries are less relevant
- **Largest Strategy**: Best for memory optimization where large result sets consume disproportionate resources, while providing the least impact on query performance


## Integration Examples

### Production Monitoring

```bash
#!/bin/bash
# Daily cache maintenance script

# Check eviction status
pcache-postgresql-eviction-manager status --table-prefix production_cache

# Manual cleanup if automatic eviction is disabled
if [[ $? -ne 0 ]]; then
    pcache-manage maintenance evict \
        --strategy largest \
        --threshold 10000
fi

# Review logs for issues
pcache-postgresql-eviction-manager logs --table-prefix production_cache --limit 24
```

### Multi-Backend Environment

```bash
# Automatic eviction for PostgreSQL caches
pcache-postgresql-eviction-manager setup --table-prefix pg_cache --strategy oldest --threshold 5000
pcache-postgresql-eviction-manager enable --table-prefix pg_cache

# Manual eviction for Redis caches (scheduled via cron)
# 0 */4 * * * pcache-manage maintenance evict --type redis --strategy largest --threshold 2000
```

## Troubleshooting

### Common Issues

**Eviction Manager Setup Fails**
```bash
❌ pg_cron extension is not installed
```
**Solution**: Install pg_cron extension or use manual eviction instead.

**No Items Evicted**
```bash
Partition: user_region | Status: success | Removed: 0
```
**Solution**: Cache size is below threshold, or all entries have `failed`/`timeout` status (which are preserved).

**Largest Strategy Fails**
```bash
Status: failed | Message: No count column found
```
**Solution**: Ensure cache tables have been created through normal caching operations, which creates the required count columns.


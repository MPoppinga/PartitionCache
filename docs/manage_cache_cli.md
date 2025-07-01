# PartitionCache Management CLI

The `pcache-manage` command provides a comprehensive command-line interface for managing PartitionCache tables, caches, queues, and maintenance operations. The tool is organized into logical command groups for better usability.

## Usage

```bash
pcache-manage [--env ENV_FILE] <command> [options]
```

## Global Options

- `--env ENV_FILE`: Specify environment file to use (default: .env)

## Commands Overview

### Setup Commands (`setup`)

Initialize PartitionCache tables and configuration.

#### `setup all` (Recommended for new projects)
Set up both queue and cache metadata tables.

```bash
pcache-manage setup all [--cache BACKEND] [--queue PROVIDER]
```

Options:
- `--cache BACKEND`: Cache backend to setup (defaults to CACHE_BACKEND env var)
- `--queue PROVIDER`: Queue provider to setup (defaults to QUERY_QUEUE_PROVIDER env var)

#### `setup queue`
Set up only queue tables.

```bash
pcache-manage setup queue [--queue PROVIDER]
```

Options:
- `--queue PROVIDER`: Queue provider to setup (defaults to QUERY_QUEUE_PROVIDER env var)

#### `setup cache`
Set up only cache metadata tables.

```bash
pcache-manage setup cache [--cache BACKEND]
```

Options:
- `--cache BACKEND`: Cache backend to setup (defaults to CACHE_BACKEND env var)

Examples:
```bash
# Use environment defaults
pcache-manage setup all

# Setup specific backends
pcache-manage setup all --cache redis_set --queue postgresql
pcache-manage setup cache --cache postgresql_array
pcache-manage setup queue --queue redis
```

### Status Commands (`status`)

Check PartitionCache status and configuration.

#### `status` (Default)
Check both environment and table status.

```bash
pcache-manage status
```

#### `status env`
Validate environment configuration only.

```bash
pcache-manage status env
```

#### `status tables`
Check table status and accessibility only.

```bash
pcache-manage status tables
```

### Cache Commands (`cache`)

Manage cache data and operations.

#### `cache count`
Count cache entries.

```bash
# Count cache entries (uses CACHE_BACKEND from environment)
pcache-manage cache count

# Count specific cache type
pcache-manage cache count --type postgresql_array

# Count all cache types
pcache-manage cache count --all
```

**Options:**
- `--type CACHE_TYPE`: Cache type (default: from CACHE_BACKEND env var) - e.g., postgresql_array, redis_set, rocksdb_set
- `--all`: Count all cache types

#### `cache copy`
Copy cache from one type to another.

```bash
pcache-manage cache copy --from redis_set --to postgresql_array
```

**Options:**
- `--from SOURCE_TYPE`: Source cache type (required)
- `--to DEST_TYPE`: Destination cache type (required)

#### `cache export`
Export cache to file.

```bash
# Export cache (uses CACHE_BACKEND from environment)
pcache-manage cache export --file backup.pkl

# Export specific cache type
pcache-manage cache export --type postgresql_array --file backup.pkl
```

**Options:**
- `--type CACHE_TYPE`: Cache type to export (default: from CACHE_BACKEND env var)
- `--file FILE_PATH`: Archive file path (required)

#### `cache import`
Import cache from file.

```bash
# Import cache (uses CACHE_BACKEND from environment)
pcache-manage cache import --file backup.pkl

# Import to specific cache type
pcache-manage cache import --type postgresql_array --file backup.pkl
```

**Options:**
- `--type CACHE_TYPE`: Cache type to import to (default: from CACHE_BACKEND env var)
- `--file FILE_PATH`: Archive file path (required)

#### `cache delete`
Delete cache data.

```bash
# Delete cache (uses CACHE_BACKEND from environment)
pcache-manage cache delete

# Delete specific cache type
pcache-manage cache delete --type postgresql_array

# Delete all cache types (with confirmation)
pcache-manage cache delete --all
```

**Options:**
- `--type CACHE_TYPE`: Cache type to delete (default: from CACHE_BACKEND env var)
- `--all`: Delete all cache types (requires confirmation)

### Queue Commands (`queue`)

Manage queue operations.

#### `queue count`
Count queue entries.

```bash
pcache-manage queue count
```

#### `queue clear`
Clear queue entries.

```bash
# Clear both queues
pcache-manage queue clear

# Clear only original query queue
pcache-manage queue clear --original

# Clear only fragment query queue
pcache-manage queue clear --fragment
```

**Options:**
- `--original`: Clear only original query queue
- `--fragment`: Clear only fragment query queue

### Maintenance Commands (`maintenance`)

Perform maintenance operations on caches and partitions.

#### `maintenance prune`
Remove old queries from caches.

```bash
# Prune all caches (remove queries older than 30 days)
pcache-manage maintenance prune --days 30

# Prune specific cache type
pcache-manage maintenance prune --days 7 --type postgresql_array
```

**Options:**
- `--days DAYS`: Remove queries older than this many days (default: 30)
- `--type CACHE_TYPE`: Specific cache type to prune (default: all)

#### `maintenance evict`
Evict cache entries based on a strategy and threshold.

```bash
# Evict using oldest strategy (uses CACHE_BACKEND from environment)
pcache-manage maintenance evict --strategy oldest --threshold 1000

# Evict using largest strategy from specific cache type
pcache-manage maintenance evict --type postgresql_array --strategy largest --threshold 500
```

**Options:**
- `--type CACHE_TYPE`: Cache type to evict from (default: from CACHE_BACKEND env var)
- `--strategy {oldest,largest}`: Eviction strategy (required)
  - `oldest`: Remove entries with oldest `last_seen` timestamps
  - `largest`: Remove entries with the highest partition key counts
- `--threshold INTEGER`: Cache size threshold to trigger eviction (required)

**Note:** The `oldest` strategy is only supported for PostgreSQL cache backends. For other cache backends, only the `largest` strategy is available.

#### `maintenance cleanup`
Clean up cache entries.

```bash
# Remove termination entries (uses CACHE_BACKEND from environment)
pcache-manage maintenance cleanup --remove-termination

# Remove termination entries from specific cache type
pcache-manage maintenance cleanup --type postgresql_array --remove-termination

# Remove large entries (more than 1000 items)
pcache-manage maintenance cleanup --remove-large 1000
```

**Options:**
- `--type CACHE_TYPE`: Cache type to clean (default: from CACHE_BACKEND env var)
- `--remove-termination`: Remove termination entries (_LIMIT_, _TIMEOUT_)
- `--remove-large MAX_ENTRIES`: Remove entries with more than MAX_ENTRIES items

#### `maintenance partition`
Manage partitions.

```bash
# Delete specific partition (uses CACHE_BACKEND from environment)
pcache-manage maintenance partition --delete partition_key_name

# Delete partition from specific cache type
pcache-manage maintenance partition --type postgresql_array --delete partition_key_name
```

**Options:**
- `--type CACHE_TYPE`: Cache type (default: from CACHE_BACKEND env var)
- `--delete PARTITION_KEY`: Delete specific partition

## Examples

### Setting up a new project

```bash
# 1. Validate environment
pcache-manage status env

# 2. Set up all tables
pcache-manage setup all

# 3. Verify setup
pcache-manage status
```

### Cache operations

```bash
# Count cache entries (uses environment CACHE_BACKEND)
pcache-manage cache count

# Count specific cache type
pcache-manage cache count --type postgresql_array

# Copy from Redis to PostgreSQL
pcache-manage cache copy --from redis_set --to postgresql_array

# Export cache for backup (uses environment CACHE_BACKEND)
pcache-manage cache export --file backup.pkl

# Clean up old entries (uses environment CACHE_BACKEND)
pcache-manage maintenance cleanup --remove-termination
```

### Queue management

```bash
# Check queue status
pcache-manage queue count

# Clear old queue entries
pcache-manage queue clear --original
```

### Maintenance tasks

```bash
# Remove old queries (30+ days)
pcache-manage maintenance prune --days 30

# Evict large caches using oldest strategy (PostgreSQL only)
pcache-manage maintenance evict --strategy oldest --threshold 1000

# Evict large caches using largest strategy (all cache types)
pcache-manage maintenance evict --strategy largest --threshold 500

# Remove large result sets (>5000 items, uses environment CACHE_BACKEND)
pcache-manage maintenance cleanup --remove-large 5000

# Delete specific partition (uses environment CACHE_BACKEND)
pcache-manage maintenance partition --delete old_partition
```

## Cache Types

Supported cache types:
- `postgresql_array`: PostgreSQL with array storage
- `postgresql_bit`: PostgreSQL with bit storage
- `redis_set`: Redis with set storage
- `redis_bit`: Redis with bit storage
- `rocksdb_set`: RocksDB with set storage
- `rocksdb_bit`: RocksDB with bit storage

## Environment Variables

The tool respects the following environment variables:

- `CACHE_BACKEND`: Default cache backend type
- `QUERY_QUEUE_PROVIDER`: Queue provider (default: postgresql)
- Database connection variables (DB_HOST, DB_PORT, etc.)
- Queue connection variables (PG_QUEUE_HOST, PG_QUEUE_PORT, etc.)
- Table prefix variables (PG_ARRAY_CACHE_TABLE_PREFIX, PG_QUEUE_TABLE_PREFIX, etc.)

## Error Handling

The tool provides detailed error messages and suggestions for common issues:
- Missing environment variables
- Database connection problems
- Table access issues
- Configuration validation errors

Use `--help` with any command or subcommand to get detailed usage information. 
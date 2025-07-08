# Cross-Database pg_cron Setup for PartitionCache

This document provides comprehensive guidance for setting up pg_cron with PartitionCache in a cross-database configuration, where pg_cron is installed in one database (typically `postgres`) and jobs execute in work databases (e.g., `partition_cache_db`, `osm_example_db`).

## Architecture Overview

### Traditional Single-Database Setup (Legacy)
```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Work Database                                      │
│ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐ │
│ │   pg_cron       │  │  Queue Tables   │  │  Cache Tables           │ │
│ │   Extension     │  │  Config Tables  │  │  Processor Functions    │ │
│ │   & Jobs        │  │  Log Tables     │  │  Business Logic         │ │
│ └─────────────────┘  └─────────────────┘  └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### New Cross-Database Setup (Recommended)
```
┌─────────────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Instance                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐                    ┌─────────────────────────┐    │
│  │  pg_cron DB     │                    │     Work Database       │    │
│  │  (postgres)     │                    │  (partition_cache_db)   │    │
│  │                 │                    │                         │    │
│  │ ┌─────────────┐ │ schedule_in_       │ ┌─────────────────────┐ │    │
│  │ │  pg_cron    │ │ database()         │ │  Queue Tables       │ │    │
│  │ │  Extension  │ │ ─────────────────→ │ │  Config Tables      │ │    │
│  │ │  & Jobs     │ │                    │ │  Cache Tables       │ │    │
│  │ └─────────────┘ │                    │ │  Processor Functions│ │    │
│  └─────────────────┘                    │ │  Business Logic     │ │    │
│                                         │ └─────────────────────┘ │    │
│                                         └─────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Benefits of Cross-Database Setup

### 1. **Better Isolation**
- pg_cron jobs and metadata are isolated from business data
- Reduces risk of accidental interference between applications
- Easier to manage permissions and security

### 2. **Scalability**
- Single pg_cron instance can serve multiple work databases
- Better resource utilization across databases
- Simplified monitoring and management

### 3. **Security**
- Work databases don't need direct access to pg_cron metadata
- Reduced attack surface for each application
- Better permission granularity

### 4. **Maintainability**
- Centralized pg_cron management
- Easier upgrades and maintenance
- Better separation of concerns

## Prerequisites

### 1. PostgreSQL Version
- PostgreSQL 12+ required
- pg_cron extension 1.4+ (for `schedule_in_database()` function)

### 2. Extension Installation
```sql
-- In pg_cron database (usually 'postgres')
CREATE EXTENSION IF NOT EXISTS pg_cron;
```

### 3. PostgreSQL Configuration
Add to `postgresql.conf`:
```ini
# Required for pg_cron
shared_preload_libraries = 'pg_cron'

# Database where pg_cron is installed (default: postgres)
cron.database_name = 'postgres'

# Optional: Configure pg_cron settings
cron.log_run = on
cron.log_statement = on
cron.max_running_jobs = 32
```

## Environment Configuration

### Core Variables
```bash
# Work database (where PartitionCache runs)
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=partition_cache_db

# pg_cron database (where pg_cron is installed)
PG_CRON_HOST=localhost                   # Default: same as DB_HOST
PG_CRON_PORT=5432                       # Default: same as DB_PORT
PG_CRON_USER=your_username              # Default: same as DB_USER (needs pg_cron permissions)
PG_CRON_PASSWORD=your_password          # Default: same as DB_PASSWORD
PG_CRON_DATABASE=postgres               # Default: postgres
```

### Queue Configuration
```bash
# Queue tables (must be in same database as cache)
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5432
PG_QUEUE_USER=your_username
PG_QUEUE_PASSWORD=your_password
PG_QUEUE_DB=partition_cache_db          # Same as DB_NAME
```

### Cache Backend Configuration
```bash
# Choose your cache backend
CACHE_BACKEND=postgresql_array
PG_ARRAY_CACHE_TABLE_PREFIX=my_cache

# Other backends: postgresql_bit, postgresql_roaringbit, etc.
```

## Permissions Setup

### 1. pg_cron Database Permissions

**Automatic Setup (Recommended):**
The setup script will automatically check and grant permissions when possible:
```bash
pcache-postgresql-queue-processor setup --enable-after-setup
# Script will attempt to grant permissions automatically

# To check permissions independently:
pcache-postgresql-queue-processor check-permissions
```

**Manual Setup (If Automatic Fails):**
```sql
-- Connect to pg_cron database (usually 'postgres') as superuser
\c postgres

-- Grant pg_cron permissions to your user
GRANT USAGE ON SCHEMA cron TO your_username;
GRANT EXECUTE ON FUNCTION cron.schedule TO your_username;
GRANT EXECUTE ON FUNCTION cron.schedule_in_database TO your_username;
GRANT EXECUTE ON FUNCTION cron.unschedule TO your_username;
GRANT EXECUTE ON FUNCTION cron.alter_job TO your_username;

-- Optional: Grant read access to job status
GRANT SELECT ON cron.job TO your_username;
```

### 2. Work Database Permissions
```sql
-- Connect to work database
\c partition_cache_db

-- Standard PartitionCache permissions
GRANT CREATE ON DATABASE partition_cache_db TO your_username;
GRANT USAGE ON SCHEMA public TO your_username;
GRANT CREATE ON SCHEMA public TO your_username;

-- Permissions for queue processing
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO your_username;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_username;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO your_username;
```

## Installation Steps

### 1. Setup pg_cron Database
```bash
# Connect to PostgreSQL as superuser
psql -U postgres -d postgres

# Create extension
CREATE EXTENSION IF NOT EXISTS pg_cron;

# Verify installation
SELECT * FROM pg_extension WHERE extname = 'pg_cron';
```

### 2. Configure Environment
```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your pg_cron database settings
# Set PG_CRON_DATABASE=postgres (or your chosen pg_cron database)

# Validate configuration
python test_env_vars.py
```

### 3. Setup PartitionCache
```bash
# Setup all components
pcache-manage setup all

# Setup PostgreSQL queue processor with cross-database support
pcache-postgresql-queue-processor setup \
    --frequency 1 \
    --timeout 1800 \
    --enable-after-setup
```

### 4. Verify Setup
```bash
# Check processor status
pcache-postgresql-queue-processor status

# Check pg_cron jobs
psql -U postgres -d postgres -c "SELECT jobid, jobname, schedule, database, active FROM cron.job;"
```

## Configuration Examples

### 1. Single Instance, Multiple Databases
```bash
# pg_cron in 'postgres' database
PG_CRON_DATABASE=postgres

# App 1 in 'app1_db'
DB_NAME=app1_db
PG_ARRAY_CACHE_TABLE_PREFIX=app1_cache

# App 2 in 'app2_db'
DB_NAME=app2_db
PG_ARRAY_CACHE_TABLE_PREFIX=app2_cache
```

### 2. Development Environment
```bash
# Use default postgres database for pg_cron
PG_CRON_DATABASE=postgres

# Work database for development
DB_NAME=dev_partitioncache
PG_ARRAY_CACHE_TABLE_PREFIX=dev_cache
```

### 3. Production Environment
```bash
# Dedicated pg_cron database
PG_CRON_DATABASE=cron_management

# Production work database
DB_NAME=production_cache
PG_ARRAY_CACHE_TABLE_PREFIX=prod_cache
```

## Advanced Configuration

### 1. Custom pg_cron Database
```sql
-- Create dedicated database for pg_cron
CREATE DATABASE cron_management;

-- Install pg_cron in custom database
\c cron_management
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Update postgresql.conf
-- cron.database_name = 'cron_management'
```

### 2. Multiple Work Databases
```bash
# Environment for App 1
DB_NAME=ecommerce_cache
PG_ARRAY_CACHE_TABLE_PREFIX=ecom_cache

# Environment for App 2
DB_NAME=analytics_cache
PG_ARRAY_CACHE_TABLE_PREFIX=analytics_cache

# Both use same pg_cron database
PG_CRON_DATABASE=postgres
```

### 3. Security Hardening
```sql
-- Create dedicated user for pg_cron operations
CREATE USER partitioncache_cron WITH PASSWORD 'secure_password';

-- Grant minimal required permissions
GRANT USAGE ON SCHEMA cron TO partitioncache_cron;
GRANT EXECUTE ON FUNCTION cron.schedule_in_database TO partitioncache_cron;
GRANT EXECUTE ON FUNCTION cron.unschedule TO partitioncache_cron;

-- Use in environment
PG_CRON_USER=partitioncache_cron
PG_CRON_PASSWORD=secure_password
```

## Monitoring and Maintenance

### 1. Job Status Monitoring
```sql
-- Check active jobs
SELECT jobid, jobname, schedule, database, active, last_run, next_run
FROM cron.job
WHERE jobname LIKE 'partitioncache%';

-- Check job run history
SELECT * FROM cron.job_run_details
WHERE jobname LIKE 'partitioncache%'
ORDER BY start_time DESC
LIMIT 10;
```

### 2. Performance Monitoring
```sql
-- Monitor job execution times
SELECT 
    jobname,
    AVG(extract(epoch from (end_time - start_time))) as avg_duration_seconds,
    COUNT(*) as run_count
FROM cron.job_run_details
WHERE jobname LIKE 'partitioncache%'
    AND start_time > NOW() - INTERVAL '1 hour'
GROUP BY jobname;
```

### 3. Cleanup Operations
```sql
-- Remove old job run details (run from pg_cron database)
DELETE FROM cron.job_run_details
WHERE start_time < NOW() - INTERVAL '7 days';

-- Remove orphaned jobs
DELETE FROM cron.job
WHERE jobname LIKE 'partitioncache%'
    AND database NOT IN (
        SELECT datname FROM pg_database WHERE datistemplate = false
    );
```

## Troubleshooting

### 1. Common Issues

#### Jobs Not Created
```bash
# Check if pg_cron is accessible
psql -U $PG_CRON_USER -d $PG_CRON_DATABASE -c "SELECT * FROM cron.job LIMIT 1;"

# Check trigger installation
psql -U $DB_USER -d $DB_NAME -c "SELECT tgname FROM pg_trigger WHERE tgname LIKE '%sync_cron%';"

# Check configuration
psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM partitioncache_queue_processor_config;"
```

#### Jobs Not Running
```bash
# Check job status
psql -U $PG_CRON_USER -d $PG_CRON_DATABASE -c "SELECT jobid, jobname, active, last_run FROM cron.job WHERE jobname LIKE 'partitioncache%';"

# Check PostgreSQL logs for pg_cron errors
tail -f /var/log/postgresql/postgresql-*-main.log | grep cron
```

#### Permission Errors
```bash
# Verify permissions
psql -U $PG_CRON_USER -d $PG_CRON_DATABASE -c "SELECT has_function_privilege('cron.schedule_in_database', 'EXECUTE');"

# Check work database access
psql -U $DB_USER -d $DB_NAME -c "SELECT current_user, current_database();"
```

### 2. Recovery Procedures

#### Reset pg_cron Jobs
```bash
# Remove all PartitionCache jobs
pcache-postgresql-queue-processor disable
pcache-postgresql-queue-processor remove

# Recreate with fresh configuration
pcache-postgresql-queue-processor setup --enable-after-setup
```

#### Database Migration
```bash
# Export configuration
pg_dump -U $DB_USER -d $DB_NAME --data-only -t "*processor_config" > config_backup.sql

# Setup new database
createdb new_work_database
psql -U $DB_USER -d new_work_database -f config_backup.sql

# Update environment
DB_NAME=new_work_database
```

## Best Practices

### 1. **Database Naming**
- Use descriptive names for work databases
- Keep pg_cron database name simple (e.g., `postgres`, `cron_management`)
- Avoid special characters in database names

### 2. **Security**
- Use dedicated users for pg_cron operations
- Limit permissions to minimum required
- Regularly rotate passwords
- Monitor job execution logs

### 3. **Performance**
- Monitor job execution times
- Use appropriate parallelism settings
- Regular cleanup of old job run details
- Consider resource limits for long-running jobs

### 4. **Backup and Recovery**
- Include pg_cron database in backup procedures
- Test recovery procedures regularly
- Document custom configurations
- Version control environment configurations

## Migration from Single-Database Setup

### 1. **Assessment**
```bash
# Check current setup
pcache-postgresql-queue-processor status

# Identify existing jobs
psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM cron.job WHERE jobname LIKE 'partitioncache%';"
```

### 2. **Migration Steps**
```bash
# 1. Disable current processor
pcache-postgresql-queue-processor disable

# 2. Export configuration
pg_dump -U $DB_USER -d $DB_NAME --data-only -t "*processor_config" > migration_config.sql

# 3. Setup pg_cron in postgres database
psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_cron;"

# 4. Update environment variables
# Add PG_CRON_* variables to .env

# 5. Remove old setup
pcache-postgresql-queue-processor remove

# 6. Setup new cross-database configuration
pcache-postgresql-queue-processor setup --enable-after-setup

# 7. Verify migration
pcache-postgresql-queue-processor status
```

### 3. **Rollback Procedure**
```bash
# 1. Disable cross-database setup
pcache-postgresql-queue-processor disable

# 2. Remove cross-database configuration
pcache-postgresql-queue-processor remove

# 3. Restore single-database setup
# Remove PG_CRON_* variables from .env
pcache-postgresql-queue-processor setup --enable-after-setup
```

## Conclusion

The cross-database pg_cron setup provides better isolation, security, and scalability for PartitionCache deployments. While slightly more complex to configure, it offers significant advantages in production environments and multi-application setups.

For development and testing, the single-database approach may be sufficient, but production deployments should strongly consider the cross-database configuration for its operational benefits.
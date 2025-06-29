# PartitionCache Migration Guide

This guide covers migrating between different PartitionCache backends, upgrading versions, and transitioning from external systems to PartitionCache.

## Backend Migration Strategies

### Overview of Migration Scenarios

```mermaid
graph TB
    subgraph "Current State"
        CURR_PG[PostgreSQL Array]
        CURR_REDIS[Redis Set]
        CURR_ROCKS[RocksDB]
        CURR_EXT[External System]
    end
    
    subgraph "Target State"
        TARGET_PG[PostgreSQL Array]
        TARGET_REDIS[Redis Set]
        TARGET_ROCKS[RocksDB]
        TARGET_BIT[Bit-based Backends]
    end
    
    subgraph "Migration Types"
        SAME[Same Backend Type<br/>Configuration Change]
        DIFF[Different Backend<br/>Type Migration]
        NEW[New Installation<br/>Fresh Setup]
    end
    
    CURR_PG -.-> TARGET_REDIS
    CURR_REDIS -.-> TARGET_PG
    CURR_ROCKS -.-> TARGET_PG
    CURR_EXT -.-> TARGET_PG
    
    CURR_PG --> SAME
    CURR_REDIS --> DIFF
    CURR_EXT --> NEW
    
    style SAME fill:#e8f5e8
    style DIFF fill:#fff3e0
    style NEW fill:#e1f5fe
```

## 1. Between Cache Backends

### 1.1 PostgreSQL Array ↔ Redis Set

#### Export from PostgreSQL Array

```bash
# Export current cache data
pcache-manage cache export \
    --type postgresql_array \
    --file postgresql_backup_$(date +%Y%m%d).pkl

# Verify export
ls -la postgresql_backup_*.pkl
```

#### Import to Redis Set

```bash
# Update environment for Redis
export CACHE_BACKEND=redis_set
export REDIS_HOST=your-redis-server
export REDIS_PORT=6379
export REDIS_PASSWORD=your-redis-password

# Import to Redis
pcache-manage cache import \
    --type redis_set \
    --file postgresql_backup_20240115.pkl

# Verify import
pcache-manage cache count --type redis_set
```

#### Live Migration Script

```python
#!/usr/bin/env python3
"""
Live migration between PostgreSQL Array and Redis Set backends.
Maintains service availability during migration.
"""

import partitioncache
import time
from datetime import datetime

def migrate_postgresql_to_redis():
    """Migrate from PostgreSQL Array to Redis Set with minimal downtime."""
    
    print(f"🚀 Starting migration at {datetime.now()}")
    
    # Setup source and destination
    pg_cache = partitioncache.create_cache_helper("postgresql_array", "default", "integer")
    redis_cache = partitioncache.create_cache_helper("redis_set", "default", "integer", 
                                                   host="redis-server", port=6379)
    
    # Get all partition keys from PostgreSQL
    partitions = pg_cache.get_partition_keys()
    print(f"📋 Found {len(partitions)} partitions to migrate")
    
    total_keys = 0
    migrated_keys = 0
    
    for partition_key, datatype in partitions:
        print(f"\n📦 Migrating partition: {partition_key} ({datatype})")
        
        # Skip unsupported datatypes for Redis
        if datatype not in ['integer', 'text']:
            print(f"⚠️  Skipping partition {partition_key}: Redis doesn't support {datatype}")
            continue
            
        # Get all keys for this partition
        keys = pg_cache.get_all_keys(partition_key)
        total_keys += len(keys)
        print(f"   📊 {len(keys)} keys to migrate")
        
        # Migrate each key
        for i, key in enumerate(keys):
            try:
                # Read from PostgreSQL
                data = pg_cache.get(key, partition_key)
                if data is not None:
                    # Write to Redis
                    success = redis_cache.set_set(key, data, partition_key)
                    if success:
                        migrated_keys += 1
                    else:
                        print(f"   ❌ Failed to migrate key: {key}")
                        
                # Progress indicator
                if (i + 1) % 100 == 0:
                    print(f"   📈 Progress: {i + 1}/{len(keys)} keys")
                    
            except Exception as e:
                print(f"   ❌ Error migrating key {key}: {e}")
        
        print(f"   ✅ Completed partition {partition_key}")
    
    # Verification
    print(f"\n🔍 Verification:")
    print(f"   Total keys: {total_keys}")
    print(f"   Migrated: {migrated_keys}")
    print(f"   Success rate: {migrated_keys/total_keys*100:.1f}%")
    
    # Verify random sample
    verification_sample = min(10, migrated_keys)
    verified = 0
    
    for partition_key, datatype in partitions[:3]:  # Check first 3 partitions
        if datatype not in ['integer', 'text']:
            continue
            
        keys = pg_cache.get_all_keys(partition_key)[:verification_sample]
        for key in keys:
            pg_data = pg_cache.get(key, partition_key)
            redis_data = redis_cache.get(key, partition_key)
            
            if pg_data == redis_data:
                verified += 1
            else:
                print(f"   ❌ Verification failed for {key}")
    
    print(f"   ✅ Verified: {verified}/{verification_sample} sample keys")
    print(f"\n🎉 Migration completed at {datetime.now()}")

if __name__ == "__main__":
    migrate_postgresql_to_redis()
```

### 1.2 Redis Set → PostgreSQL Array

More complex due to datatype inference requirements.

```python
#!/usr/bin/env python3
"""
Migration from Redis Set to PostgreSQL Array with datatype detection.
"""

import partitioncache
from collections import defaultdict

def detect_datatype(values):
    """Detect the most appropriate datatype for a set of values."""
    if not values:
        return "integer"  # Default fallback
    
    sample = list(values)[:10]  # Sample for performance
    
    # Check if all values are integers
    try:
        all(int(v) for v in sample)
        return "integer"
    except (ValueError, TypeError):
        pass
    
    # Check if all values are floats
    try:
        all(float(v) for v in sample)
        return "float"
    except (ValueError, TypeError):
        pass
    
    # Default to text
    return "text"

def migrate_redis_to_postgresql():
    """Migrate from Redis Set to PostgreSQL Array with datatype detection."""
    
    print("🚀 Starting Redis to PostgreSQL migration")
    
    # Setup source and destination
    redis_cache = partitioncache.create_cache_helper("redis_set", "default", "integer",
                                                    host="redis-server", port=6379)
    
    # Analyze Redis data structure
    partitions = redis_cache.get_partition_keys()
    partition_datatypes = {}
    
    print("🔍 Analyzing partition datatypes...")
    for partition_key, _ in partitions:
        keys = redis_cache.get_all_keys(partition_key)
        if keys:
            # Sample data to determine datatype
            sample_data = redis_cache.get(keys[0], partition_key)
            detected_type = detect_datatype(sample_data)
            partition_datatypes[partition_key] = detected_type
            print(f"   📊 Partition {partition_key}: detected {detected_type}")
    
    # Create PostgreSQL cache with detected datatypes
    pg_cache = partitioncache.create_cache_helper("postgresql_array", "default", "integer")
    
    # Register all partitions with correct datatypes
    for partition_key, datatype in partition_datatypes.items():
        pg_cache.register_partition_key(partition_key, datatype)
        print(f"   📝 Registered {partition_key} as {datatype}")
    
    # Migrate data
    total_migrated = 0
    for partition_key, datatype in partition_datatypes.items():
        keys = redis_cache.get_all_keys(partition_key)
        print(f"\n📦 Migrating {len(keys)} keys for partition {partition_key}")
        
        for key in keys:
            data = redis_cache.get(key, partition_key)
            if data is not None:
                success = pg_cache.set_set(key, data, partition_key)
                if success:
                    total_migrated += 1
    
    print(f"\n✅ Migration completed: {total_migrated} keys migrated")
    return total_migrated

if __name__ == "__main__":
    migrate_redis_to_postgresql()
```

### 1.3 Array → Bit Backend Migration

Special considerations for migrating to bit-based backends.

```python
#!/usr/bin/env python3
"""
Migration from array-based to bit-based backends.
Only supports integer datatypes.
"""

def migrate_to_bit_backend():
    """Migrate integer partitions to bit-based storage."""
    
    # Source: PostgreSQL Array
    source_cache = partitioncache.create_cache_helper("postgresql_array", "default", "integer")
    
    # Analyze partitions for compatibility
    partitions = source_cache.get_partition_keys()
    compatible_partitions = []
    
    print("🔍 Checking partition compatibility for bit backend...")
    for partition_key, datatype in partitions:
        if datatype == "integer":
            # Check value ranges for bit backend compatibility
            keys = source_cache.get_all_keys(partition_key)
            max_value = 0
            
            for key in keys[:10]:  # Sample check
                data = source_cache.get(key, partition_key)
                if data:
                    current_max = max(data)
                    max_value = max(max_value, current_max)
            
            if max_value < 50000:  # Reasonable bit array size
                compatible_partitions.append((partition_key, max_value))
                print(f"   ✅ {partition_key}: compatible (max value: {max_value})")
            else:
                print(f"   ❌ {partition_key}: incompatible (max value: {max_value} > 50000)")
        else:
            print(f"   ❌ {partition_key}: incompatible datatype ({datatype})")
    
    if not compatible_partitions:
        print("❌ No compatible partitions found for bit backend")
        return False
    
    # Setup destination with appropriate bit sizes
    dest_cache = partitioncache.create_cache_helper("postgresql_bit", "default", "integer")
    
    for partition_key, max_value in compatible_partitions:
        # Calculate required bit size with buffer
        bitsize = max(10000, max_value + 1000)
        
        # Create partition table with appropriate bit size
        dest_cache.underlying_handler._create_partition_table(partition_key, bitsize)
        print(f"   📝 Created bit partition {partition_key} with bitsize {bitsize}")
        
        # Migrate data
        keys = source_cache.get_all_keys(partition_key)
        migrated = 0
        
        for key in keys:
            data = source_cache.get(key, partition_key)
            if data is not None:
                success = dest_cache.set_set(key, data, partition_key)
                if success:
                    migrated += 1
        
        print(f"   ✅ Migrated {migrated}/{len(keys)} keys for {partition_key}")
    
    return True

if __name__ == "__main__":
    migrate_to_bit_backend()
```

## 2. Configuration Migration

### 2.1 Environment Variable Updates

#### Before (Old Configuration)

```bash
# Old environment variables
CACHE_TYPE=postgresql_array
DB_CONNECTION_STRING=postgresql://user:pass@host:5432/db
QUEUE_TYPE=postgresql
```

#### After (New Configuration)

```bash
# New environment variables (current standard)
CACHE_BACKEND=postgresql_array
DB_HOST=host
DB_PORT=5432
DB_USER=user
DB_PASSWORD=pass
DB_NAME=db

QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=host
PG_QUEUE_PORT=5432
PG_QUEUE_USER=user
PG_QUEUE_PASSWORD=pass
PG_QUEUE_DB=db
```

#### Migration Script for Environment

```bash
#!/bin/bash
# migrate-env-config.sh

echo "🔧 Migrating PartitionCache configuration..."

# Backup old configuration
cp .env .env.backup.$(date +%Y%m%d)

# Convert old format to new format
if grep -q "CACHE_TYPE" .env; then
    echo "📝 Converting CACHE_TYPE to CACHE_BACKEND"
    sed -i 's/CACHE_TYPE=/CACHE_BACKEND=/g' .env
fi

if grep -q "QUEUE_TYPE" .env; then
    echo "📝 Converting QUEUE_TYPE to QUERY_QUEUE_PROVIDER"
    sed -i 's/QUEUE_TYPE=/QUERY_QUEUE_PROVIDER=/g' .env
fi

# Parse DB_CONNECTION_STRING and split into components
if grep -q "DB_CONNECTION_STRING" .env; then
    echo "📝 Converting DB_CONNECTION_STRING to individual variables"
    
    # Extract connection string
    CONNECTION_STRING=$(grep "DB_CONNECTION_STRING=" .env | cut -d'=' -f2)
    
    # Parse components (basic regex parsing)
    DB_USER=$(echo $CONNECTION_STRING | sed -n 's/.*\/\/\([^:]*\):.*/\1/p')
    DB_PASS=$(echo $CONNECTION_STRING | sed -n 's/.*\/\/[^:]*:\([^@]*\)@.*/\1/p')
    DB_HOST=$(echo $CONNECTION_STRING | sed -n 's/.*@\([^:]*\):.*/\1/p')
    DB_PORT=$(echo $CONNECTION_STRING | sed -n 's/.*:\([0-9]*\)\/.*/\1/p')
    DB_NAME=$(echo $CONNECTION_STRING | sed -n 's/.*\/\([^?]*\).*/\1/p')
    
    # Add new variables
    echo "" >> .env
    echo "# Migrated database configuration" >> .env
    echo "DB_HOST=$DB_HOST" >> .env
    echo "DB_PORT=$DB_PORT" >> .env
    echo "DB_USER=$DB_USER" >> .env
    echo "DB_PASSWORD=$DB_PASS" >> .env
    echo "DB_NAME=$DB_NAME" >> .env
    
    # Comment out old variable
    sed -i 's/^DB_CONNECTION_STRING=/#DB_CONNECTION_STRING=/' .env
fi

echo "✅ Configuration migration completed"
echo "📋 Backup saved as .env.backup.$(date +%Y%m%d)"
echo "🔍 Please review .env and test configuration"
```

### 2.2 Code Migration

#### Before (Legacy API)

```python
# Legacy code patterns
from partitioncache.cache_handler import get_cache_handler

# Manual handler creation
cache_handler = get_cache_handler("postgresql_array")
cache_handler.set_set("key", {1, 2, 3}, set, "partition_key")

# Direct queue functions
from partitioncache.queue import add_to_queue
add_to_queue("SELECT * FROM table", "partition_key")
```

#### After (Modern API)

```python
# Modern recommended patterns
import partitioncache

# Use create_cache_helper for validation and convenience
cache = partitioncache.create_cache_helper("postgresql_array", "partition_key", "integer")
cache.set_set("key", {1, 2, 3})

# Use apply_cache_lazy for optimal performance
enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM table",
    cache_handler=cache.underlying_handler,
    partition_key="partition_key"
)

# Modern queue operations with datatype support
partitioncache.push_to_original_query_queue(
    "SELECT * FROM table", 
    partition_key="partition_key",
    partition_datatype="integer"
)
```

## 3. Version Migration

### 3.1 Upgrading from v0.4.x to v0.5.x

#### Database Schema Changes

```sql
-- Add new columns for enhanced queue support
ALTER TABLE original_query_queue 
ADD COLUMN IF NOT EXISTS partition_datatype TEXT DEFAULT 'integer';

ALTER TABLE query_fragment_queue 
ADD COLUMN IF NOT EXISTS partition_datatype TEXT DEFAULT 'integer';

-- Add new metadata tracking
CREATE TABLE IF NOT EXISTS partitioncache_version_info (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

INSERT INTO partitioncache_version_info (version, notes) 
VALUES ('0.5.0', 'Added datatype support and enhanced queue system');
```

#### Python Code Updates

```python
#!/usr/bin/env python3
"""
Upgrade script from PartitionCache v0.4.x to v0.5.x
"""

import partitioncache
import os

def upgrade_v04_to_v05():
    """Upgrade from v0.4.x to v0.5.x"""
    
    print("🚀 Upgrading PartitionCache from v0.4.x to v0.5.x")
    
    # 1. Update database schema
    print("📊 Updating database schema...")
    
    # Run schema updates
    from partitioncache.db_handler import get_db_handler
    db = get_db_handler()
    
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            # Add datatype columns if they don't exist
            cur.execute("""
                ALTER TABLE original_query_queue 
                ADD COLUMN IF NOT EXISTS partition_datatype TEXT DEFAULT 'integer'
            """)
            cur.execute("""
                ALTER TABLE query_fragment_queue 
                ADD COLUMN IF NOT EXISTS partition_datatype TEXT DEFAULT 'integer'
            """)
            
            print("   ✅ Database schema updated")
    
    # 2. Migrate configuration
    print("🔧 Checking configuration...")
    
    # Check for old environment variables
    old_vars = ['CACHE_TYPE', 'QUEUE_TYPE', 'DB_CONNECTION_STRING']
    for var in old_vars:
        if os.getenv(var):
            print(f"   ⚠️  Found legacy variable: {var}")
            print(f"   📝 Please update to new format (see migration guide)")
    
    # 3. Test new functionality
    print("🧪 Testing new functionality...")
    
    try:
        # Test create_cache_helper with datatype
        cache = partitioncache.create_cache_helper("postgresql_array", "test_partition", "integer")
        
        # Test datatype validation
        cache.set_set("test_key", {1, 2, 3}, partition_key="test_partition")
        result = cache.get("test_key", partition_key="test_partition")
        
        if result == {1, 2, 3}:
            print("   ✅ Cache operations working correctly")
        
        # Cleanup test data
        cache.delete("test_key", partition_key="test_partition")
        
    except Exception as e:
        print(f"   ❌ Error testing new functionality: {e}")
        return False
    
    print("🎉 Upgrade completed successfully!")
    print("📋 Next steps:")
    print("   1. Update application code to use new API patterns")
    print("   2. Consider using apply_cache_lazy for better performance") 
    print("   3. Review environment variables for new format")
    
    return True

if __name__ == "__main__":
    upgrade_v04_to_v05()
```

## 4. External System Migration

### 4.1 From Manual Caching to PartitionCache

#### Before: Manual Redis Cache

```python
# Legacy manual caching approach
import redis
import json
import hashlib

class ManualCache:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379)
    
    def get_cache_key(self, query, partition):
        combined = f"{query}:{partition}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def cache_query_result(self, query, partition, result_ids):
        key = self.get_cache_key(query, partition)
        self.redis.setex(key, 3600, json.dumps(list(result_ids)))
    
    def get_cached_result(self, query, partition):
        key = self.get_cache_key(query, partition)
        data = self.redis.get(key)
        return set(json.loads(data)) if data else None
```

#### After: PartitionCache Integration

```python
# Modern PartitionCache approach
import partitioncache

class ModernCache:
    def __init__(self):
        self.cache = partitioncache.create_cache_helper(
            "redis_set", 
            "default_partition", 
            "integer"
        )
    
    def cache_query_result(self, query, partition_key, result_ids):
        # PartitionCache handles hashing automatically
        from partitioncache.query_processor import generate_hash
        query_hash = generate_hash(query, partition_key)
        return self.cache.set_set(query_hash, result_ids, partition_key)
    
    def get_cached_result(self, query, partition_key):
        # Use modern apply_cache_lazy for optimal performance
        enhanced_query, stats = partitioncache.apply_cache_lazy(
            query=query,
            cache_handler=self.cache.underlying_handler,
            partition_key=partition_key
        )
        
        if stats['enhanced']:
            return enhanced_query, stats
        return None, stats
```

#### Migration Script for External Systems

```python
#!/usr/bin/env python3
"""
Migrate from external caching system to PartitionCache
"""

def migrate_from_manual_redis():
    """Migrate from manual Redis caching to PartitionCache."""
    
    print("🚀 Migrating from manual Redis cache to PartitionCache")
    
    # Connect to existing Redis
    import redis
    manual_redis = redis.Redis(host='localhost', port=6379)
    
    # Setup PartitionCache
    cache = partitioncache.create_cache_helper("redis_set", "migrated_partition", "integer")
    
    # Scan all keys in manual cache
    pattern = "cache:*"  # Adjust to your key pattern
    keys = manual_redis.keys(pattern)
    
    migrated_count = 0
    
    for key in keys:
        try:
            # Extract original query and partition from key
            key_str = key.decode('utf-8')
            
            # Parse your key format (adjust as needed)
            if ':' in key_str:
                _, query_hash = key_str.split(':', 1)
                
                # Get cached data
                data = manual_redis.get(key)
                if data:
                    import json
                    result_ids = set(json.loads(data))
                    
                    # Store in PartitionCache
                    success = cache.set_set(query_hash, result_ids, "migrated_partition")
                    if success:
                        migrated_count += 1
                        
        except Exception as e:
            print(f"   ❌ Error migrating key {key}: {e}")
    
    print(f"✅ Migrated {migrated_count}/{len(keys)} cache entries")
    return migrated_count

if __name__ == "__main__":
    migrate_from_manual_redis()
```

### 4.2 From Database Views to PartitionCache

```python
#!/usr/bin/env python3
"""
Migrate from materialized views to PartitionCache
"""

def migrate_from_materialized_views():
    """Migrate from materialized views to dynamic PartitionCache."""
    
    print("🚀 Migrating from materialized views to PartitionCache")
    
    # Connect to database
    from partitioncache.db_handler import get_db_handler
    db = get_db_handler()
    
    # Setup PartitionCache
    cache = partitioncache.create_cache_helper("postgresql_array", "city_id", "integer")
    
    # Find all materialized views to migrate
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schemaname, matviewname 
                FROM pg_matviews 
                WHERE matviewname LIKE 'cache_%'
            """)
            
            views = cur.fetchall()
            
    for schema, view_name in views:
        print(f"📦 Migrating view: {schema}.{view_name}")
        
        try:
            # Extract data from materialized view
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT DISTINCT city_id FROM {schema}.{view_name}")
                    city_ids = {row[0] for row in cur.fetchall()}
            
            # Store in PartitionCache
            # Use view name as cache key
            cache_key = view_name.replace('cache_', '')
            success = cache.set_set(cache_key, city_ids, "city_id")
            
            if success:
                print(f"   ✅ Migrated {len(city_ids)} city IDs")
                
                # Optionally drop old view
                # with db.get_connection() as conn:
                #     with conn.cursor() as cur:
                #         cur.execute(f"DROP MATERIALIZED VIEW {schema}.{view_name}")
                
        except Exception as e:
            print(f"   ❌ Error migrating {view_name}: {e}")
    
    print("✅ Materialized view migration completed")

if __name__ == "__main__":
    migrate_from_materialized_views()
```

## 5. Migration Validation

### 5.1 Data Integrity Verification

```python
#!/usr/bin/env python3
"""
Comprehensive validation suite for PartitionCache migrations
"""

def validate_migration(source_cache, dest_cache, partition_keys):
    """Validate migration between cache backends."""
    
    print("🔍 Validating migration data integrity...")
    
    total_keys = 0
    valid_keys = 0
    errors = []
    
    for partition_key, datatype in partition_keys:
        print(f"\n📊 Validating partition: {partition_key} ({datatype})")
        
        # Get all keys from source
        source_keys = source_cache.get_all_keys(partition_key)
        dest_keys = dest_cache.get_all_keys(partition_key)
        
        print(f"   Source keys: {len(source_keys)}")
        print(f"   Destination keys: {len(dest_keys)}")
        
        # Check key count consistency
        if len(source_keys) != len(dest_keys):
            errors.append(f"Key count mismatch in {partition_key}: {len(source_keys)} vs {len(dest_keys)}")
        
        # Validate each key
        for key in source_keys:
            total_keys += 1
            
            source_data = source_cache.get(key, partition_key)
            dest_data = dest_cache.get(key, partition_key)
            
            if source_data == dest_data:
                valid_keys += 1
            else:
                errors.append(f"Data mismatch for key {key} in partition {partition_key}")
                if len(errors) < 10:  # Limit error reporting
                    print(f"     ❌ Key {key}: {source_data} != {dest_data}")
    
    # Summary
    success_rate = (valid_keys / total_keys * 100) if total_keys > 0 else 0
    print(f"\n📈 Validation Results:")
    print(f"   Total keys validated: {total_keys}")
    print(f"   Valid keys: {valid_keys}")
    print(f"   Success rate: {success_rate:.1f}%")
    print(f"   Errors: {len(errors)}")
    
    if success_rate >= 99.0:
        print("✅ Migration validation PASSED")
        return True
    else:
        print("❌ Migration validation FAILED")
        for error in errors[:5]:  # Show first 5 errors
            print(f"   • {error}")
        return False

def performance_comparison(source_cache, dest_cache, test_keys):
    """Compare performance between source and destination caches."""
    
    import time
    
    print("\n⚡ Performance comparison...")
    
    # Test source cache performance
    start_time = time.time()
    for key in test_keys:
        source_cache.get(key, "test_partition")
    source_time = time.time() - start_time
    
    # Test destination cache performance
    start_time = time.time()
    for key in test_keys:
        dest_cache.get(key, "test_partition")
    dest_time = time.time() - start_time
    
    print(f"   Source cache: {source_time:.3f}s ({len(test_keys)} operations)")
    print(f"   Destination cache: {dest_time:.3f}s ({len(test_keys)} operations)")
    
    if dest_time <= source_time * 1.5:  # Allow 50% performance degradation
        print("✅ Performance validation PASSED")
        return True
    else:
        print("⚠️  Performance validation WARNING: significant slowdown detected")
        return False
```

### 5.2 Rollback Procedures

```python
#!/usr/bin/env python3
"""
Rollback procedures for failed migrations
"""

def rollback_migration(backup_file, target_cache_type):
    """Rollback to previous state using backup."""
    
    print("🔄 Rolling back migration...")
    
    try:
        # Clear current cache
        print("🧹 Clearing current cache...")
        cache = partitioncache.create_cache_helper(target_cache_type, "default", "integer")
        
        # Get all partitions and clear them
        partitions = cache.get_partition_keys()
        for partition_key, _ in partitions:
            keys = cache.get_all_keys(partition_key)
            for key in keys:
                cache.delete(key, partition_key)
        
        # Restore from backup
        print(f"📥 Restoring from backup: {backup_file}")
        os.system(f"pcache-manage cache import --file {backup_file} --type {target_cache_type}")
        
        # Verify restoration
        restored_cache = partitioncache.create_cache_helper(target_cache_type, "default", "integer")
        partitions = restored_cache.get_partition_keys()
        
        total_keys = 0
        for partition_key, _ in partitions:
            keys = restored_cache.get_all_keys(partition_key)
            total_keys += len(keys)
        
        print(f"✅ Rollback completed: {total_keys} keys restored")
        return True
        
    except Exception as e:
        print(f"❌ Rollback failed: {e}")
        return False

def create_migration_checkpoint(cache_type, checkpoint_name):
    """Create a checkpoint before migration."""
    
    import os
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"checkpoint_{checkpoint_name}_{timestamp}.pkl"
    
    print(f"💾 Creating migration checkpoint: {backup_file}")
    
    try:
        os.system(f"pcache-manage cache export --type {cache_type} --file {backup_file}")
        print(f"✅ Checkpoint created: {backup_file}")
        return backup_file
    except Exception as e:
        print(f"❌ Failed to create checkpoint: {e}")
        return None
```

## 6. Best Practices for Migration

### 6.1 Migration Checklist

```bash
# Pre-migration checklist
□ Backup current cache data
□ Test migration in staging environment  
□ Verify application compatibility
□ Plan rollback procedure
□ Schedule maintenance window
□ Notify stakeholders

# During migration
□ Monitor system performance
□ Validate data integrity
□ Check error logs
□ Verify cache hit rates
□ Test critical application flows

# Post-migration
□ Performance comparison
□ Data validation
□ Monitor for 24-48 hours
□ Update documentation
□ Clean up old cache data (after validation period)
```

### 6.2 Migration Testing Strategy

```python
#!/usr/bin/env python3
"""
Comprehensive migration testing strategy
"""

def test_migration_thoroughly():
    """Comprehensive migration testing."""
    
    tests = [
        test_data_integrity,
        test_performance_regression, 
        test_cache_hit_rates,
        test_error_handling,
        test_concurrent_access,
        test_failover_scenarios
    ]
    
    results = {}
    
    for test in tests:
        try:
            result = test()
            results[test.__name__] = result
            print(f"✅ {test.__name__}: {'PASS' if result else 'FAIL'}")
        except Exception as e:
            results[test.__name__] = False
            print(f"❌ {test.__name__}: ERROR - {e}")
    
    # Overall assessment
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    
    print(f"\n📊 Migration Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All migration tests PASSED - migration is ready for production")
    else:
        print("⚠️  Some tests FAILED - review issues before proceeding")
    
    return passed == total
```

This migration guide provides comprehensive coverage of all common migration scenarios with practical scripts and validation procedures to ensure safe and successful transitions.
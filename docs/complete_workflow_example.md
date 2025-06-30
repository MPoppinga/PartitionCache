# Complete PartitionCache Workflow Example

This document demonstrates a complete end-to-end PartitionCache workflow using the OpenStreetMap POI example with PostgreSQL Queue Processor integration. The example showcases automatic cache population, real-time monitoring, and performance optimization.

## Overview

The complete workflow demonstrates:

1. **PostgreSQL queue processor setup and configuration**
2. **Automatic query processing via pg_cron**
3. **Cache population from complex spatial queries**
4. **Performance monitoring and logging**
5. **Cache effectiveness demonstration**

## Prerequisites

1. **Docker and Docker Compose** for PostGIS database
2. **Python 3.10+** with PartitionCache installed
3. **OpenStreetMap POI data** processed (see [OpenStreetMap POI Example](../examples/openstreetmap_poi/README.md))

## Quick Start

### 1. Start the Database

```bash
cd examples/openstreetmap_poi
docker-compose up -d
```

### 2. Configure Environment

Create a `.env` file:

```bash
# Database Configuration for PostGIS Container
POSTGRES_DB=osm_poi_db
POSTGRES_USER=osm_user
POSTGRES_PASSWORD=secure_password

# PartitionCache Database Configuration (points to the same container)
DB_HOST=localhost
DB_PORT=55432
DB_USER=osm_user
DB_PASSWORD=secure_password
DB_NAME=osm_poi_db

# Cache Backend Configuration
CACHE_BACKEND=postgresql_array
PG_ARRAY_CACHE_TABLE_PREFIX=osm_cache_

# Queue Configuration (for PostgreSQL Queue Processor)
QUERY_QUEUE_PROVIDER=postgresql
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=55432
PG_QUEUE_USER=osm_user
PG_QUEUE_PASSWORD=secure_password
PG_QUEUE_DB=osm_poi_db
PG_QUEUE_TABLE_PREFIX=osm_queue_
```

### 3. Process OSM Data (if not done already)

```bash
python process_osm_data.py
```

### 4. Run Complete Workflow Test

```bash
python test_workflow.py
```

## Workflow Steps Explained

### Step 1: PartitionCache Setup

The script automatically sets up all required PartitionCache tables:

```bash
pcache-manage setup all
```

This creates:
- Queue tables for original queries and query fragments
- Cache metadata tables for partition tracking
- All necessary indexes and constraints

### Step 2: PostgreSQL Queue Processor Setup

Sets up the PostgreSQL Queue Processor with pg_cron integration:

```bash
# Setup processor infrastructure
pcache-postgresql-queue-processor setup

# Enable real-time processing
pcache-postgresql-queue-processor enable

# Configure for optimal performance
pcache-postgresql-queue-processor config --max-jobs 5 --frequency 2
```

**Configuration Details:**
- **max-jobs**: 5 parallel processing jobs
- **frequency**: Process queue every 2 seconds
- **pg_cron integration**: Automatic scheduling without external dependencies

### Step 3: Initial Status Check

Monitors the system before processing:

```bash
# Check processor status
pcache-postgresql-queue-processor status

# Check queue and cache state
pcache-manage queue count
pcache-manage cache count
```

**Expected Initial State:**
- Processor: Enabled and configured
- Queue: Empty (0 entries)
- Cache: Empty (0 entries)

### Step 4: Baseline Query Performance

Runs initial POI queries without cache to establish baseline performance:

```sql
-- Example Query (q1.sql): Ice cream shops near pharmacies and ALDI
SELECT p1.name AS ice_cream_name,
       p2.name AS pharmacy_name,
       p3.name AS supermarket_name,
       p1.zipcode
FROM pois AS p1, pois as p2, pois as p3
WHERE p1.name LIKE '%Eis %' 
  AND p2.subtype = 'pharmacy'
  AND p3.subtype = 'supermarket'
  AND p3.name LIKE '%ALDI%'
  AND p2.zipcode = p1.zipcode
  AND p3.zipcode = p1.zipcode
  AND p3.zipcode = p2.zipcode
  AND ST_DWithin(p1.geom, p3.geom, 300)
  AND ST_DWithin(p1.geom, p2.geom, 400);
```

**Performance Metrics Collected:**
- Execution time without cache
- Number of results returned
- Query complexity assessment

### Step 5: Queue Population

Adds complex spatial queries to the processing queue for automatic cache population:

```bash
# Add all landkreis-based queries to the queue
pcache-add --queue-original \
  --query-file testqueries_examples/landkreis/q1.sql \
  --partition-key landkreis \
  --partition-datatype text

pcache-add --queue-original \
  --query-file testqueries_examples/landkreis/q2.sql \
  --partition-key landkreis \
  --partition-datatype text

pcache-add --queue-original \
  --query-file testqueries_examples/landkreis/q3.sql \
  --partition-key landkreis \
  --partition-datatype text
```

**Queue Processing Logic:**
1. Queries are decomposed into partition-specific subqueries
2. Each subquery is processed independently
3. Results are cached as partition key sets
4. Failed queries are logged with error details

### Step 6: Real-Time Monitoring

Monitors the automatic processing in real-time:

```bash
# Check queue progress
pcache-manage queue count

# Monitor processing for 15 seconds
# (processor runs every 2 seconds)
```

**Monitoring Indicators:**
- Queue length decreasing
- Processing logs showing activity
- No error messages in logs

### Step 7: Processing Analysis

Analyzes the results of automatic processing:

```bash
# Detailed processor status
pcache-postgresql-queue-processor status-detailed

# Recent processing logs
pcache-postgresql-queue-processor logs --limit 10

# Final queue and cache state
pcache-manage queue count
pcache-manage cache count
```

**Expected Processing Results:**
- Queue: Empty or near-empty
- Cache: Contains partition keys for processed queries
- Logs: Show successful processing with timing information

### Step 8: Cache Effectiveness Test

Re-runs the same queries to demonstrate cache effectiveness:

**Performance Comparison:**
- **Without Cache**: Full spatial query execution
- **With Cache**: Optimized query with partition key restriction

**Expected Results:**
```
Without cache: 45 results in 0.234 seconds
With cache: 45 results in 0.015 + 0.089 = 0.104 seconds
Cache hits: 3/3 subqueries, Speedup: 2.25x
```

### Step 9: Advanced Analysis

Provides detailed system analysis:

```bash
# Detailed cache architecture analysis
pcache-postgresql-queue-processor queue-info

# Optional: Run complete POI query suite
python run_poi_queries.py
```

**Analysis Output:**
- Partition key distribution
- Cache table architecture (bit vs array)
- Queue processing statistics
- Performance optimization recommendations

## Expected Output

### Successful Workflow Output

```
================================================================================
🚀 PartitionCache Complete Workflow Test
================================================================================

✅ Loaded environment from /path/to/examples/openstreetmap_poi/.env

📊 Configuration:
   Database: localhost:55432/osm_poi_db
   Cache Backend: postgresql_array
   Queue Provider: postgresql

⏳ Waiting for database to be ready...
   ✅ Database is ready!

============================================================
📝 Step 1: Setting up PartitionCache tables
============================================================

🔧 Setting up queue and cache metadata tables
   Command: pcache-manage setup all
   Output: ✓ Queue tables setup completed successfully
          ✓ Cache metadata tables setup completed successfully
          PartitionCache setup completed successfully!

============================================================
🛠️ Step 2: Setting up PostgreSQL Queue Processor
============================================================

🛠️ Setting up PostgreSQL queue processor with pg_cron
       Command: pcache-postgresql-queue-processor setup
    Output: PostgreSQL queue processor setup complete!

🛠️ Enabling the PostgreSQL queue processor
    Command: pcache-postgresql-queue-processor enable
    Output: Processor enabled

🛠️ Configuring processor for 5 parallel jobs every 2 seconds
    Command: pcache-postgresql-queue-processor config --max-jobs 5 --frequency 2
    Output: Configuration updated

============================================================
📊 Step 3: Checking initial status
============================================================

🛠️ Checking PostgreSQL queue processor status
    Command: pcache-postgresql-queue-processor status
    Output: === PartitionCache PostgreSQL Queue Processor Status ===
          Enabled: Yes
          Max Parallel Jobs: 5
          Frequency: Every 2 second(s)
          
          Current State:
            Active Jobs: 0
            Queue Length: 0
            Recent Successes (5 min): 0
            Recent Failures (5 min): 0

🔧 Checking initial queue length
   Command: pcache-manage queue count
   Output: Queue statistics (using postgresql):
          Original query queue: 0 entries
          Query fragment queue: 0 entries

🔧 Checking initial cache entries
   Command: pcache-manage cache count
   Output: Cache statistics for postgresql_array:
          Total keys: 0
          Valid entries: 0

============================================================
🔍 Step 4: Running initial POI queries (no cache expected)
============================================================

🔍 Running POI query: Ice cream near pharmacies and ALDI (Q1)
   Query file: /path/to/testqueries_examples/landkreis/q1.sql
   Without cache: 45 results in 0.234 seconds
   No cached partition keys found
   ✅ No cache hit as expected for initial run

============================================================
📥 Step 5: Adding queries to processing queue
============================================================

🔧 Adding testqueries_examples/landkreis/q1.sql to processing queue
   Command: pcache-add --queue-original --query-file ... --partition-key landkreis --partition-datatype text
   Output: Query added to original query queue successfully

🔧 Adding testqueries_examples/landkreis/q2.sql to processing queue
   Command: pcache-add --queue-original --query-file ... --partition-key landkreis --partition-datatype text
   Output: Query added to original query queue successfully

🔧 Adding testqueries_examples/landkreis/q3.sql to processing queue
   Command: pcache-add --queue-original --query-file ... --partition-key landkreis --partition-datatype text
   Output: Query added to original query queue successfully

============================================================
⏳ Step 6: Monitoring queue processing
============================================================

🔧 Checking queue length after adding queries
   Command: pcache-manage queue count
   Output: Queue statistics (using postgresql):
          Original query queue: 3 entries
          Query fragment queue: 0 entries

⏳ Waiting for PostgreSQL queue processor to process queries...
   The PostgreSQL queue processor automatically populated the cache and queries are now optimized!

============================================================
📊 Step 7: Checking processing results
============================================================

🔧 Checking detailed processor status
   Command: pcache-postgresql-queue-processor status-detailed
   Output: === PartitionCache PGQueue Processor - Detailed Status ===
          Enabled: Yes
          Max Parallel Jobs: 5
          Frequency: Every 2 second(s)
          
          Current Processing State:
            Active Jobs: 0
            Queue Length: 0
            Recent Successes (5 min): 9
            Recent Failures (5 min): 0
          
          Cache Architecture Overview:
            Total Partitions: 3
            Partitions with Cache: 3
            Array Cache Partitions: 3
            Uncreated Cache Partitions: 0

🔧 Checking recent processor logs
   Command: pcache-postgresql-queue-processor logs --limit 10
   Output: === Recent Processor Logs (limit: 10) ===
          
          [2024-01-15 14:23:45] Job: 12345
            Query Hash: abc123, Partition: Hamburg
            Status: success
            Execution Time: 89ms
            Rows Affected: 1

🔧 Checking remaining queue length
   Command: pcache-manage queue count
   Output: Queue statistics (using postgresql):
          Original query queue: 0 entries
          Query fragment queue: 0 entries

🔧 Checking cache entries after processing
   Command: pcache-manage cache count
   Output: Cache statistics for postgresql_array:
          Total keys: 9
          Valid entries: 9

============================================================
🔍 Step 8: Re-running queries to test cache effectiveness
============================================================

🔍 Running POI query: Ice cream near pharmacies and ALDI (Q1) - with cache
   Query file: /path/to/testqueries_examples/landkreis/q1.sql
   Without cache: 45 results in 0.234 seconds
   With cache: 45 results in 0.015 + 0.089 = 0.104 seconds
   Cache hits: 3/3 subqueries, Speedup: 2.25x

📈 Results Summary:
   Initial run (no cache): 45 results
   Final run (with cache): 45 results
   Cache effectiveness: ✅ Cache hit!
   🎉 Workflow completed successfully!
   The PostgreSQL queue processor automatically populated the cache and queries are now optimized!

============================================================
🔍 Step 9: Advanced monitoring and analysis
============================================================

🔧 Checking detailed queue and cache architecture info
       Command: pcache-postgresql-queue-processor queue-info
    Output: === Queue and Cache Architecture Details ===
          Partition Key        Architecture    Queue Items     Cache Table                   Last Update         
          ----------------------------------------------------------------------------------------------------
          Hamburg              array           0               osm_cache_landkreis           2024-01-15 14:23:45
          Berlin               array           0               osm_cache_landkreis           2024-01-15 14:23:43
          München              array           0               osm_cache_landkreis           2024-01-15 14:23:41

================================================================================
🎉 PartitionCache Workflow Test Complete!
================================================================================
Summary:
✅ PostgreSQL queue processor setup and configuration
✅ Automatic query processing via pg_cron
✅ Cache population from complex spatial queries
✅ Performance monitoring and logging
✅ Cache effectiveness demonstration

Next steps:
- Add more queries to the queue for automatic processing
- Monitor processor status: pcache-postgresql-queue-processor status
- Check cache statistics: pcache-manage cache count
- View processor logs: pcache-postgresql-queue-processor logs
```

## Performance Benefits 

TODO: Verify numbers and add more metrics

### Query Optimization Results

| Metric | Without Cache | With Cache | Improvement |
|--------|---------------|------------|-------------|
| **Execution Time** | 0.234s | 0.104s | **2.25x faster** |
| **Cache Lookup** | N/A | 0.015s | Added overhead |
| **Optimized Query** | N/A | 0.089s | Reduced execution |
| **Cache Hit Rate** | 0% | 100% (3/3) | Perfect hits |

### System Scalability

- **Automatic Processing**: No manual intervention required
- **Real-time Updates**: 2-second processing cycle
- **Parallel Processing**: 5 concurrent jobs
- **Error Recovery**: Automatic retry and logging
- **Resource Efficiency**: Minimal CPU and memory overhead

## Troubleshooting

### Common Issues

**Database Connection Failures**
```bash
❌ Cannot connect to database. Is the container running?
   Try: docker-compose up -d
```
**Solution**: Ensure PostGIS container is running and accessible.

**pg_cron Extension Missing**
```bash
❌ Failed to create pg_cron extension
```
**Solution**: Use the provided Dockerfile which includes pg_cron setup.

**No Cache Hits After Processing**
```bash
⚠️ Cache not effective. Check processor logs for issues.
```
**Solution**: Check processor logs for errors:
```bash
pcache-postgresql-queue-processor logs --limit 20
```

**Query Processing Failures**
```bash
Recent Failures (5 min): 3
```
**Solution**: Analyze error messages in processor logs and verify query syntax.

### Performance Tuning

**Optimize Processing Frequency**
```bash
# For high-volume workloads
pcache-postgresql-queue-processor config --frequency 1

# For low-volume workloads  
pcache-postgresql-queue-processor config --frequency 5
```

**Adjust Parallel Processing**
```bash
# Increase for powerful systems
pcache-postgresql-queue-processor config --max-jobs 10

# Decrease for resource-constrained systems
pcache-postgresql-queue-processor config --max-jobs 2
```

## Integration with Existing Applications

### Adding Custom Queries

```bash
# Add your application queries
pcache-add --queue-original \
  --query-file your_query.sql \
  --partition-key your_partition_column \
  --partition-datatype integer
```

### Monitoring in Production

```bash
# Daily cache statistics
pcache-manage cache count

# Weekly performance analysis
pcache-postgresql-queue-processor status-detailed

# Monthly cache cleanup
pcache-manage maintenance prune --days 30
```

### Scaling Recommendations

- **Small Applications**: 1-2 parallel jobs, 5-second frequency
- **Medium Applications**: 3-5 parallel jobs, 2-second frequency  
- **Large Applications**: 5-10 parallel jobs, 1-second frequency, dedicated database servers for cache population


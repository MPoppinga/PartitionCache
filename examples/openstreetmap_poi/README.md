# OpenStreetMap POI Example with PartitionCache

This example demonstrates PartitionCache's complete workflow using real OpenStreetMap Point of Interest (POI) data for Germany, showcasing both traditional PostgreSQL queries and **high-performance DuckDB integration**.

## 🚀 **New: DuckDB Integration (Recommended)**

**Hybrid Database Architecture** providing 44-60x performance improvements:
- **DuckDB**: Fast analytical queries (OLAP) - sub-second response times
- **PostgreSQL**: Reliable cache metadata storage (OLTP) - proven infrastructure
- **Cross-Database Integration**: Seamless operation via DuckDB's PostgreSQL extension

## Overview

The example imports OSM data for Germany and extracts POIs with their associated:
- **Zipcode** (integer partition key): Postal codes for fine-grained geographic partitioning  
- **Landkreis** (text partition key): Administrative districts (admin_level=6) for broader regional partitioning

This demonstrates:
- **Dual Database Architecture**: DuckDB + PostgreSQL integration
- **Advanced Performance**: 44-60x query speedup with analytical workloads
- **Lazy Cache Support**: Cross-database lazy cache via PostgreSQL extension
- **Multi-partition Support**: Different datatypes (integer vs text)
- **Spatial Compatibility**: Full PostGIS spatial function support
- **Complete CLI Workflow**: End-to-end cache management

## Quick Start

Choose your preferred workflow:

### 🦆 **Option A: DuckDB Integration (Recommended for Analytics)**

**1. Complete Setup**
```bash
# Start PostgreSQL database
docker-compose up -d

# Configure environment  
cp .env.example .env

# Install dependencies
pip install -r requirements.txt
pip install duckdb  # Additional requirement for DuckDB

# Import OSM data (traditional PostgreSQL import)
python process_osm_data.py

# Setup PartitionCache tables
pcache-manage setup all
```

**2. Migrate to DuckDB**
```bash
# One-time migration (5.16M rows in ~8 seconds)
python migrate_to_duckdb.py

# Verify dual database setup
python test_dual_setup.py
```

**3. Run High-Performance Queries**
```bash
# Comprehensive query testing with performance comparison
python run_poi_queries_dual.py

# Expected: 44-60x speedup vs PostgreSQL queries
```

**4. Explore Advanced Features**
```bash
# Test different cache backends
CACHE_BACKEND=postgresql_array python run_poi_queries_dual.py   # Full lazy support
CACHE_BACKEND=postgresql_roaringbit python run_poi_queries_dual.py  # Memory optimized
```

📖 **Complete Guide**: See [DUCKDB_INTEGRATION_GUIDE.md](DUCKDB_INTEGRATION_GUIDE.md) for detailed documentation.

### 🐘 **Option B: Traditional PostgreSQL (Original Workflow)**

### 1. Start the PostGIS Database

From the `examples/openstreetmap_poi/` directory, run:

```bash
docker-compose up -d
```

This starts a PostgreSQL database with PostGIS enabled, accessible on port 55432.

### 2. Configure Environment Variables

Copy and configure the environment file:

```bash
cp .env.example .env
# Edit .env with your specific configuration if needed
```

### 3. Install Python Dependencies

From the project root:

```bash
pip install -r requirements.txt
```

### 4. Import OSM Data

Run the import script (this may take a while for large OSM files):

```bash
python process_osm_data.py
```

This script will:
- Download the Germany OSM file if not present
- Extract POIs (amenities, shops, tourism, leisure)
- Assign zipcode and administrative boundaries to each POI

### 5. Run Example Queries

Test queries with both partition strategies:

```bash
python run_poi_queries.py
```

This will:
- Run the same queries against zipcode, landkreis, and spatial partition keys
- Show performance comparisons between strategies
- Demonstrate cache hit/miss scenarios
- Provide helpful hints for adding queries to cache

Expected output shows timing comparisons and cache effectiveness for each partition strategy.

## Working with the Cache

### Adding Queries to Cache

#### For Zipcode Partition (Integer)

```bash
# Add query directly to cache
pcache-add --direct \
  --query-file testqueries_examples/zipcode/q1.sql \
  --partition-key zipcode \
  --partition-datatype integer \
  --cache-backend postgresql_array \
  --env .env

# Add to queue for async processing
pcache-add --queue-original \
  --query-file testqueries_examples/zipcode/q1.sql \
  --partition-key zipcode \
  --partition-datatype integer \
  --env .env

# Add all test queries for zipcode
for query in testqueries_examples/zipcode/*.sql; do
  pcache-add --direct \
    --query-file "$query" \
    --partition-key zipcode \
    --partition-datatype integer \
    --cache-backend postgresql_array \
    --env .env
done
```

#### For Landkreis Partition (Text)

```bash
# Add query directly to cache
pcache-add --direct \
  --query-file testqueries_examples/landkreis/q1.sql \
  --partition-key landkreis \
  --partition-datatype text \
  --cache-backend postgresql_array \
  --env .env

# Add to queue for async processing
pcache-add --queue-original \
  --query-file testqueries_examples/landkreis/q1.sql \
  --partition-key landkreis \
  --partition-datatype text \
  --env .env

# Add all test queries for landkreis
for query in testqueries_examples/landkreis/*.sql; do
  pcache-add --direct \
    --query-file "$query" \
    --partition-key landkreis \
    --partition-datatype text \
    --cache-backend postgresql_array \
    --env .env
done
```

#### For Spatial Partition (Geometry)

Spatial cache uses PostGIS geometry instead of discrete partition keys. Queries with `ST_DWithin` spatial joins benefit from geometric intersection filtering. The `--geometry-column` flag tells `pcache-add` which column to use for spatial cell derivation (H3 hexagons or BBox grid cells).

```bash
# H3 backend
pcache-add --direct \
  --query-file testqueries_examples/spatial/q1.sql \
  --partition-key spatial \
  --partition-datatype h3 \
  --cache-backend postgis_h3 \
  --geometry-column geom \
  --env .env

# BBox backend
pcache-add --direct \
  --query-file testqueries_examples/spatial/q1.sql \
  --partition-key spatial \
  --partition-datatype bbox \
  --cache-backend postgis_bbox \
  --geometry-column geom \
  --env .env

# Add all spatial test queries
for query in testqueries_examples/spatial/*.sql; do
  pcache-add --direct \
    --query-file "$query" \
    --partition-key spatial \
    --partition-datatype h3 \
    --cache-backend postgis_h3 \
    --geometry-column geom \
    --env .env
done
```

**Note on buffer distance**: The `--buffer-distance` parameter is a read-time concern — it controls how much cached envelopes are expanded when `apply_cache` / `apply_cache_lazy` intersects them to build the spatial filter. It is **not** used during cache population (`pcache-add`). When omitted at query time, PartitionCache auto-derives it from `ST_DWithin` distances in the query using the weighted graph diameter. For example, a query with `ST_DWithin(p1.geom, p3.geom, 300) AND ST_DWithin(p1.geom, p2.geom, 400)` auto-computes a buffer of 700m (path p2→p1→p3 = 400+300).

```python
# Python API: buffer_distance is used at apply_cache time, not at population time
enhanced_query, stats = partitioncache.apply_cache(
    query=sql_query,
    cache_handler=handler,
    partition_key="spatial",
    geometry_column="geom",
    buffer_distance=500,       # explicit override
    # buffer_distance=None,    # or omit to auto-derive from ST_DWithin
)
```

### Reading from Cache

```bash
# Read zipcode partition keys for a query
pcache-read \
  --query-file testqueries_examples/zipcode/q1.sql \
  --partition-key zipcode \
  --partition-datatype integer \
  --cache-backend postgresql_array \
  --env-file .env

# Read landkreis partition keys for a query
pcache-read \
  --query-file testqueries_examples/landkreis/q1.sql \
  --partition-key landkreis \
  --partition-datatype text \
  --cache-backend postgresql_array \
  --env-file .env
```

### Queue Monitoring

Start the queue monitor to process queries asynchronously:

```bash
pcache-monitor \
  --cache-backend postgresql_array \
  --db-backend postgresql \
  --env .env
```

### Cache Management

```bash
# Count cache entries (uses CACHE_BACKEND from environment)
pcache-manage cache count --env .env

# Count queue entries
pcache-manage queue count --env .env

# Clear specific partition (uses CACHE_BACKEND from environment)
pcache-manage maintenance partition --delete zipcode \
  --env .env
```

## Example Queries

Each partition strategy (`zipcode`, `landkreis`, `spatial`) has three test queries in `testqueries_examples/`:

1. **q1.sql**: Find ice cream shops near pharmacies and ALDI supermarkets
2. **q2.sql**: Find ice cream shops near swimming pools and Edeka supermarkets
3. **q3.sql**: Find ice cream shops near swimming pools and ALDI supermarkets

The `zipcode` and `landkreis` variants include partition key join conditions (e.g. `p2.zipcode = p1.zipcode`), while the `spatial` variants use only `ST_DWithin` spatial joins without partition key joins.

## Performance Comparison

The example demonstrates how different partition strategies affect query performance:

- **Zipcode partitioning**: Fine-grained, good for local searches
- **Landkreis partitioning**: Broader regions, good for administrative queries
- **Spatial partitioning** (H3/BBox): Geometry-based, good for spatial proximity queries without explicit partition key columns

Run `python run_poi_queries.py` to see performance comparisons between:
- Queries without cache
- Queries with zipcode partitioning
- Queries with landkreis partitioning
- Queries with spatial partitioning (H3 or BBox)

## Automated Workflow Test

The `test_workflow.py` script demonstrates a complete end-to-end PartitionCache workflow:

```bash
python test_workflow.py
```

This sets up the PostgreSQL queue processor via pg_cron, adds queries to the processing queue, waits for automatic processing, and verifies cache effectiveness. It requires the database to be running and OSM data imported.

## Cache Backend Options

The example supports multiple cache backends:

```bash
# PostgreSQL Array (supports all datatypes)
CACHE_BACKEND=postgresql_array

# PostgreSQL Bit (integers only, memory efficient)
CACHE_BACKEND=postgresql_bit

# RocksDB (file-based, good for development)
CACHE_BACKEND=rocksdb_set

# Redis (in-memory, good for distributed setups)
CACHE_BACKEND=redis_set

# PostGIS H3 (spatial partitioning with hexagonal cells)
SPATIAL_CACHE_BACKEND=postgis_h3

# PostGIS BBox (spatial partitioning with grid-based bounding boxes)
SPATIAL_CACHE_BACKEND=postgis_bbox
```

Spatial backends require PostGIS and h3-pg extensions. Configure via `PG_H3_*` / `PG_BBOX_*` env vars in `.env` (see `.env.example`).

## Troubleshooting

### Database Connection Issues
- Ensure PostgreSQL is running: `docker-compose ps`
- Check connection settings in `.env`

### Cache Compatibility Issues
- Use `pcache-add` without arguments to see datatype compatibility
- PostgreSQL Array supports all datatypes
- Bit-based handlers only support integers

### Performance Issues
- Ensure database indexes are created (done automatically)
- Consider using queue-based processing for large datasets
- Monitor cache hit rates in query output



# NYC Taxi Benchmark for PartitionCache

This example demonstrates PartitionCache on **real-world NYC taxi data with spatial queries**. Unlike synthetic benchmarks (TPC-H, SSB), this uses actual GPS coordinates from pre-2016 TLC Yellow Taxi data and OpenStreetMap POIs, enabling genuinely expensive spatial joins that showcase PartitionCache's value.

**Key design**: Each taxi trip is one mini partition (`trip_id` as partition key). Every query has 3 independent parts (start point, trip data, end point), each with 1-10% selectivity, giving 0.001-0.1% overall selectivity.

**Key feature**: The benchmark uses **original analytical queries** (with EXISTS, GROUP BY, ORDER BY). PartitionCache's variant generation (`keep_all_attributes=False`) automatically decomposes queries into cacheable fragments by omitting individual conditions — no manual query rewriting needed.

## Quick Start

```bash
# 1. Start PostgreSQL with PostGIS
docker compose up -d

# 2. Copy and edit environment config
cp .env.example .env
# Edit .env with your PostgreSQL credentials (defaults match docker-compose.yml)

# 3. Generate data (1 month ~ 14M trips)
python generate_nyc_taxi_data.py --months 1 --year 2010 --month 1

# 4. Run full benchmark
python run_nyc_taxi_benchmark.py --mode all --output results.json

# 5. Compare query extension methods
python run_nyc_taxi_benchmark.py --mode method-comparison

# 6. Cross-dimension reuse evaluation
python run_nyc_taxi_benchmark.py --mode cross-dimension

# 7. Hierarchical drill-down evaluation
python run_nyc_taxi_benchmark.py --mode hierarchy

# 8. Compare non-lazy vs lazy API with all methods
python run_nyc_taxi_benchmark.py --mode api-comparison

# 9. Backend comparison with both API paths
python run_nyc_taxi_benchmark.py --mode backend-comparison --api both

# 10. Run with lazy API (database-side intersection)
python run_nyc_taxi_benchmark.py --mode all --api lazy --method IN_SUBQUERY
```

## Prerequisites

- Docker and Docker Compose (for PostgreSQL + PostGIS)
- Python packages: `duckdb`, `psycopg[binary]`, `python-dotenv`

```bash
pip install duckdb psycopg[binary] python-dotenv
```

### Docker Setup

The included `docker-compose.yml` provides PostgreSQL 15 with PostGIS 3.4:

```bash
docker compose up -d
```

Configuration details:
- **Image**: `postgis/postgis:15-3.4`
- **Port**: `127.0.0.1:5433:5432` (localhost only, mapped to 5433 to avoid conflicts)
- **Credentials**: `app_user` / `secure_password` / `nyc_taxi_db`
- **Shared memory**: `shm_size: 1g` (required for complex spatial queries with PostGIS; default 64MB causes `No space left on device` errors)
- **Storage**: Named volume `pgdata` for persistence

## PartitionCache Workflow

The benchmark demonstrates the complete PartitionCache API workflow using **original SQL queries** (no manual adaptation required):

### 1. Cache Population

```python
from partitioncache.query_processor import generate_all_query_hash_pairs

# Generate variant fragments by omitting individual conditions
pairs = generate_all_query_hash_pairs(
    query=original_sql,
    partition_key="trip_id",
    keep_all_attributes=False,  # Creates variants by omitting conditions
    strip_select=True,
    min_component_size=1,
    auto_detect_partition_join=False,
    skip_partition_key_joins=True,
    follow_graph=False,
)

# Execute each fragment and store results
for fragment_sql, hash_val in pairs:
    result_set = execute(fragment_sql)  # Returns set of trip_ids
    cache_helper.set_entry(
        key=hash_val,
        partition_key_identifiers=result_set,
        query_text=fragment_sql,
    )
```

**How variant generation works**: Given a query with conditions A, B, C, `keep_all_attributes=False` generates fragments for {A,B,C}, {A,B}, {A,C}, {B,C} etc. Each fragment is independently cacheable and reusable across queries sharing those conditions.

### 2. Cache Application

```python
import partitioncache

# Look up cached partition keys (intersection of all matching fragments)
cached_keys, num_variants, num_hits = partitioncache.get_partition_keys(
    query=original_sql,
    cache_handler=cache_helper.underlying_handler,
    partition_key="trip_id",
    min_component_size=1,
    auto_detect_partition_join=False,
    skip_partition_key_joins=True,
    follow_graph=False,
)

# Extend the original query with cache restriction
enhanced_sql = partitioncache.extend_query_with_partition_keys(
    original_sql, cached_keys,
    partition_key="trip_id",
    method="IN",       # Also: VALUES, TMP_TABLE_IN, TMP_TABLE_JOIN
    p0_alias="t",      # Alias of the fact table
)
```

### 3. Non-Lazy vs Lazy API

The benchmark supports two API paths controlled by the `--api` flag:

| API | How It Works | Trade-off |
|-----|-------------|-----------|
| `non-lazy` (default) | Python materializes partition key sets, intersects them, then embeds results in SQL | Full control, enables search space reduction stats |
| `lazy` | Generates SQL subqueries that PostgreSQL evaluates at query time (no Python-side materialization) | Avoids transferring large key sets; database optimizer can combine cache lookup with query execution |

**Non-lazy** uses `get_partition_keys()` + `extend_query_with_partition_keys()` — the cached partition key IDs are fetched into Python, intersected, then injected into the query as an IN list or temp table.

**Lazy** uses `apply_cache_lazy()` — generates a SQL subquery (e.g., `SELECT pk FROM cache WHERE hash IN (...)`) that PostgreSQL evaluates as part of the enhanced query. The partition keys never leave the database.

Use `--mode api-comparison` to benchmark both paths with all applicable methods on representative queries.

### 4. Query Extension Methods

The `--method` flag controls how cached partition keys are applied to queries:

| Method | API | SQL Pattern | Best For |
|--------|-----|-----------|----------|
| `IN` (default) | non-lazy | `WHERE t.trip_id IN (1, 2, 3, ...)` | Small-medium key sets (<500K) |
| `VALUES` | non-lazy | `WHERE t.trip_id IN (VALUES(1),(2),(3),...)` | Medium key sets |
| `TMP_TABLE_IN` | both | `CREATE TEMP TABLE ...; WHERE t.trip_id IN (SELECT ...)` | Large key sets |
| `TMP_TABLE_JOIN` | both | `CREATE TEMP TABLE ...; INNER JOIN tmp ON ...` | Very large key sets |
| `IN_SUBQUERY` | lazy | `WHERE t.trip_id IN (SELECT ... FROM cache ...)` | Default lazy method |

## Data Sources

### TLC Yellow Taxi (pre-2016)
- **URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- **Columns**: Actual GPS coordinates (`pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`) -- present in 2009-2015 data, removed July 2016
- **Volume**: ~14M trips/month (2013-2015)
- **Quality filters**: NYC bbox, non-zero coordinates, positive distance/fare/duration

### OpenStreetMap POIs
- Downloaded via Overpass API for the NYC bounding box
- 10 POI types: restaurant, hospital, hotel, museum, attraction, station, bar, park, theatre, university

## Data Model (PostgreSQL + PostGIS)

### Fact Table: `taxi_trips`
| Column | Type | Description |
|--------|------|-------------|
| trip_id | INTEGER PK | Sequential ID |
| pickup_datetime | TIMESTAMP | Pickup time |
| dropoff_datetime | TIMESTAMP | Dropoff time |
| pickup_geom | GEOMETRY(Point, 32118) | Pickup GPS point |
| dropoff_geom | GEOMETRY(Point, 32118) | Dropoff GPS point |
| passenger_count | INTEGER | |
| trip_distance | DOUBLE PRECISION | Miles (taximeter) |
| fare_amount | DOUBLE PRECISION | |
| tip_amount | DOUBLE PRECISION | |
| tolls_amount | DOUBLE PRECISION | |
| total_amount | DOUBLE PRECISION | |
| payment_type | INTEGER | |
| rate_code_id | INTEGER | |
| duration_seconds | INTEGER | Derived |
| pickup_hour | INTEGER | Derived |
| is_rush_hour | BOOLEAN | Derived (7-10am or 4-7pm weekdays) |
| is_weekend | BOOLEAN | Derived |

### Dimension: `osm_pois`
| Column | Type | Description |
|--------|------|-------------|
| poi_id | INTEGER PK | OSM node ID |
| name | VARCHAR | POI name |
| poi_type | VARCHAR | restaurant, hotel, hospital, museum, station, bar, park, theatre, university, attraction |
| poi_category | VARCHAR | food, accommodation, medical, attraction, transport, nightlife, recreation, culture, education |
| geom | GEOMETRY(Point, 32118) | Location |

> **Note:** Coordinates are transformed from WGS84 (EPSG:4326) to NYS State Plane (EPSG:32118) during data generation for metric-distance spatial queries.

**No bridge/proximity table** -- spatial distances are computed live via `ST_DWithin()`, making PartitionCache valuable.

## Scale Options

| Config | Months | Approx Trips | Notes |
|--------|--------|-------------|-------|
| small | 1 | ~14M | Quick testing (Jan 2010) |
| medium | 3 | ~42M | Standard benchmark |
| large | 12 | ~170M | Full year evaluation |

## Query Structure

Queries use standard SQL with EXISTS subqueries, GROUP BY, and ORDER BY. Example (q1_1):

```sql
SELECT t.trip_id, t.pickup_datetime, t.total_amount, ...
FROM taxi_trips t, osm_pois p_start
WHERE ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'museum'
  AND t.duration_seconds > 2700
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE ST_DWithin(t.dropoff_geom, p_end.geom, 200)
      AND p_end.poi_type = 'hotel'
  )
GROUP BY t.trip_id, ...
ORDER BY t.total_amount DESC
LIMIT 20
```

PartitionCache decomposes this into cacheable fragments by omitting individual conditions (e.g., removing the EXISTS, removing the duration filter), enabling reuse when other queries share any subset of these conditions.

## Query Flights (8 flights, 25 queries)

| Flight | Theme | Queries | Description |
|--------|-------|---------|-------------|
| 1 | Tourism | q1_1 - q1_3 | Museum pickups, hotel/subway dropoffs |
| 2 | Medical/Emergency | q2_1 - q2_3 | Hospital-to-hospital, night/congested |
| 3 | Commuter | q3_1 - q3_3 | Subway-to-subway, long commutes |
| 4 | Nightlife | q4_1 - q4_3 | Bar pickups, late-night patterns |
| 5 | Start Hierarchy | q5_1 - q5_3 | Drill-down: park -> museum -> theatre |
| 6 | Trip Hierarchy | q6_1 - q6_3 | Drill-down: anomaly -> +far -> +long |
| 7 | Cross-Dimension | q7_1 - q7_4 | All conditions previously cached |
| 8 | Max Complexity | q8_1 - q8_3 | Multiple stacked conditions |

## Cache Reuse Matrix

| Fragment | First Used | Reused By |
|----------|-----------|-----------|
| S_MUSEUM(200m) | q1_1 | q1_2, q1_3, q5_2, q7_1, q8_1 |
| S_HOSPITAL(300m) | q2_1 | q2_2, q2_3, q7_2 |
| S_SUBWAY(200m) | q3_1 | q3_2, q3_3, q6_1-q6_3, q8_2 |
| S_BAR(150m) | q4_1 | q4_2, q4_3, q7_3, q8_3 |
| S_PARK(300m) | q5_1 | q7_4 |
| T_LONG(>45min) | q1_1 | q2_3, q5_1-q5_3, q6_3, q7_3, q8_1 |
| T_INDIRECT(>3x) | q1_2 | q4_3, q7_2, q8_1 |
| T_EXPENSIVE(>$8/mi) | q1_3 | q4_2, q7_4, q8_3 |
| T_NIGHT(1-4am) | q2_1 | q4_1, q7_1, q8_1, q8_3 |
| T_CONGESTED(<5mph) | q2_2 | q3_2, q8_2 |
| T_FAR(>10mi) | q3_1 | q6_2, q6_3, q8_2 |
| T_ANOMALY(>2x) | q3_3 | q6_1-q6_3, q8_2 |
| E_HOTEL(200m) | q1_1 | q1_2, q2_3, q3_2, q4_1, q5_1-q5_3, q8_3 |
| E_SUBWAY(200m) | q1_3 | q4_2, q6_1-q6_3 |
| E_HOSPITAL(300m) | q2_1 | q2_2, q7_1, q8_2 |
| E_PARK(300m) | q3_3 | q7_4 |
| E_BAR(150m) | q4_3 | q8_3 |

## Why This Tests PartitionCache Well

1. **ST_DWithin spatial joins are expensive**: Each start/end subquery joins millions of trip points against thousands of POIs via spatial index. Even with GiST indexes, this takes seconds. Caching avoids recomputation.

2. **ST_Distance per-row computation is expensive**: Trip anomaly detection (T_INDIRECT, T_ANOMALY) computes geodesic distance for every row. Caching avoids redoing this.

3. **Massive cross-query reuse**: S_MUSEUM appears in 6 queries, T_LONG in 8, E_HOTEL in 9. Each computed once, reused many times.

4. **3-part structure = 3 independent cache dimensions**: Start, trip, and end conditions are fully independent, enabling partial cache hits.

5. **Real data distribution**: Manhattan-heavy pickup distribution tests cache under realistic skew.

6. **Hierarchy demonstration**: Flights 5-6 show that only the drill-target dimension changes while constant dimensions are fully cached.

## Benchmark Modes

| Mode | Description |
|------|-------------|
| `--mode all` | All 25 queries |
| `--mode flight N` | Single flight (1-8) |
| `--mode cross-dimension` | F1-F4 populate, then F7 shows cross-reuse |
| `--mode cold-vs-warm` | F7 with cold vs warm cache |
| `--mode hierarchy` | F5 + F6 with level-by-level reuse tracking |
| `--mode spatial` | Only spatial-heavy queries |
| `--mode backend-comparison` | Compare postgresql_array vs _bit vs _roaringbit (supports `--api both`) |
| `--mode method-comparison` | Compare IN vs VALUES vs TMP_TABLE_IN vs TMP_TABLE_JOIN |
| `--mode api-comparison` | Compare non-lazy vs lazy API with all applicable methods |

## Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--cache-backend` | Cache backend | postgresql_array |
| `--mode` | Benchmark mode | all |
| `--method` | Query extension method: IN, VALUES, TMP_TABLE_IN, TMP_TABLE_JOIN, IN_SUBQUERY | IN |
| `--api` | API path: non-lazy, lazy, both | non-lazy |
| `--repeat N` | Repeat queries N times for stable timing | 1 |
| `--output FILE` | Save results to JSON file | None |

## Benchmark Results (1 Month / 14.5M Trips)

Results from running `--mode all` with `postgresql_array` backend and `IN` method on January 2010 data (14,483,721 trips, 5,872 POIs). PostgreSQL 15 with PostGIS 3.4, SRID 32118 (NYS State Plane, metric distances). Original queries with automatic variant generation.

### Summary Table

| Query | Baseline | Cached | Speedup | Reduction | Match |
|-------|----------|--------|---------|-----------|-------|
| q1_1 | 1.36s | 0.59s | **2.3x** | 99.9% | Y |
| q1_2 | 10.08s | 2.71s | **3.7x** | 98.9% | Y |
| q1_3 | 9.80s | 5.81s | **1.7x** | 97.2% | Y |
| q2_1 | 2.95s | 10.53s | 0.3x | 91.8% | Y |
| q2_2 | 2.10s | 0.03s | **74.2x** | 100.0% | Y |
| q2_3 | 0.65s | 0.58s | **1.1x** | 99.9% | Y |
| q3_1 | 3.91s | 1.25s | **3.1x** | 99.5% | Y |
| q3_2 | 1.04s | 0.16s | **6.3x** | 100.0% | Y |
| q3_3 | 25.92s | 20.64s | **1.3x** | 91.3% | Y |
| q4_1 | 10.29s | 2.87s | **3.6x** | 98.1% | Y |
| q4_2 | 7.96s | 6.04s | **1.3x** | 97.2% | Y |
| q4_3 | 5.24s | 2.46s | **2.1x** | 98.9% | Y |
| q5_1 | 0.93s | 0.47s | **2.0x** | 99.9% | Y |
| q5_2 | 1.14s | 0.41s | **2.8x** | 99.9% | Y |
| q5_3 | 1.76s | 0.40s | **4.4x** | 99.9% | Y |
| q6_1 | 21.49s | 7.85s | **2.7x** | 95.7% | Y |
| q6_2 | 1.27s | 0.33s | **3.8x** | 99.9% | Y |
| q6_3 | 0.33s | 0.11s | **2.8x** | 100.0% | Y |
| q7_1 | 3.26s | 2.86s | **1.1x** | 98.5% | Y |
| q7_2 | 7.19s | 2.83s | **2.5x** | 98.8% | Y |
| q7_3 | 1.26s | 1.15s | **1.1x** | 99.8% | Y |
| q7_4 | 8.66s | 7.70s | **1.1x** | 95.9% | Y |
| q8_1 | 0.06s | 0.01s | **4.3x** | 100.0% | Y |
| q8_2 | 0.54s | 0.003s | **163.0x** | 100.0% | Y |
| q8_3 | 1.03s | 0.54s | **1.9x** | 99.9% | Y |

**Average speedup: 11.8x** | All results correct: **Yes (25/25)**

### Method Comparison (q1_1, q1_2, q3_1)

| Method | q1_1 | q1_2 | q3_1 |
|--------|------|------|------|
| IN | **2.5x** | **3.9x** | **3.3x** |
| VALUES | 1.1x | 2.0x | 2.9x |
| TMP_TABLE_IN | 1.1x | 2.0x | 1.7x |
| TMP_TABLE_JOIN | 1.1x | 2.1x | 1.9x |

The `IN` method consistently outperforms temp table approaches for this workload because PostgreSQL optimizes `WHERE trip_id IN (...)` better than temp table joins when the query already contains complex EXISTS subqueries.

### Key Observations

1. **Extreme speedups on highly selective queries**: q8_2 (163x) and q2_2 (74x) achieve near-instant execution by reducing 14.5M trips to just 10 and 1,214 rows respectively through cache intersection.

2. **Consistent speedups across query types**: 24/25 queries show speedup >= 1x. Tourism, commuter, nightlife, and hierarchy queries all benefit from cache reuse.

3. **Cache reuse across flights**: Later queries (F5-F8) find most fragments already cached from earlier flights (F1-F4), with near-zero population time for reused fragments.

4. **One slowdown case**: q2_1 (0.3x) has a cached set of 1.2M keys (91.8% reduction). With the `IN` method, the `WHERE trip_id IN (...)` clause with 1.2M values is expensive to parse and evaluate. The `TMP_TABLE_IN` or `TMP_TABLE_JOIN` methods may be better for such large key sets.

5. **Search space reduction consistently >91%**: Even the worst case (q3_3 at 91.3%) eliminates most rows. Most queries achieve 97-100% reduction.

6. **All results match**: 25/25 queries produce identical results with and without caching, validating correctness.

## File Structure

```
examples/nyc_taxi_benchmark/
  docker-compose.yml              # PostgreSQL + PostGIS container
  generate_nyc_taxi_data.py       # Download taxi Parquet + OSM POIs -> PostgreSQL
  run_nyc_taxi_benchmark.py       # Benchmark runner
  .env.example                    # PostgreSQL + cache backend config
  README.md                       # This file
  queries/
    original/                     # 25 analytical queries (EXISTS, GROUP BY, ORDER BY)
      q1_1.sql .. q8_3.sql
    adapted/                      # 25 PartitionCache-ready queries (IN-subqueries)
      q1_1.sql .. q8_3.sql
```

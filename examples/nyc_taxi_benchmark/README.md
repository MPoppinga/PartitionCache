# NYC Taxi Benchmark for PartitionCache

This example demonstrates PartitionCache on **real-world NYC taxi data with spatial queries**. Unlike synthetic benchmarks (TPC-H, SSB), this uses actual GPS coordinates from pre-2016 TLC Yellow Taxi data and OpenStreetMap POIs, enabling genuinely expensive spatial joins that showcase PartitionCache's value.

**Key design**: Each taxi trip is one mini partition (`trip_id` as partition key). Every query has 3 independent parts (start point, trip data, end point), each with 1-10% selectivity, giving 0.001-0.1% overall selectivity.

## Quick Start

```bash
# 1. Copy and edit environment config
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 2. Generate data (1 month ~ 14M trips)
python generate_nyc_taxi_data.py --months 1 --year 2010 --month 1

# 3. Run benchmark
python run_nyc_taxi_benchmark.py --mode all

# 4. Cross-dimension reuse evaluation
python run_nyc_taxi_benchmark.py --mode cross-dimension

# 5. Hierarchical drill-down evaluation
python run_nyc_taxi_benchmark.py --mode hierarchy

# 6. Compare cache backends
python run_nyc_taxi_benchmark.py --mode backend-comparison
```

## Prerequisites

- PostgreSQL with **PostGIS** extension
- Python packages: `duckdb`, `psycopg[binary]`, `python-dotenv`

```bash
pip install duckdb psycopg[binary] python-dotenv
```

## Data Sources

### TLC Yellow Taxi (pre-2016)
- **URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- **Columns**: Actual GPS coordinates (`pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`) — present in 2009-2015 data, removed July 2016
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

**No bridge/proximity table** — spatial distances are computed live via `ST_DWithin()`, making PartitionCache valuable.

## Scale Options

| Config | Months | Approx Trips | Notes |
|--------|--------|-------------|-------|
| small | 1 | ~14M | Quick testing (Jan 2010) |
| medium | 3 | ~42M | Standard benchmark |
| large | 12 | ~170M | Full year evaluation |

## 3-Part Query Structure

Every adapted query has exactly 3 (or more) IN-subqueries on `trip_id`:

```sql
SELECT t.trip_id
FROM taxi_trips t
WHERE t.trip_id IN (
    -- PART 1: START POINT (pickup near POI type)
    SELECT t2.trip_id FROM taxi_trips t2, osm_pois p
    WHERE ST_DWithin(t2.pickup_geom::geography, p.geom::geography, 200)
      AND p.poi_type = 'museum'
)
AND t.trip_id IN (
    -- PART 2: TRIP DATA (duration/distance/anomaly condition)
    SELECT t3.trip_id FROM taxi_trips t3
    WHERE t3.duration_seconds > 2700
)
AND t.trip_id IN (
    -- PART 3: END POINT (dropoff near POI type)
    SELECT t4.trip_id FROM taxi_trips t4, osm_pois p
    WHERE ST_DWithin(t4.dropoff_geom::geography, p.geom::geography, 200)
      AND p.poi_type = 'hotel'
)
```

Each part is independently cacheable. Start, trip, and end conditions are fully independent — any query combining a previously-cached start with a new trip and a previously-cached end gets 2/3 cache hits.

## Query Flights (8 flights, 25 queries)

| Flight | Theme | Queries | Description |
|--------|-------|---------|-------------|
| 1 | Tourism | q1_1 - q1_3 | Museum pickups, hotel/subway dropoffs |
| 2 | Medical/Emergency | q2_1 - q2_3 | Hospital-to-hospital, night/congested |
| 3 | Commuter | q3_1 - q3_3 | Subway-to-subway, long commutes |
| 4 | Nightlife | q4_1 - q4_3 | Bar pickups, late-night patterns |
| 5 | Start Hierarchy | q5_1 - q5_3 | Drill-down: park → museum → theatre |
| 6 | Trip Hierarchy | q6_1 - q6_3 | Drill-down: anomaly → +far → +long |
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
| `--mode backend-comparison` | Compare postgresql_array vs _bit vs _roaringbit |

## Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--cache-backend` | Cache backend | postgresql_array |
| `--mode` | Benchmark mode | all |
| `--repeat N` | Repeat queries N times for stable timing | 1 |
| `--output FILE` | Save results to JSON file | None |

## File Structure

```
examples/nyc_taxi_benchmark/
  generate_nyc_taxi_data.py       # Download taxi Parquet + OSM POIs → PostgreSQL
  run_nyc_taxi_benchmark.py       # Benchmark runner
  .env.example                    # PostgreSQL + cache backend config
  README.md                       # This file
  queries/
    original/                     # 25 analytical queries (joins, GROUP BY, aggregation)
      q1_1.sql .. q8_3.sql
    adapted/                      # 25 PartitionCache-ready queries (IN-subqueries)
      q1_1.sql .. q8_3.sql
```

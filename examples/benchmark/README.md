# Unified Benchmark Runner

A single config-driven benchmark script for all PartitionCache workloads: SSB, TPC-H, NYC Taxi, and OSM POI.

## Usage

```bash
python examples/benchmark/run_benchmark.py --config config/<workload>.yaml [OPTIONS]
```

## Available Configs

| Config | Workload | DB Backends | Partition Keys |
|--------|----------|-------------|----------------|
| `config/ssb.yaml` | Star Schema Benchmark | DuckDB, PostgreSQL | lo_custkey, lo_suppkey, lo_partkey, lo_orderdate |
| `config/tpch.yaml` | TPC-H | DuckDB, PostgreSQL | l_orderkey, l_partkey, l_suppkey |
| `config/nyc_taxi.yaml` | NYC Taxi + Spatial | PostgreSQL | trip_id |
| `config/osm_poi.yaml` | OpenStreetMap POI | PostgreSQL | zipcode, landkreis, spatial |

## Per-Workload Setup

### SSB (Star Schema Benchmark)

```bash
# Generate data
python examples/ssb_benchmark/generate_ssb_data.py --scale-factor 0.01 --db-backend duckdb

# Run benchmark
python examples/benchmark/run_benchmark.py --config config/ssb.yaml --db-backend duckdb --mode all
```

### TPC-H

```bash
# Generate data (uses DuckDB built-in dbgen)
python examples/tpch_benchmark/generate_tpch_data.py --scale-factor 0.01 --db-backend duckdb

# Run benchmark
python examples/benchmark/run_benchmark.py --config config/tpch.yaml --db-backend duckdb --mode all
```

### NYC Taxi (requires PostGIS)

```bash
# Start PostgreSQL + PostGIS
docker compose -f examples/nyc_taxi_benchmark/docker-compose.yml up -d

# Configure environment
cp examples/nyc_taxi_benchmark/.env.example examples/nyc_taxi_benchmark/.env

# Generate data (downloads TLC Parquet + OSM POIs)
python examples/nyc_taxi_benchmark/generate_nyc_taxi_data.py --months 1 --year 2010 --month 1

# Run benchmark
python examples/benchmark/run_benchmark.py --config config/nyc_taxi.yaml --mode all
```

### OSM POI (requires PostGIS + osmium)

```bash
# Start PostgreSQL + PostGIS
docker-compose -f examples/openstreetmap_poi/docker-compose.yml up -d

# Configure environment
cp examples/openstreetmap_poi/.env.example examples/openstreetmap_poi/.env

# Import OSM data
python examples/openstreetmap_poi/process_osm_data.py

# Run benchmark
python examples/benchmark/run_benchmark.py --config config/osm_poi.yaml --db-backend postgresql --mode all
```

## Benchmark Modes

| Mode | Description |
|------|-------------|
| `--mode all` | All queries with per-query and per-partition-key metrics |
| `--mode flight N` | Only flight N queries |
| `--mode cross-dimension` | Cross-dimension cache reuse evaluation |
| `--mode cold-vs-warm` | Compare cold cache vs warm cache performance |
| `--mode hierarchy` | Hierarchical drill-down with reuse tracking |
| `--mode spatial` | Spatial-heavy queries |
| `--mode spatial-method-comparison` | Compare SUBDIVIDE_INLINE vs SUBDIVIDE_TMP_TABLE for each spatial backend |
| `--mode backend-comparison` | Compare cache backends |
| `--mode method-comparison` | Compare query extension methods (IN, VALUES, TMP_TABLE_*) |
| `--mode api-comparison` | Compare non-lazy vs lazy API paths |

## Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to YAML config file | Required |
| `--db-backend` | Database backend: `duckdb` or `postgresql` | First enabled in config |
| `--cache-backend` | Cache backend override | First in config's backends list |
| `--scale-factor SF` | Scale factor (SSB/TPC-H) | 0.01 |
| `--db-path` | DuckDB file path override | From config template |
| `--method` | Query extension method: IN, VALUES, TMP_TABLE_IN, TMP_TABLE_JOIN, IN_SUBQUERY | TMP_TABLE_IN |
| `--api` | API path: non-lazy, lazy, both | non-lazy |
| `--repeat N` | Repeat baseline/cached queries N times for stable timing | 1 |
| `--output FILE` | Save results to JSON file | None |

## Query Extension Methods

| Method | API | SQL Pattern | Best For |
|--------|-----|-------------|----------|
| `IN` | non-lazy | `WHERE pk IN (1, 2, 3, ...)` | Small-medium key sets |
| `VALUES` | non-lazy | `WHERE pk IN (VALUES(1),(2),(3),...)` | Medium key sets |
| `TMP_TABLE_IN` | both | `CREATE TEMP TABLE ...; WHERE pk IN (SELECT ...)` | Large key sets |
| `TMP_TABLE_JOIN` | both | `CREATE TEMP TABLE ...; INNER JOIN tmp ON ...` | Very large key sets |
| `IN_SUBQUERY` | lazy | `WHERE pk IN (SELECT ... FROM cache ...)` | Default lazy method |

## JSON Output

When `--output` is specified, results are saved with metadata:

```json
{
  "metadata": {
    "mode": "all",
    "db_backend": "duckdb",
    "cache_backend": "duckdb_bit",
    "scale_factor": 0.01,
    "repeat": 1,
    "timestamp": "2026-02-23T12:00:00+00:00"
  },
  "results": [...]
}
```

## YAML Config Format

Each workload is defined by a YAML config file. Key sections:

- **`fact_table`** / **`p0_alias`**: Fact table name and SQL alias
- **`partition_keys`**: Map of partition key names to datatypes (`integer`, `text`, `geometry`)
- **`query_partition_keys`**: Map of query name to list of applicable partition keys
- **`query_flights`**: Numbered groups of queries for sequential execution
- **`query_processing`**: Parameters for `generate_all_query_hash_pairs()` (keep_all_attributes, follow_graph, strip_select, etc.)
- **`fragment_filter`**: FROM clause filtering (require/exclude tables)
- **`cache`**: Table prefix and bitsize strategy (`fixed`, `scale_factor`, `data_driven`)
- **`modes`**: Mode-specific configuration (cross-dimension flights, hierarchy flights, etc.)

See existing configs in `config/` for complete examples.

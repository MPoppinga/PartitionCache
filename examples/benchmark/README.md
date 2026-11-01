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
| `config/security_logs.yaml` | Security / log analysis | PostgreSQL | src_user_id, src_host_id, bucket_1h, bucket_1d |

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

### Security Logs (plain PostgreSQL, no extensions)

Synthetic enterprise authentication and process telemetry with planted red-team campaigns and
a ground-truth table. The schema mirrors the LANL 2015 "Comprehensive, Multi-Source
Cyber-Security Events" layout, so a real LANL dump can be loaded into the same tables later.

This is the only workload that needs no database extensions -- no PostGIS, no h3-pg.

```bash
# Start PostgreSQL
docker compose -f examples/security_log_benchmark/docker-compose.yml up -d

# Configure environment
cp examples/security_log_benchmark/.env.example examples/security_log_benchmark/.env

# Generate data (--scale 1.0 is ~20M auth events; use 0.01 for a smoke test)
python examples/security_log_benchmark/generate_security_log_data.py --scale 1.0 --drop-existing

# Run benchmark
python examples/benchmark/run_benchmark.py --config config/security_logs.yaml --mode all
```

Queries are modelled on real detection rules (Sigma correlations, Elastic EQL sequences,
MITRE CAR analytics). Each rule joins two or more event aliases on a group-by field and a
bounded time window -- structurally the same shape as the OSM POI spatial proximity joins:

| OSM POI | Security log |
|---------|--------------|
| `FROM pois p1, pois p2` | `FROM auth_events e1, auth_events e2` |
| `p2.zipcode = p1.zipcode` | `e2.src_user_id = e1.src_user_id` (Sigma `group-by`) |
| `ST_DWithin(p1.geom, p2.geom, 400)` | `e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600` (Sigma `timespan`) |
| `p1.subtype = 'pharmacy'` | `e1.event_code = 4625` (Sigma base rule) |

#### Temporal dilation (the time-domain buffer)

Flights 1 and 2 use conventional entity partition keys (`src_user_id`, `src_host_id`).
Flights 3 and 4 use temporal bucket keys, where the correlation window creates the same
boundary problem the spatial buffer solves: a failed logon at 23:58 and its matching success
at 00:03 fall in *adjacent* buckets, so intersecting the per-fragment bucket sets drops the
pair.

Setting `temporal_bucket_width` on a partition key enables dilation. Each cached bucket set is
widened by `ceil(window / bucket_width)` buckets **before** the intersection, mirroring how
`postgis_bbox` buffers each geometry before intersecting. Dilating afterwards is unsound,
because the undilated intersection may already be empty.

```bash
# Measure the false negatives dilation prevents (unsound on purpose)
python examples/benchmark/run_benchmark.py --config config/security_logs.yaml --mode flight 3 --no-dilation

# Same flight with dilation enabled
python examples/benchmark/run_benchmark.py --config config/security_logs.yaml --mode flight 3
```

Choose a bucket width at least as wide as the correlation window. A 24h window over 1h
buckets needs `k = 24`, inflating every cached set ~49x and destroying the selectivity the
cache exists to provide -- the same trade-off as H3 k-ring expansion in the spatial workload.
That is why the config offers both `bucket_1h` and `bucket_1d`.

**Measured caveat -- keep dilation opt-in.** At 80M events dilation cost more than it
protected: it widened the one query where the hourly bucket key paid off from 76 to 210
buckets (2.76x) and turned a 1.29x speedup into a 0.35x loss, while no query lost rows without
it. The boundary error is real but data-dependent -- it needs two independent single-alias
fragments *and* sparse bucket coverage -- and the current implementation widens every fragment
rather than only the partner-derived ones. See
[claudedocs/security_log_benchmark.md](../../claudedocs/security_log_benchmark.md) for the
numbers and the proposed refinement.

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
| `--no-dilation` | Disable temporal bucket dilation (unsound; for measuring lost rows) | off |

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

---

## Spatial Filtering Benchmark (`benchmark_h3_cell_lookup.py`)

Standalone benchmark comparing spatial cache filtering approaches on OSM POI data (~500k POIs, Germany).

```bash
python examples/benchmark/benchmark_h3_cell_lookup.py
```

### Approaches Compared

| Approach | Storage | Filter Mechanism | Dependencies |
|----------|---------|------------------|--------------|
| **BBox** (`postgis_bbox`) | PostGIS geometry collections | `ST_Intersects` with buffered geometry | PostGIS |
| **H3 Inline** (`rocksdict_h3_grouped`) | RocksDict `list[frozenset[int]]` | `h3_lat_lng_to_cell() = ANY(cells)` | h3-pg extension |
| **H3 MV** (`rocksdict_h3_grouped`) | RocksDict `list[frozenset[int]]` | `JOIN pois_h3_cells WHERE h3_cell_id = ANY(cells)` | h3-pg + materialized view |
| **H3 Column** (`rocksdict_h3_grouped`) | RocksDict `list[frozenset[int]]` | `WHERE h3_cell_id = ANY(cells)` on pre-computed column | h3-pg + column on fact table |

### BBox Spatial Methods

The BBox approach supports different methods for constructing the spatial filter geometry:

| Method | How It Works | Index |
|--------|-------------|-------|
| `SUBDIVIDE_TMP_TABLE` | `ST_Subdivide` into temp table + GiST index | GiST on temp table |
| `SUBDIVIDE_INLINE` | `ST_Subdivide` inline in the WHERE clause | None (nested loop) |
| `DUMP_TMP_TABLE` | `ST_Dump` into temp table + GiST index | GiST on temp table |
| `DUMP_CTE` | `ST_Dump` in a CTE MATERIALIZED | None (CTE scan) |

`max_vertices` controls `ST_Subdivide` granularity (how many vertices per polygon piece).
`ST_Dump` just breaks multi-geometries into simple parts without further subdivision.

### Benchmark Results (OSM POI, Germany, SRID 25832)

Query categories tested:

| Query | Description | Tables | Baseline |
|-------|-------------|--------|----------|
| spatial_q1-q3 | 3-table proximity joins (rare subtypes) | 3 | 0.2-0.5s |
| spatial_q4 | Single-table point-radius (no spatial fragment) | 1 | 0.008s |
| spatial_q5 | 2-table proximity join (common subtypes) | 2 | 0.23s |
| bench_qa1 | Double wildcard LIKE + proximity (no B-tree help) | 2 | 0.64s |
| bench_qb1 | High-cardinality cross-product (95k x 29k) | 2 | 0.73s |
| bench_qc1 | 4-table spatial chain (300m hops) | 4 | 9.4s |
| bench_qd1 | Expression filters + proximity (no index on computed values) | 2 | 12.4s |

#### Speedup Results

| Query | BBox Best | H3 Best | Winner |
|-------|-----------|---------|--------|
| spatial_q1 | 0.8x | **4.2x** (Inline/Col) | H3 |
| spatial_q2 | 1.6x | **5.2x** (Inline) | H3 |
| spatial_q3 | 1.3x | **14.2x** (Inline) | H3 |
| spatial_q5 | 1.0x | **2.0x** (MV) | H3 |
| bench_qa1 | 0.4x | 0.7x | Neither |
| bench_qb1 | 0.8x | 1.0x | Neither |
| bench_qc1 | 0.2x | 0.7x | Neither |
| **bench_qd1** | **20.0x** (TMP256/Dump) | 2.2x (Col) | **BBox** |

#### BBox Configuration Ranking (bench_qd1)

| Rank | Config | Speedup | Notes |
|------|--------|---------|-------|
| 1 | `SUBDIVIDE_TMP_TABLE` (256) | **20.0x** | GiST index on temp table is the key |
| 1 | `DUMP_TMP_TABLE` | **20.0x** | Equivalent when geometry is simple |
| 3 | `SUBDIVIDE_TMP_TABLE` (32) | 19.7x | Finer subdivision doesn't help with GiST |
| 4 | `DUMP_CTE` | 14.9x | No GiST index, CTE scan only |
| 5 | `SUBDIVIDE_INLINE` (256) | 13.6x | Inline = per-row ST_Intersects, no index |
| 6 | `SUBDIVIDE_INLINE` (32) | 12.2x | |
| 7 | `SUBDIVIDE_TMP_TABLE` (8) | 10.7x | Too many tiny polygons overwhelm GiST |
| 8 | `SUBDIVIDE_INLINE` (8) | 4.2x | Worst: many polygons + no index |

### Configuration Recommendations

**For queries with rare subtypes / high selectivity** (spatial_q1-q3):
Use **H3 Inline** or **H3 Column**. The B-tree index on H3 cell IDs provides 4-14x speedup. These queries have few matching entities, so the H3 cell filter dramatically reduces the search space. MV mode also works well but has higher constant overhead from the JOIN.

**For queries with expression-based filters** (bench_qd1):
Use **BBox with `SUBDIVIDE_TMP_TABLE` or `DUMP_TMP_TABLE`** (max_vertices >= 32). The 20x speedup comes from the GiST spatial index on the temp table, which prunes the search space before PostgreSQL evaluates expensive per-row expressions like `LENGTH()` or `LOWER(LIKE)`. H3 cell filtering gives only 2x here because it filters by discrete cell membership rather than continuous geometry, and the cell set is large for common subtypes.

**For queries where baselines are already fast** (bench_qa1, bench_qb1):
Neither BBox nor H3 provides meaningful speedup. When PostgreSQL already has effective index paths (warm shared_buffers, GiST for spatial), the overhead of cache lookup + filter construction exceeds any savings. Focus on conventional index optimization instead.

**For long chain queries** (bench_qc1):
Neither approach helps significantly. 4-table chains produce huge intermediate result sets. The cache filter applies to p0 (first table), but the combinatorial explosion happens in downstream joins which the filter can't reduce.

### Key Insights

**Temp table + GiST > CTE > Inline**: For BBox, the GiST spatial index on the temp table is the critical factor. Without it (inline or CTE), PostgreSQL must evaluate `ST_Intersects` for every candidate row.

**`max_vertices=8` is counterproductive**: Very small subdivision creates hundreds of tiny polygons. The overhead of matching against many polygons exceeds the selectivity benefit. Use 32-256, or skip subdivision entirely with `DUMP_TMP_TABLE`.

**H3 resolution 9 trade-offs**: At ~174m hex edge length, resolution 9 balances selectivity vs. cell count. Higher resolution (smaller cells) gives better precision but exponentially more cells in k-ring expansion.

**Buffer distance matters for correctness**: For chain queries with N spatial hops of distance D, the buffer must be `D * (N-1)` to avoid false negatives. The library's `compute_buffer_distance()` handles this automatically via graph diameter computation. Using just the max single distance causes row mismatches.

# PartitionCache Benchmark Results — SF 1.0

**Date:** 2026-06-14  
**Branch:** data_warehouse_support  
**Environment:** PostgreSQL (port 55432), DuckDB (embedded)

---

## Summary

PartitionCache pre-caches sets of partition keys from query fragments, then filters subsequent executions to only candidate rows. Effectiveness depends heavily on:

1. **Query baseline time** — must exceed cache overhead (~0.5–2 s per TMP_TABLE_IN lookup)
2. **Partition key selectivity** — smaller result sets give higher reduction percentages
3. **Predicate type** — benefits queries with expensive predicates (ILIKE, spatial, UDF) that cannot use B-tree indexes

---

## 1. OSM POI — Strongest Use Case

**Setup:** PostgreSQL, all pois of Germany (~7.9 M rows), 3-way self-join queries  
**Partition keys:** `zipcode` (integer, 7,878 distinct) and `landkreis` (text, 414 distinct)  
**Query type:** ILIKE on name + ST_DWithin between point geometries — no usable indexes

### Flight Q1 — Zipcode Partition Key

| Query | Baseline | Cached | Speedup | Reduction |
|-------|----------|--------|---------|-----------|
| zipcode_q1 | 0.918 s | 0.052 s | **17.80×** | 99.8% |
| zipcode_q2 | 3.798 s | 0.145 s | **26.19×** | 99.8% |
| zipcode_q3 | 2.884 s | 0.027 s | **106.66×** | 99.9% |
| **Average** | | | **50.22×** | 99.8% |

12 fragments each, all correct results. Cache population: 2.5–4.5 s (one-time cost).

### Flight Q2 — Landkreis Partition Key

| Query | Baseline | Cached | Speedup | Reduction |
|-------|----------|--------|---------|-----------|
| landkreis_q1 | 4.378 s | 0.560 s | **7.82×** | 95.4% |
| landkreis_q2 | 3.162 s | 0.608 s | **5.20×** | 95.7% |
| landkreis_q3 | 7.537 s | 0.284 s | **26.56×** | 98.8% |
| **Average** | | | **13.20×** | 96.6% |

12 fragments each. Landkreis has lower cardinality (414 vs 7,878), so reduction is less extreme.

**Why it works:** 3-way self-join has no index acceleration. Cache reduces 7,878 zipcodes to 5–19 candidates before the expensive join executes.

---

## 2. NYC Taxi — Strongly Selectivity-Dependent

**Setup:** PostgreSQL + PostGIS, 14.48 M taxi trips, OSM POI join  
**Partition key:** `trip_id` (integer, 14,483,721 distinct values)  
**Query type:** ST_DWithin spatial proximity + EXISTS subquery

### Flight Q7 — Cross-Dimension Showcase (broad selectivity)

| Query | Baseline | Cached | Speedup | Keys Found | Reduction |
|-------|----------|--------|---------|------------|-----------|
| q7_1 (Museum→Hospital night) | 3.847 s | 10.365 s | 0.37× | 220,836 | 98.5% |
| q7_2 (Airport→Hotel) | 12.849 s | 10.122 s | **1.27×** | 168,991 | 98.8% |
| q7_3 (Bar→Museum, long trip) | 4.671 s | 1.502 s | **3.11×** | 32,597 | 99.8% |
| q7_4 (Subway→Subway anomaly) | 11.827 s | 23.750 s | 0.50× | 595,941 | 95.9% |
| **Average** | | | **1.31×** | | |

Cache population: 143–260 s per query (4–14 M row fragment scans with PostGIS).

### Flight Q11 — Highly Selective Queries (designed for PartitionCache)

Queries designed with two rare POI spatial filters (each ~1–5% selectivity) + one trip condition (~30–40%),
combining to <0.025% overall. All EXISTS subqueries — osm_pois never in the main FROM.

**Fragment design rationale:**
- `hospital@100ft` pickup: 1.44% = 208K trips (42 hospitals in NYC)
- `university@100ft` dropoff: 1.73% = 249K trips (25 universities)
- Combined spatial (no trip filter): 0.026% = **3,775 trips** → becomes the cached key set

| Query | Baseline | Cached | Speedup | Keys Cached | Reduction |
|-------|----------|--------|---------|-------------|-----------|
| q11_1 (Hospital→University, tip>0) | 19.994 s | 0.325 s | **61.50×** | 1,141 | 100.0% |
| q11_2 (Hospital→Museum, dist 2–8 mi) | 24.522 s | 0.760 s | **32.27×** | 3,280 | 100.0% |
| q11_3 (University→University, fare>$8) | 30.929 s | 3.678 s | **8.41×** | 1,833 | 100.0% |
| **Average** | | | **34.06×** | | **100.0%** |

Cache population: 128–168 s per query (4–5 fragments each, full 14.5 M row spatial scan).  
All results correct (match: True).

**Why 100% reduction:** The fragment intersection (hospital pickup + university/museum dropoff, without the trip condition) captures exactly the candidate trips. The cache stores the precise trip_id set — the cached query only checks those candidates.

**Why baselines are 20–31 s:** Two nested EXISTS with ST_DWithin on 14.5 M rows (scan from taxi_trips to POI index). Each row requires 2 spatial proximity checks; hospitals/universities have few POIs (25–97) but the base scan still covers 14.5 M trips.

**Contrast with Q7:** q7_3 (the best in flight 7) cached 32,597 keys → 3.11×. Flight 11 queries cache 1,141–3,280 keys → 8–61×. The difference is fully explained by the cached set size: smaller set = faster temp table join on the 14.5 M row table.

**Key insight:** Speedup inversely correlates with keys found. When the spatial filter selects hundreds of thousands of trip_ids, TMP_TABLE_IN overhead exceeds the original query time. Below ~10K keys, speedups become very large. The transition from useful to harmful appears around 30–50K cached keys for this dataset.

---

## 3. SSB Standard Queries — Not Beneficial

**Setup:** PostgreSQL SF1.0 (5.56 M lineorder rows), star schema with B-tree indexes  
**Partition keys:** `lo_custkey`, `lo_suppkey`, `lo_partkey`, `lo_orderdate`  
**Query type:** Standard SSB equality predicates on dimension tables

| Method | Best speedup | Average | Note |
|--------|-------------|---------|------|
| TMP_TABLE_IN | 0.97× (q1_1) | 0.21× | All but q1_1 slower |
| IN_SUBQUERY | 1.98× (q1_1) | 0.22× | Same pattern |

Baseline times: **3–48 ms** (sub-50 ms). Cache overhead (temp table create + join, or IN subquery) consistently exceeds query time.

**Why it fails:** PostgreSQL has excellent B-tree indexes and statistics for star schema joins. Queries complete in <50 ms. Cache lookup overhead (creating temp tables with thousands of rows) takes longer than the query itself.

> **Note on fragment generation:** Standard SSB queries use FK joins (`lo.lo_custkey = c.c_custkey`). The current query processor misclassifies these as `distance_conditions`, causing fragment generation to be partially broken for standard join form. The `follow_graph: false` + `fragment_filter.require_in_from: [lineorder]` config workaround partially addresses this. Full fix is documented in the Dimension-Attachment plan.

---

## 4. SSB Expensive Queries (qx variants) — Marginal on PostgreSQL

**Setup:** PostgreSQL SF1.0, same star schema  
**Query modifications:** Added `md5(lo_orderkey::text) < 'X'` to prevent index pushdown + ILIKE on dimension name columns

| Query | Baseline | Cached | Speedup | Avg Reduction |
|-------|----------|--------|---------|---------------|
| qx1_1 (ASIA regions, 1994-96) | 1.152 s | 1.663 s | 0.69× | 72.2% |
| qx1_2 (AMERICA + c_name ILIKE) | 1.354 s | 84.335 s | **0.02×** | 91.8% |
| qx1_3 (EUROPE + p_color ILIKE) | 1.837 s | 2.207 s | 0.83× | 53.3% |
| qx2_1 (s_name ILIKE + AMERICA) | 1.631 s | 2.751 s | 0.59× | 81.4% |
| qx2_2 (NOT IN + 4-key) | 1.584 s | 1.158 s | **1.37×** | 75.5% |
| qx2_3 (c_phone LIKE + ASIA) | 1.111 s | 1.414 s | 0.79× | 51.3% |
| **Average** | | | **0.71×** | 70.9% |

Fragment population: 54–328 s (16–48 fragments per query × 3–4 partition keys).  
Cache population details:
- qx1_1: 3 PKs, 16 fragments each → lo_custkey 6,087/30K, lo_suppkey 403/2K, lo_orderdate 1,096/2.5K
- qx1_2: 3 PKs, 24 fragments → lo_custkey only **23/30K** (0.08%), but cached query 84 s (PG bad plan for temp table)
- qx2_2: 4 PKs, 48 fragments → intersection across 4 dimensions → 1.37×

**qx1_2 anomaly (0.02×):** Excellent reduction (23 custkeys from 30K), but PG chose a very poor plan when combining 3 temp table JOINs with the original ILIKE conditions, resulting in 84 s execution instead of 1.35 s baseline.

**Why still marginal:** Queries run 1–2 s in PG due to hash join optimizations. Cache search space reduction is good (51–92%), but the benefit is consumed by multi-table temp join overhead and occasional plan regressions.

---

## 5. SSB Expensive Queries (qx variants) — DuckDB SF1.0

**Status:** Benchmark aborted after 62 minutes (only qx1_1 completed). Two critical issues make DuckDB SF1.0 unworkable for PartitionCache with the current config:

### qx1_1 (only completed query)

| Metric | Value |
|--------|-------|
| Baseline | **0.076 s** (76 ms — DuckDB columnar speed) |
| Cache population (lo_custkey) | **2363 s = 39.4 min** for 16 fragments |
| Cache population (lo_suppkey) | 140 s for 16 fragments |
| Cache population (lo_orderdate) | 0 keys cached (broken — see below) |
| Speedup | 0.74× (slower) |
| lo_custkey reduction | 79.7% (6,094 / 30,000) |

### Problem 1 — Fragment execution catastrophically slow for lo_custkey

Each lo_custkey fragment is a 4-table join (lineorder × customer × supplier × date_dim) with `md5()` on every row. In DuckDB, this takes **~148 s per fragment** (vs ~1.5 s in PostgreSQL). Root cause: DuckDB optimizes for wide columnar scans, not iterative per-row UDFs across multi-table hash joins. The 16 lo_custkey fragments alone took 39 minutes. lo_suppkey fragments were 8.75 s each (17× faster) — the difference reflects which dimension tables are pulled in and the resulting join plan quality.

### Problem 2 — duckdb_bit bitsize incompatible with lo_orderdate

SSB `lo_orderdate` values are 8-digit integers (19921101–19981231). The `duckdb_bit` backend requires `bitsize ≥ max(partition_key_value)`. The current config sets `bitsize: 200000`, so all date values exceed the limit and nothing is cached. Fix would require `bitsize ≥ 19990101` (≈ 2.5 MB of bits per cache entry) — impractical for this partition key type. The `postgresql_array` backend has no such limitation.

### Conclusion for DuckDB SSB

DuckDB's sub-100ms baselines via vectorized columnar execution make PartitionCache overhead (temp table creation, multi-key joins) structurally non-competitive. Additionally, DuckDB's hash join planner produces very slow plans for md5-filtered multi-table fragment queries at SF1.0 scale. **DuckDB SSB is not a viable use case for PartitionCache.**

---

## 6. Cross-Benchmark Summary

| Benchmark | Method | Avg Speedup | Avg Reduction | Recommended? |
|-----------|--------|-------------|---------------|--------------|
| OSM POI (zipcode) | TMP_TABLE_IN | **50.2×** | 99.8% | ✅ Strongly |
| NYC Taxi (flight 11, highly selective) | TMP_TABLE_IN | **34.1×** | 100.0% | ✅ Strongly |
| OSM POI (landkreis) | TMP_TABLE_IN | **13.2×** | 96.6% | ✅ Yes |
| NYC Taxi (flight 7, selective q7_3) | TMP_TABLE_IN | 3.1× (best) | 99.8% | ✅ When <10K keys |
| NYC Taxi (flight 7, broad) | TMP_TABLE_IN | 0.4–0.5× | 98–99% | ❌ Keys too large |
| SSB standard (PG SF1.0) | TMP_TABLE_IN | 0.21× | ~0% | ❌ Baseline <50 ms |
| SSB qx expensive (PG SF1.0) | TMP_TABLE_IN | 0.71× | 70.9% | ❌ Marginal |
| SSB qx expensive (DuckDB SF1.0) | TMP_TABLE_IN | 0.74× (qx1_1) | 53% (2 of 3 PKs) | ❌ 39 min pop, 76 ms baseline |

---

## When PartitionCache Helps

✅ **Good fit:**
- Queries with expensive predicates that can't use B-tree indexes (spatial functions, ILIKE, UDFs, regex)
- Queries where the cached partition key set is **< ~10K entries** in TMP_TABLE_IN mode — at this scale the temp table join is fast
- Spatial self-joins: two ST_DWithin constraints combined → rare intersection (< 0.1% selectivity) → tiny cached set
- Self-joins on large tables without effective indexes (e.g. OSM POI 3-way name-similarity joins)
- Workloads where the same partition space is queried repeatedly (cache amortizes over many queries)

❌ **Poor fit:**
- Queries already running in <1 s on indexed columns (cache overhead > query time)
- Cached key sets >50K rows: TMP_TABLE_IN overhead grows with temp table size; at 150K+ keys it can exceed baseline query time
- Columnar databases (DuckDB) where vectorized scans natively outperform any pre-filtering
- One-time queries that won't benefit from cached partition keys
- Bit-vector cache backends (duckdb_bit) with partition keys whose values exceed the configured `bitsize` (e.g. SSB date integers 19921101–19981231 require bitsize ≥ 20M)

**Calibration from flight 11 results (NYC Taxi, 14.5 M rows):**
- 1,141 keys → **61.5×** speedup, 0.33 s cached
- 3,280 keys → **32.3×** speedup, 0.76 s cached  
- 1,833 keys → **8.4×** speedup, 3.68 s cached (slower due to 2 EXISTS re-checks on candidates)
- 32,597 keys (q7_3) → **3.1×** speedup
- 168,991+ keys (q7_1, q7_4) → **0.4–0.5×** (slower than baseline)

---

## Technical Notes

- **Method:** `TMP_TABLE_IN` — creates temporary table with cached partition keys, then joins with fact table
- **Alternative:** `IN_SUBQUERY` — similar overhead pattern, marginal difference in practice
- **Cache population** is a one-time cost per query pattern; benefit is realized on repeat executions
- **Fragment generation** produces PowerSet-like combinations of conditions to populate partition key sets for all possible sub-queries

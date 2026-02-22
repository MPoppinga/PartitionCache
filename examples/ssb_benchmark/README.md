# SSB Benchmark for PartitionCache

This example demonstrates PartitionCache on the **Star Schema Benchmark (SSB)**, a data warehouse workload with multiple dimension tables and a central fact table (`lineorder`).

The key features demonstrated are **multiple partition keys per query**, **cross-dimension cache reuse**, and **hierarchical drill-down cache reuse**.

## Quick Start (DuckDB)

```bash
# 1. Generate data (SF=0.01 ~ 60K lineorder rows)
python generate_ssb_data.py --scale-factor 0.01 --db-backend duckdb

# 2. Run benchmark
python run_ssb_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode all

# 3. Cross-dimension reuse evaluation
python run_ssb_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode cross-dimension

# 4. Hierarchical drill-down evaluation
python run_ssb_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode hierarchy

# 5. Repeated timing for stable measurements
python run_ssb_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode all --repeat 3
```

## Quick Start (PostgreSQL)

```bash
# 1. Copy and edit environment config
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 2. Generate data
python generate_ssb_data.py --scale-factor 0.01 --db-backend postgresql

# 3. Run benchmark
python run_ssb_benchmark.py --scale-factor 0.01 --db-backend postgresql --mode all

# 4. Compare cache backends
python run_ssb_benchmark.py --scale-factor 0.01 --db-backend postgresql --mode backend-comparison
```

## Data Generation

The data generator uses **custom Python code** to create SSB tables. Unlike the TPC-H benchmark (which uses DuckDB's built-in generator), DuckDB does not ship a built-in SSB generator.

Limitations compared to the official `ssb-dbgen`:
- **Distributions**: Uniform random instead of TPC-H-derived skew (Zipf, seasonal patterns)
- **Names/Addresses**: Simplified placeholders ("Customer 1") instead of grammar-based generation
- **Determinism**: Reproducible with `--seed` but not equivalent to official `ssb-dbgen` output

What IS correct: schemas, date dimension hierarchy, scale factor cardinalities, value domains (regions, nations, categories, brands).

For PartitionCache benchmarking this is sufficient - the benchmark tests cache mechanics (variant decomposition, cross-dimension reuse, hierarchy drill-down), not data-dependent optimizer behavior.

For spec-compliant data, use the official SSB generator: https://github.com/eyalroz/ssb-dbgen

## Multiple Partition Keys

Each lineorder FK column is a separate partition key:

| Partition Key | Dimension Table | Filter Examples |
|--------------|-----------------|-----------------|
| `lo_custkey` | customer | Region, nation, city |
| `lo_suppkey` | supplier | Region, nation, city |
| `lo_partkey` | part | Category, brand, mfgr |
| `lo_orderdate` | date_dim | Year, month, week |

For each adapted SSB query, PartitionCache generates variants **per partition key**. The same query template is used for all partition keys - `generate_all_query_hash_pairs()` with `strip_select=True` rebuilds SELECT as `SELECT DISTINCT {partition_key}`.

### Condition Classification Rotation

When `partition_key=lo_custkey`:
- `lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_region='ASIA')` -> **partition_key_condition**
- `lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region='ASIA')` -> **attribute_condition**

When `partition_key=lo_suppkey` (same query):
- `lo.lo_custkey IN (...)` -> **attribute_condition**
- `lo.lo_suppkey IN (...)` -> **partition_key_condition**

## Cross-Dimension Cache Reuse

This is the primary value proposition. Variant decomposition generates all subsets of AND conditions. Shared conditions across queries produce identical hashes leading to cache hits.

**Example with Q3 flight (customer + supplier + date filters):**

1. **Q3.1** runs first: `c_region='ASIA'`, `s_region='ASIA'`, `d_year BETWEEN 1992 AND 1997`
   - Populates cache for `lo_custkey`, `lo_suppkey`, `lo_orderdate`

2. **Q3.2** runs next: `c_nation='UNITED STATES'`, `s_nation='UNITED STATES'`, same date range
   - The date-only variant has the SAME hash -> **cache hit on `lo_orderdate`**

3. A new query with same customer/supplier regions but different dates:
   - **Cache hit on `lo_custkey` and `lo_suppkey`** from Q3.1's cached data

### Applying Multiple Cached Restrictions

`extend_query_with_partition_keys()` uses `sqlglot` AND-chaining. Sequential calls for different partition keys correctly produce:

```
original_query
  AND lo.lo_custkey IN (...)
  AND lo.lo_suppkey IN (...)
  AND lo.lo_orderdate IN (...)
```

## Hierarchical Drill-Down

Flights Q5 and Q6 demonstrate **cache reuse across hierarchy levels** within the same dimension. Non-target dimensions are held constant so only the target dimension drills from broad to narrow.

### Q5: Customer Hierarchy (region -> nation -> city)

| Query | Customer Filter | Hierarchy Level |
|-------|----------------|----------------|
| q5_1 | `c_region = 'ASIA'` | Region (broadest) |
| q5_2 | `c_nation = 'CHINA'` | Nation |
| q5_3 | `c_city = 'CHINA0'` | City (narrowest) |

Constant: `s_region = 'ASIA'`, `d_year >= 1992 AND d_year <= 1997`

As the customer dimension narrows, supplier and date cache entries from q5_1 are **reused** by q5_2 and q5_3 (marked with `[REUSED]` in output).

### Q6: Part Hierarchy (mfgr -> category -> brand)

| Query | Part Filter | Hierarchy Level |
|-------|------------|----------------|
| q6_1 | `p_mfgr = 'MFGR#1'` | Manufacturer (broadest) |
| q6_2 | `p_category = 'MFGR#12'` | Category |
| q6_3 | `p_brand = 'MFGR#1201'` | Brand (narrowest) |

Constant: `s_region = 'AMERICA'`, `d_year >= 1992 AND d_year <= 1997`

Q5 uses `s_region = 'ASIA'` while Q6 uses `s_region = 'AMERICA'`, so the two flights don't share cache entries, keeping hierarchy reuse self-contained.

### Example Hierarchy Output

```
Level                          Pre-Hits   New Hits   Reuse%   Speedup
----------------------------------------------------------------------
Region (broadest)                     0         12     0.0%     1.20x
Nation                                4          8    33.3%     2.50x
City (narrowest)                      6          6    50.0%     4.00x
```

## Partition Keys Per Query Flight

| Flight | Queries | lo_custkey | lo_suppkey | lo_partkey | lo_orderdate | Description |
|--------|---------|-----------|-----------|-----------|-------------|-------------|
| Q1 (1.1-1.3) | 3 | - | - | - | YES | Date + lineorder attrs |
| Q2 (2.1-2.3) | 3 | - | YES | YES | - | Part category + supplier region |
| Q3 (3.1-3.4) | 4 | YES | YES | - | YES | Customer + supplier + date |
| Q4 (4.1-4.3) | 3 | YES | YES | YES | YES | All 4 dimensions |
| Q5 (5.1-5.3) | 3 | YES | YES | - | YES | Customer hierarchy drill-down |
| Q6 (6.1-6.3) | 3 | - | YES | YES | YES | Part hierarchy drill-down |
| Q7 (7.1-7.3) | 3 | * | * | * | - | Single PK per query |
| Q8 (8.1-8.3) | 3 | * | * | * | * | 2-PK combinations |

\* Q7/Q8 use different PK combinations per query within the flight.

### Partition Key Coverage Detail

| PKs | Flight | Specific Keys | Status |
|-----|--------|--------------|--------|
| 1 | Q1 | lo_orderdate | Original |
| 1 | Q7 | lo_custkey (q7_1), lo_suppkey (q7_2), lo_partkey (q7_3) | New |
| 2 | Q2 | lo_suppkey + lo_partkey | Original |
| 2 | Q8 | custkey+date (q8_1), custkey+partkey (q8_2), suppkey+date (q8_3) | New |
| 3 | Q3 | lo_custkey + lo_suppkey + lo_orderdate | Original |
| 3 | Q5 | lo_custkey + lo_suppkey + lo_orderdate (hierarchy) | New |
| 3 | Q6 | lo_partkey + lo_suppkey + lo_orderdate (hierarchy) | New |
| 4 | Q4 | all 4 | Original |

## Benchmark Modes

| Mode | Description |
|------|-------------|
| `--mode all` | All 25 queries with per-query and per-partition-key metrics |
| `--mode flight N` | Only flight N queries (1-8) |
| `--mode cross-dimension` | Primary evaluation - demonstrates cross-dimension cache reuse (Q3+Q4) |
| `--mode cold-vs-warm` | Compare cold cache vs warm cache performance |
| `--mode hierarchy` | Hierarchical drill-down evaluation (Q5+Q6) with reuse tracking |
| `--mode backend-comparison` | Compare cache backends with representative queries |

## Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--scale-factor SF` | Scale factor (must match generated data) | 0.01 |
| `--db-backend` | Database backend: `duckdb` or `postgresql` | duckdb |
| `--db-path` | DuckDB file path | `ssb_sf{SF}.duckdb` |
| `--cache-backend` | Cache backend override | Auto-detected |
| `--repeat N` | Repeat baseline/cached queries N times for stable timing | 1 |
| `--output FILE` | Save results to JSON file (includes metadata) | None |

### JSON Output Format

When `--output` is specified, results are saved with metadata:

```json
{
  "metadata": {
    "mode": "hierarchy",
    "db_backend": "duckdb",
    "cache_backend": "duckdb_bit",
    "scale_factor": 0.01,
    "repeat": 3,
    "timestamp": "2026-02-15T22:00:00+00:00"
  },
  "results": [...]
}
```

## Search Space Reduction Metrics

The benchmark reports **search space reduction** per partition key for each query. This metric shows how much the cache narrows the fact table scan:

```
Search Space Reduction:
  lo_custkey:   300 total -> 45 cached (85.0% reduction)
  lo_suppkey:    20 total ->  4 cached (80.0% reduction)
  lo_orderdate: 2400 total -> 1800 cached (25.0% reduction)
```

- **total**: `COUNT(DISTINCT pk)` across the entire `lineorder` table (computed once at startup)
- **cached**: number of distinct PK values in the intersected cache entry
- **reduction**: `(1 - cached / total) * 100` - percentage of the search space eliminated

Higher reduction means the cache provides a tighter constraint. The summary table includes an average reduction column per query when these metrics are available.

In JSON output (`--output`), each partition key's `apply_stats` entry includes:
```json
"search_space_reduction": {
  "total_distinct": 300,
  "cached_set_size": 45,
  "reduction_pct": 85.0
}
```

The `fact_table_stats` are also included in the `metadata` section.

## Scale Factors

| SF | lineorder | customer | supplier | part | date_dim |
|----|-----------|----------|----------|------|----------|
| 0.01 | ~60K | ~300 | ~20 | ~2K | 2,556 |
| 0.1 | ~600K | ~3K | ~200 | ~20K | 2,556 |
| 1.0 | ~6M | ~30K | ~2K | ~200K | 2,556 |

## Adapted Query Format

Original SSB queries use comma-joins and GROUP BY. The adapted versions rewrite these as single-table queries on `lineorder` with dimension filters as `IN (SELECT ...)` subqueries:

**Original Q3.1:**
```sql
SELECT c.c_nation, s.s_nation, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'ASIA'
  AND s.s_region = 'ASIA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY c.c_nation, s.s_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC
```

**Adapted Q3.1 (used for cache population):**
```sql
SELECT lo.lo_custkey
FROM lineorder lo
WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_region = 'ASIA')
  AND lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'ASIA')
  AND lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_year >= 1992 AND d_year <= 1997)
```

The SELECT column is irrelevant when `strip_select=True` - it gets replaced with `SELECT DISTINCT {partition_key}` during variant generation.

## File Structure

```
examples/ssb_benchmark/
  generate_ssb_data.py         # Data generator (DuckDB + PostgreSQL)
  run_ssb_benchmark.py         # Benchmark evaluation script
  .env.example                 # PostgreSQL config template
  README.md                    # This file
  queries/
    original/                  # 25 standard SSB queries
      q1_1.sql ... q1_3.sql    # Flight 1: date-only (3 queries)
      q2_1.sql ... q2_3.sql    # Flight 2: supplier+part (3 queries)
      q3_1.sql ... q3_4.sql    # Flight 3: customer+supplier+date (4 queries)
      q4_1.sql ... q4_3.sql    # Flight 4: all dimensions (3 queries)
      q5_1.sql ... q5_3.sql    # Flight 5: customer hierarchy drill-down (3 queries)
      q6_1.sql ... q6_3.sql    # Flight 6: part hierarchy drill-down (3 queries)
      q7_1.sql ... q7_3.sql    # Flight 7: single partition key (3 queries)
      q8_1.sql ... q8_3.sql    # Flight 8: multi-PK combinations (3 queries)
    adapted/                   # 25 PC-adapted query templates
      q1_1.sql ... q8_3.sql    # Same structure as original/
```

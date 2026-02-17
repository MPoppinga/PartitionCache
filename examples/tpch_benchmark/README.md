# TPC-H Benchmark for PartitionCache

This example demonstrates PartitionCache on the **TPC-H benchmark**, a normalized data warehouse workload with 8 tables and a central fact table (`lineitem`).

The key feature demonstrated is the **`l_orderkey` dual-duty pattern**: because `orders` bridges both customer and date dimensions, the same partition key (`l_orderkey`) provides access to both dimensions via separate IN-subqueries, enabling independent caching of each condition.

## Quick Start (DuckDB)

```bash
# 1. Generate data (SF=0.01 ~ 60K lineitem rows)
python generate_tpch_data.py --scale-factor 0.01 --db-backend duckdb

# 2. Run benchmark
python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode all

# 3. Cross-dimension reuse evaluation
python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode cross-dimension

# 4. Hierarchical drill-down evaluation
python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode hierarchy

# 5. Repeated timing for stable measurements
python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode all --repeat 3
```

## Quick Start (PostgreSQL)

```bash
# 1. Copy and edit environment config
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 2. Generate data
python generate_tpch_data.py --scale-factor 0.01 --db-backend postgresql

# 3. Run benchmark
python run_tpch_benchmark.py --scale-factor 0.01 --db-backend postgresql --mode all

# 4. Compare cache backends
python run_tpch_benchmark.py --scale-factor 0.01 --db-backend postgresql --mode backend-comparison
```

## TPC-H Schema (8 Tables)

```
region (5 rows, fixed)
  └── nation (25 rows, fixed)
        ├── supplier ──── partsupp ──── part
        └── customer ──── orders ──── lineitem (fact table)
```

| Table | SF=0.01 | SF=0.1 | SF=1.0 |
|-------|---------|--------|--------|
| region | 5 | 5 | 5 |
| nation | 25 | 25 | 25 |
| part | 2,000 | 20,000 | 200,000 |
| supplier | 100 | 1,000 | 10,000 |
| partsupp | 8,000 | 80,000 | 800,000 |
| customer | 1,500 | 15,000 | 150,000 |
| orders | 15,000 | 150,000 | 1,500,000 |
| lineitem | ~60,000 | ~600,000 | ~6,000,000 |

## The l_orderkey Dual-Duty Pattern

In SSB, each FK on the fact table leads to exactly one dimension. In TPC-H, `l_orderkey` serves as the path to **both** customer and date dimensions via the `orders` bridge table:

```
lineitem.l_orderkey ──→ orders ──→ o_custkey ──→ customer (+ nation/region)
                               ──→ o_orderdate (date dimension)
```

In adapted queries, customer and date are expressed as **separate IN-subqueries on the same column**:

```sql
-- Customer access via l_orderkey
l.l_orderkey IN (SELECT o_orderkey FROM orders
  WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE ...))
-- Date access via l_orderkey (separate condition for independent caching)
AND l.l_orderkey IN (SELECT o_orderkey FROM orders
  WHERE o_orderdate >= '1994-01-01' AND o_orderdate < '1997-01-01')
```

This separation enables PartitionCache to cache each condition independently - a date-only variant gets its own hash, reusable by any query with the same date range regardless of customer filters.

## Deeper Hierarchy Nesting

TPC-H's nation/region hierarchy creates deeper IN-subquery nesting than SSB:

```sql
-- SSB: supplier directly has s_region
lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'ASIA')

-- TPC-H: supplier → nation → region chain
l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN
    (SELECT n_nationkey FROM nation WHERE n_regionkey IN
        (SELECT r_regionkey FROM region WHERE r_name = 'ASIA')))
```

## Partition Keys

Three partition key columns on the `lineitem` fact table:

| Partition Key | Path | Dimensions Accessed |
|--------------|------|---------------------|
| `l_orderkey` | → orders | Customer (via o_custkey) + Date (via o_orderdate) |
| `l_partkey` | → part | Part type, brand, manufacturer |
| `l_suppkey` | → supplier → nation → region | Supplier hierarchy |

## Partition Keys Per Query Flight

| Flight | Queries | l_orderkey | l_partkey | l_suppkey | Theme |
|--------|---------|------------|-----------|-----------|-------|
| Q1 (1.1-1.3) | 3 | YES (date) | - | - | Date via orders + lineitem attrs |
| Q2 (2.1-2.3) | 3 | - | YES | YES | Part type/brand + supplier region |
| Q3 (3.1-3.4) | 4 | YES (date+cust) | - | YES | Customer + date + supplier |
| Q4 (4.1-4.3) | 3 | YES (date+cust) | YES | YES | All 3 PKs, all dimensions |
| Q5 (5.1-5.3) | 3 | YES (date+cust) | - | YES | Supplier hierarchy drill-down |
| Q6 (6.1-6.3) | 3 | YES (date) | YES | YES | Part hierarchy drill-down |
| Q7 (7.1-7.3) | 3 | * | * | * | Single PK per query |
| Q8 (8.1-8.3) | 3 | * | * | * | Two-PK combinations |

\* Q7/Q8 use different PK combinations per query within the flight.

## Cross-Dimension Reuse Story

When running `--mode cross-dimension`:

**Phase 1 (Q3 flight)**: Populates `l_orderkey` and `l_suppkey` caches
- Q3.1: EUROPE region + 1994-1996 → all new
- Q3.2: FRANCE + 1994-1996 → **date cache HIT** (same range as Q3.1)
- Q3.3: FRANCE/GERMANY + 1995 → new date, but nation conditions partially overlap
- Q3.4: BUILDING segment + Dec 1996 + FRANCE → different conditions

**Phase 2 (Q4 flight)**: Uses all 3 PKs, benefits from Q3's cache
- Q4.1: AMERICA region → all new (different region), but part dimension is new
- Q4.2: AMERICA + 1997-1998 → **customer cache HIT** from Q4.1, part cache HIT from Q4.1
- Q4.3: AMERICA + 1997-1998 + US + Brand#34 → **customer+date reuse** from Q4.2

## Hierarchical Drill-Down

Flights Q5 and Q6 demonstrate **cache reuse across hierarchy levels** within the same dimension. Non-target dimensions are held constant so only the target dimension drills from broad to narrow.

### Q5: Supplier Hierarchy (region → nation → nation+acctbal)

| Query | Supplier Filter | Hierarchy Level |
|-------|----------------|----------------|
| q5_1 | region = ASIA | Region (broadest) |
| q5_2 | nation = JAPAN | Nation |
| q5_3 | nation = JAPAN + acctbal > 5000 | Nation + filter (narrowest) |

Constant: ASIA customers + 1994-1996 date

### Q6: Part Hierarchy (mfgr → brand → brand+type)

| Query | Part Filter | Hierarchy Level |
|-------|------------|----------------|
| q6_1 | mfgr = Manufacturer#1 | Manufacturer (broadest) |
| q6_2 | brand = Brand#13 | Brand |
| q6_3 | brand = Brand#13 + type = ECONOMY BURNISHED BRASS | Brand + type (narrowest) |

Constant: 1994-1996 date + AMERICA suppliers

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
| `--db-path` | DuckDB file path | `tpch_sf{SF}.duckdb` |
| `--cache-backend` | Cache backend override | Auto-detected |
| `--repeat N` | Repeat baseline/cached queries N times for stable timing | 1 |
| `--output FILE` | Save results to JSON file (includes metadata) | None |

## Adapted Query Format

Original TPC-H queries use comma-joins with GROUP BY. The adapted versions rewrite these as single-table queries on `lineitem` with dimension filters as `IN (SELECT ...)` subqueries:

**Original Q3.1:**
```sql
SELECT cn.n_name AS cust_nation, sn.n_name AS supp_nation,
       EXTRACT(YEAR FROM o.o_orderdate) AS l_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s,
     nation cn, nation sn, region cr, region sr
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  ...
GROUP BY cn.n_name, sn.n_name, EXTRACT(YEAR FROM o.o_orderdate)
```

**Adapted Q3.1 (used for cache population):**
```sql
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders
        WHERE o_custkey IN (SELECT c_custkey FROM customer
            WHERE c_nationkey IN (SELECT n_nationkey FROM nation
                WHERE n_regionkey IN (SELECT r_regionkey FROM region
                    WHERE r_name = 'EUROPE'))))
  AND l.l_orderkey IN (SELECT o_orderkey FROM orders
        WHERE o_orderdate >= '1994-01-01' AND o_orderdate < '1997-01-01')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier
        WHERE s_nationkey IN (SELECT n_nationkey FROM nation
            WHERE n_regionkey IN (SELECT r_regionkey FROM region
                WHERE r_name = 'EUROPE')))
```

The SELECT column is irrelevant when `strip_select=True` - it gets replaced with `SELECT DISTINCT {partition_key}` during variant generation.

## File Structure

```
examples/tpch_benchmark/
  generate_tpch_data.py         # Data generator (DuckDB + PostgreSQL)
  run_tpch_benchmark.py         # Benchmark evaluation script
  .env.example                  # PostgreSQL config template
  README.md                     # This file
  queries/
    original/                   # 25 standard TPC-H-inspired queries
      q1_1.sql ... q1_3.sql      # Flight 1: date-only (3 queries)
      q2_1.sql ... q2_3.sql      # Flight 2: part+supplier (3 queries)
      q3_1.sql ... q3_4.sql      # Flight 3: customer+supplier+date (4 queries)
      q4_1.sql ... q4_3.sql      # Flight 4: all dimensions (3 queries)
      q5_1.sql ... q5_3.sql      # Flight 5: supplier hierarchy drill-down (3 queries)
      q6_1.sql ... q6_3.sql      # Flight 6: part hierarchy drill-down (3 queries)
      q7_1.sql ... q7_3.sql      # Flight 7: single partition key (3 queries)
      q8_1.sql ... q8_3.sql      # Flight 8: two-PK combinations (3 queries)
    adapted/                    # 25 PC-adapted query templates
      q1_1.sql ... q8_3.sql      # Same structure as original/
```

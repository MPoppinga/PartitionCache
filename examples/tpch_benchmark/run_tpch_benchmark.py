#!/usr/bin/env python3
"""
TPC-H Benchmark Evaluation Script for PartitionCache.

Demonstrates PartitionCache's multi-partition-key support and cross-dimension
cache reuse using TPC-H with a normalized schema.

Key difference from SSB: TPC-H uses `orders` as a bridge table, so `l_orderkey`
serves dual-duty as the path to both customer (via o_custkey) and date (via
o_orderdate) dimensions. Customer and date filters are expressed as SEPARATE
IN-subqueries on l_orderkey, enabling independent caching of each condition.

Partition keys on the lineitem fact table:
  l_orderkey (-> orders -> customer/date), l_partkey (-> part), l_suppkey (-> supplier)

Usage:
    # Generate data first
    python generate_tpch_data.py --scale-factor 0.01 --db-backend duckdb

    # Run benchmark
    python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode all
    python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode cross-dimension
    python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode flight 3
    python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode hierarchy
    python run_tpch_benchmark.py --scale-factor 0.01 --db-backend duckdb --mode all --repeat 3
"""

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

# --- Partition key configuration per query flight ---

QUERY_PARTITION_KEYS = {
    # Q1 flight: date only via l_orderkey
    "q1_1": ["l_orderkey"],
    "q1_2": ["l_orderkey"],
    "q1_3": ["l_orderkey"],
    # Q2 flight: part + supplier
    "q2_1": ["l_partkey", "l_suppkey"],
    "q2_2": ["l_partkey", "l_suppkey"],
    "q2_3": ["l_partkey", "l_suppkey"],
    # Q3 flight: customer + date (via l_orderkey) + supplier
    "q3_1": ["l_orderkey", "l_suppkey"],
    "q3_2": ["l_orderkey", "l_suppkey"],
    "q3_3": ["l_orderkey", "l_suppkey"],
    "q3_4": ["l_orderkey", "l_suppkey"],
    # Q4 flight: all 3 PKs (customer+date via l_orderkey, part, supplier)
    "q4_1": ["l_orderkey", "l_partkey", "l_suppkey"],
    "q4_2": ["l_orderkey", "l_partkey", "l_suppkey"],
    "q4_3": ["l_orderkey", "l_partkey", "l_suppkey"],
    # Q5: supplier hierarchy drill-down (customer+date via l_orderkey, supplier)
    "q5_1": ["l_orderkey", "l_suppkey"],
    "q5_2": ["l_orderkey", "l_suppkey"],
    "q5_3": ["l_orderkey", "l_suppkey"],
    # Q6: part hierarchy drill-down (date via l_orderkey, part, supplier)
    "q6_1": ["l_orderkey", "l_partkey", "l_suppkey"],
    "q6_2": ["l_orderkey", "l_partkey", "l_suppkey"],
    "q6_3": ["l_orderkey", "l_partkey", "l_suppkey"],
    # Q7: single partition key per query
    "q7_1": ["l_orderkey"],
    "q7_2": ["l_suppkey"],
    "q7_3": ["l_partkey"],
    # Q8: two-PK combinations
    "q8_1": ["l_orderkey", "l_partkey"],
    "q8_2": ["l_orderkey", "l_suppkey"],
    "q8_3": ["l_partkey", "l_suppkey"],
}

QUERY_FLIGHTS = {
    1: ["q1_1", "q1_2", "q1_3"],
    2: ["q2_1", "q2_2", "q2_3"],
    3: ["q3_1", "q3_2", "q3_3", "q3_4"],
    4: ["q4_1", "q4_2", "q4_3"],
    5: ["q5_1", "q5_2", "q5_3"],
    6: ["q6_1", "q6_2", "q6_3"],
    7: ["q7_1", "q7_2", "q7_3"],
    8: ["q8_1", "q8_2", "q8_3"],
}

ALL_QUERIES = [q for flight in sorted(QUERY_FLIGHTS) for q in QUERY_FLIGHTS[flight]]

# Hierarchy level labels for Q5 and Q6
HIERARCHY_LABELS = {
    "q5_1": "Region (broadest)",
    "q5_2": "Nation",
    "q5_3": "Nation + acctbal (narrowest)",
    "q6_1": "Manufacturer (broadest)",
    "q6_2": "Brand",
    "q6_3": "Brand + Type (narrowest)",
}


def load_query(query_dir, query_name):
    """Load a SQL query from file."""
    path = os.path.join(query_dir, f"{query_name}.sql")
    with open(path) as f:
        return f.read().strip()


# --- DuckDB Execution ---

class DuckDBExecutor:
    """Execute queries against a DuckDB database."""

    def __init__(self, db_path):
        import duckdb
        self.conn = duckdb.connect(str(db_path), read_only=True)

    def execute(self, query):
        """Execute query and return (rows, elapsed_seconds)."""
        start = time.perf_counter()
        result = self.conn.execute(query).fetchall()
        elapsed = time.perf_counter() - start
        return result, elapsed

    def execute_fragment(self, fragment):
        """Execute a cache fragment query and return set of partition key values."""
        rows = self.conn.execute(fragment).fetchall()
        return {row[0] for row in rows}

    def close(self):
        self.conn.close()


# --- PostgreSQL Execution ---

class PostgreSQLExecutor:
    """Execute queries against a PostgreSQL database."""

    def __init__(self):
        import psycopg
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

        self.conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER", "app_user"),
            password=os.getenv("DB_PASSWORD", ""),
            dbname=os.getenv("DB_NAME", "tpch_db"),
        )
        self.conn.autocommit = True

    def execute(self, query):
        """Execute query and return (rows, elapsed_seconds)."""
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(query)
            rows = cur.fetchall()
            elapsed = time.perf_counter() - start
        return rows, elapsed

    def execute_fragment(self, fragment):
        """Execute a cache fragment query and return set of partition key values."""
        with self.conn.cursor() as cur:
            cur.execute(fragment)
            rows = cur.fetchall()
        return {row[0] for row in rows}

    def close(self):
        self.conn.close()


# --- Cache Manager ---

class CacheManager:
    """Manage partition cache handlers for multiple partition keys."""

    def __init__(self, cache_type, scale_factor=0.01, db_path=None):
        self.cache_type = cache_type
        self.handlers = {}
        self.db_path = db_path
        self.scale_factor = scale_factor

    def get_handler(self, partition_key):
        """Get or create a cache handler for a partition key."""
        if partition_key not in self.handlers:
            if self.cache_type == "duckdb_bit":
                cache_db = str(self.db_path).replace(".duckdb", "_cache.duckdb") if self.db_path else "tpch_cache.duckdb"
                os.environ["DUCKDB_BIT_PATH"] = cache_db
                # Dynamic bitsize based on scale factor: orders count + margin
                max_key = int(self.scale_factor * 1600000)
                os.environ["DUCKDB_BIT_BITSIZE"] = str(max(max_key, 200000))
            elif self.cache_type == "postgresql_roaringbit":
                os.environ.setdefault("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "tpch_cache")
            elif self.cache_type == "postgresql_bit":
                os.environ.setdefault("PG_BIT_CACHE_TABLE_PREFIX", "tpch_cache")
                max_key = int(self.scale_factor * 1600000)
                os.environ.setdefault("PG_BIT_CACHE_BITSIZE", str(max(max_key, 200000)))
            elif self.cache_type == "postgresql_array":
                os.environ.setdefault("PG_ARRAY_CACHE_TABLE_PREFIX", "tpch_cache")
            elif self.cache_type == "rocksdict_roaringbit":
                cache_dir = os.path.join(os.path.dirname(__file__), "tpch_cache_rocksdict")
                os.environ.setdefault("ROCKSDICT_ROARINGBIT_PATH", cache_dir)

            self.handlers[partition_key] = partitioncache.create_cache_helper(
                self.cache_type, partition_key, "integer",
            )
        return self.handlers[partition_key]

    def clear_all(self):
        """Clear all cache entries."""
        for handler in self.handlers.values():
            try:
                handler.delete_partition()
            except Exception:
                pass

    def close(self):
        """Close all handlers."""
        for handler in self.handlers.values():
            try:
                handler.close()
            except Exception:
                pass
        self.handlers.clear()


# --- Benchmark Core ---

def populate_cache_for_query(executor, cache_manager, adapted_query, partition_keys):
    """
    Populate cache for a single adapted query across all its partition keys.

    Returns dict with per-partition-key stats including population_time.
    """
    stats = {}

    for pk in partition_keys:
        pk_stats = {"fragments_generated": 0, "fragments_executed": 0, "partition_keys_cached": 0}
        handler = cache_manager.get_handler(pk)

        pk_start = time.perf_counter()

        # Generate all variant fragments for this partition key
        pairs = generate_all_query_hash_pairs(
            query=adapted_query,
            partition_key=pk,
            min_component_size=1,
            strip_select=True,
            auto_detect_star_join=False,
            skip_partition_key_joins=True,
        )

        pk_stats["fragments_generated"] = len(pairs)

        for fragment, hash_val in pairs:
            # Check if already cached
            if handler.underlying_handler.exists(hash_val, partition_key=pk):
                continue

            # Execute fragment to get partition key values
            try:
                result_set = executor.execute_fragment(fragment)
                pk_stats["fragments_executed"] += 1
                pk_stats["partition_keys_cached"] = max(pk_stats["partition_keys_cached"], len(result_set))

                # Store in cache
                if result_set:
                    handler.set_entry(
                        key=hash_val,
                        partition_key_identifiers=result_set,
                        query_text=fragment,
                    )
            except Exception as e:
                print(f"    Warning: Fragment execution failed for {pk}: {e}")

        pk_stats["population_time"] = time.perf_counter() - pk_start
        stats[pk] = pk_stats

    return stats


def apply_cache_to_query(cache_manager, adapted_query, original_query, partition_keys):
    """
    Apply cached restrictions for all partition keys to the original query.

    Returns (enhanced_query, per_pk_stats, cache_apply_time).
    """
    apply_start = time.perf_counter()
    enhanced = original_query
    pk_stats = {}

    for pk in partition_keys:
        handler = cache_manager.get_handler(pk)

        cached_keys, num_generated, num_hits = partitioncache.get_partition_keys(
            query=adapted_query,
            cache_handler=handler.underlying_handler,
            partition_key=pk,
            min_component_size=1,
            auto_detect_star_join=False,
        )

        pk_stats[pk] = {
            "variants_checked": num_generated,
            "cache_hits": num_hits,
            "partition_keys_found": len(cached_keys) if cached_keys else 0,
        }

        if cached_keys:
            try:
                enhanced = partitioncache.extend_query_with_partition_keys(
                    enhanced,
                    cached_keys,
                    partition_key=pk,
                    method="IN",
                    p0_alias="l",
                )
            except Exception as e:
                print(f"    Warning: Could not extend query with {pk}: {e}")

    cache_apply_time = time.perf_counter() - apply_start
    return enhanced, pk_stats, cache_apply_time


def run_single_query(executor, cache_manager, query_name, query_dir, repeat=1):
    """Run a single query through the full benchmark pipeline."""
    adapted_query = load_query(os.path.join(query_dir, "adapted"), query_name)
    original_query = load_query(os.path.join(query_dir, "original"), query_name)
    pks = QUERY_PARTITION_KEYS[query_name]

    result = {
        "query": query_name,
        "partition_keys": pks,
    }

    # 1. Baseline: run original query (with repeat)
    baseline_times = []
    baseline_rows = None
    for _ in range(repeat):
        rows, elapsed = executor.execute(original_query)
        baseline_times.append(elapsed)
        if baseline_rows is None:
            baseline_rows = rows
    result["baseline_rows"] = len(baseline_rows)
    result["baseline_time"] = statistics.mean(baseline_times)
    if repeat > 1:
        result["baseline_stddev"] = statistics.stdev(baseline_times)

    # 2. Populate cache
    cache_start = time.perf_counter()
    cache_stats = populate_cache_for_query(executor, cache_manager, adapted_query, pks)
    cache_pop_time = time.perf_counter() - cache_start
    result["cache_population_time"] = cache_pop_time
    result["cache_stats"] = cache_stats

    # 3. Apply cache and run enhanced query (with repeat)
    enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
        cache_manager, adapted_query, original_query, pks,
    )
    result["apply_stats"] = pk_stats
    result["cache_apply_time"] = cache_apply_time

    cached_times = []
    cached_rows = None
    for _ in range(repeat):
        rows, elapsed = executor.execute(enhanced_query)
        cached_times.append(elapsed)
        if cached_rows is None:
            cached_rows = rows
    result["cached_rows"] = len(cached_rows)
    result["cached_time"] = statistics.mean(cached_times)
    if repeat > 1:
        result["cached_stddev"] = statistics.stdev(cached_times)

    # 4. Compute metrics
    result["speedup"] = result["baseline_time"] / result["cached_time"] if result["cached_time"] > 0 else float("inf")
    result["result_match"] = len(baseline_rows) == len(cached_rows)

    return result


# --- Benchmark Modes ---

def run_all_queries(executor, cache_manager, query_dir, repeat=1):
    """Run all TPC-H queries."""
    print("\n" + "=" * 80)
    print(f"TPC-H Benchmark: All Queries ({len(ALL_QUERIES)} queries)")
    print("=" * 80)

    results = []
    for query_name in ALL_QUERIES:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_flight(executor, cache_manager, query_dir, flight_num, repeat=1):
    """Run a single query flight."""
    queries = QUERY_FLIGHTS.get(flight_num)
    if not queries:
        valid = ", ".join(str(f) for f in sorted(QUERY_FLIGHTS))
        print(f"Invalid flight number: {flight_num}. Choose from: {valid}")
        return []

    print(f"\n{'=' * 80}")
    print(f"TPC-H Benchmark: Flight Q{flight_num}")
    print(f"{'=' * 80}")

    results = []
    for query_name in queries:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_cross_dimension(executor, cache_manager, query_dir, repeat=1):
    """
    Cross-dimension cache reuse evaluation.

    Demonstrates l_orderkey dual-duty pattern: cache entries for customer and date
    conditions (both on l_orderkey) are independent and reusable across queries.
    Also shows supplier cache reuse across Q3 and Q4 flights.
    """
    print("\n" + "=" * 80)
    print("TPC-H Benchmark: Cross-Dimension Cache Reuse")
    print("=" * 80)

    results = []

    # Phase 1: Run Q3 flight sequentially, showing cumulative cache reuse
    print("\n### Phase 1: Q3 Flight (customer + date via l_orderkey + supplier)")
    print("Demonstrates l_orderkey dual-duty: customer and date as separate IN-subqueries.\n")

    q3_queries = QUERY_FLIGHTS[3]

    for i, query_name in enumerate(q3_queries):
        adapted_query = load_query(os.path.join(query_dir, "adapted"), query_name)
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        pks = QUERY_PARTITION_KEYS[query_name]

        print(f"\n--- {query_name} (Q3.{i + 1}) ---")

        # Check for pre-existing cache hits BEFORE populating
        pre_hits = {}
        for pk in pks:
            handler = cache_manager.get_handler(pk)
            _, num_gen, num_hits = partitioncache.get_partition_keys(
                query=adapted_query,
                cache_handler=handler.underlying_handler,
                partition_key=pk,
                min_component_size=1,
                auto_detect_star_join=False,
            )
            pre_hits[pk] = {"variants": num_gen, "hits_before": num_hits}

        # Baseline (with repeat)
        baseline_times = []
        baseline_rows = None
        for _ in range(repeat):
            rows, elapsed = executor.execute(original_query)
            baseline_times.append(elapsed)
            if baseline_rows is None:
                baseline_rows = rows
        baseline_time = statistics.mean(baseline_times)

        # Populate
        cache_start = time.perf_counter()
        cache_stats = populate_cache_for_query(executor, cache_manager, adapted_query, pks)
        cache_pop_time = time.perf_counter() - cache_start

        # Apply and run (with repeat)
        enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
            cache_manager, adapted_query, original_query, pks,
        )
        cached_times = []
        cached_rows = None
        for _ in range(repeat):
            rows, elapsed = executor.execute(enhanced_query)
            cached_times.append(elapsed)
            if cached_rows is None:
                cached_rows = rows
        cached_time = statistics.mean(cached_times)

        speedup = baseline_time / cached_time if cached_time > 0 else float("inf")

        result = {
            "query": query_name,
            "partition_keys": pks,
            "baseline_rows": len(baseline_rows),
            "baseline_time": baseline_time,
            "cache_population_time": cache_pop_time,
            "cache_apply_time": cache_apply_time,
            "cache_stats": cache_stats,
            "apply_stats": pk_stats,
            "pre_existing_hits": pre_hits,
            "cached_rows": len(cached_rows),
            "cached_time": cached_time,
            "speedup": speedup,
            "result_match": len(baseline_rows) == len(cached_rows),
        }
        if repeat > 1:
            result["baseline_stddev"] = statistics.stdev(baseline_times)
            result["cached_stddev"] = statistics.stdev(cached_times)
        results.append(result)

        # Print with cross-dimension info
        print(f"  Baseline: {len(baseline_rows)} rows in {baseline_time:.4f}s")
        print(f"  Cache population: {cache_pop_time:.4f}s")
        for pk in pks:
            hits_before = pre_hits[pk]["hits_before"]
            hits_after = pk_stats[pk]["cache_hits"]
            new_hits = hits_after - hits_before
            shared = hits_before
            print(f"    {pk}: {pk_stats[pk]['variants_checked']} variants, "
                  f"{hits_after} hits ({shared} pre-existing, {new_hits} new), "
                  f"{pk_stats[pk]['partition_keys_found']} keys")
        print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s "
              f"(speedup: {speedup:.2f}x, match: {result['result_match']})")

    # Phase 2: Show Q4 flight benefiting from Q3's cache
    print("\n### Phase 2: Q4 Flight (all 3 PKs: l_orderkey + l_partkey + l_suppkey)")
    print("Q4 queries benefit from Q3's l_orderkey and l_suppkey cache entries.\n")

    q4_queries = QUERY_FLIGHTS[4]

    for i, query_name in enumerate(q4_queries):
        adapted_query = load_query(os.path.join(query_dir, "adapted"), query_name)
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        pks = QUERY_PARTITION_KEYS[query_name]

        print(f"\n--- {query_name} (Q4.{i + 1}) ---")

        # Check for pre-existing cache hits
        pre_hits = {}
        for pk in pks:
            handler = cache_manager.get_handler(pk)
            _, num_gen, num_hits = partitioncache.get_partition_keys(
                query=adapted_query,
                cache_handler=handler.underlying_handler,
                partition_key=pk,
                min_component_size=1,
                auto_detect_star_join=False,
            )
            pre_hits[pk] = {"variants": num_gen, "hits_before": num_hits}

        # Baseline (with repeat)
        baseline_times = []
        baseline_rows = None
        for _ in range(repeat):
            rows, elapsed = executor.execute(original_query)
            baseline_times.append(elapsed)
            if baseline_rows is None:
                baseline_rows = rows
        baseline_time = statistics.mean(baseline_times)

        # Populate
        cache_start = time.perf_counter()
        cache_stats = populate_cache_for_query(executor, cache_manager, adapted_query, pks)
        cache_pop_time = time.perf_counter() - cache_start

        # Apply and run (with repeat)
        enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
            cache_manager, adapted_query, original_query, pks,
        )
        cached_times = []
        cached_rows = None
        for _ in range(repeat):
            rows, elapsed = executor.execute(enhanced_query)
            cached_times.append(elapsed)
            if cached_rows is None:
                cached_rows = rows
        cached_time = statistics.mean(cached_times)

        speedup = baseline_time / cached_time if cached_time > 0 else float("inf")

        result = {
            "query": query_name,
            "partition_keys": pks,
            "baseline_rows": len(baseline_rows),
            "baseline_time": baseline_time,
            "cache_population_time": cache_pop_time,
            "cache_apply_time": cache_apply_time,
            "cache_stats": cache_stats,
            "apply_stats": pk_stats,
            "pre_existing_hits": pre_hits,
            "cached_rows": len(cached_rows),
            "cached_time": cached_time,
            "speedup": speedup,
            "result_match": len(baseline_rows) == len(cached_rows),
        }
        if repeat > 1:
            result["baseline_stddev"] = statistics.stdev(baseline_times)
            result["cached_stddev"] = statistics.stdev(cached_times)
        results.append(result)

        print(f"  Baseline: {len(baseline_rows)} rows in {baseline_time:.4f}s")
        print(f"  Cache population: {cache_pop_time:.4f}s")
        for pk in pks:
            hits_before = pre_hits[pk]["hits_before"]
            hits_after = pk_stats[pk]["cache_hits"]
            new_hits = hits_after - hits_before
            shared = hits_before
            marker = " *** CROSS-DIM REUSE ***" if shared > 0 else ""
            print(f"    {pk}: {pk_stats[pk]['variants_checked']} variants, "
                  f"{hits_after} hits ({shared} pre-existing, {new_hits} new), "
                  f"{pk_stats[pk]['partition_keys_found']} keys{marker}")
        print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s "
              f"(speedup: {speedup:.2f}x, match: {result['result_match']})")

    # Summary
    total_cross_dim_hits = 0
    for r in results:
        if "pre_existing_hits" in r:
            for pk in r["partition_keys"]:
                total_cross_dim_hits += r["pre_existing_hits"][pk]["hits_before"]

    print(f"\n{'=' * 80}")
    print(f"Cross-dimension reuse summary:")
    print(f"  Total pre-existing cache hits across all queries: {total_cross_dim_hits}")
    print(f"  Queries evaluated: {len(results)}")
    all_match = all(r["result_match"] for r in results)
    print(f"  All results correct: {all_match}")

    return results


def run_cold_vs_warm(executor, cache_manager, query_dir, repeat=1):
    """Compare cold cache (no pre-existing entries) vs warm cache (reuse from prior queries)."""
    print("\n" + "=" * 80)
    print("TPC-H Benchmark: Cold vs Warm Cache")
    print("=" * 80)

    # Cold run: clear cache, run Q3 flight
    print("\n### Cold Run (empty cache)")
    cache_manager.clear_all()

    cold_results = []
    for query_name in QUERY_FLIGHTS[3]:
        print(f"\n--- {query_name} (cold) ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat)
        result["mode"] = "cold"
        cold_results.append(result)
        print_query_result(result)

    # Warm run: keep cache, run Q3 again
    print("\n### Warm Run (cache from cold run)")
    warm_results = []
    for query_name in QUERY_FLIGHTS[3]:
        print(f"\n--- {query_name} (warm) ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat)
        result["mode"] = "warm"
        warm_results.append(result)
        print_query_result(result)

    # Comparison
    print(f"\n{'=' * 80}")
    print(f"{'Query':<8} {'Cold Total':>12} {'Warm Total':>12} {'Savings':>10}")
    print("-" * 44)
    for cold, warm in zip(cold_results, warm_results):
        cold_total = cold["cache_population_time"] + cold["cached_time"]
        warm_total = warm["cache_population_time"] + warm["cached_time"]
        savings = (cold_total - warm_total) / cold_total * 100 if cold_total > 0 else 0
        print(f"{cold['query']:<8} {cold_total:>11.4f}s {warm_total:>11.4f}s {savings:>9.1f}%")

    return cold_results + warm_results


def run_hierarchy(executor, cache_manager, query_dir, repeat=1):
    """
    Hierarchical drill-down evaluation.

    Runs Q5 (supplier hierarchy: region -> nation -> nation+acctbal) then Q6 (part
    hierarchy: mfgr -> brand -> brand+type) sequentially. Demonstrates increasing
    cache reuse as queries drill from broad to narrow within the same dimension.
    """
    print("\n" + "=" * 80)
    print("TPC-H Benchmark: Hierarchical Drill-Down")
    print("=" * 80)

    all_results = []

    for flight_num, flight_label in [(5, "Q5: Supplier Hierarchy (region -> nation -> nation+acctbal)"),
                                      (6, "Q6: Part Hierarchy (mfgr -> brand -> brand+type)")]:
        queries = QUERY_FLIGHTS[flight_num]

        print(f"\n### {flight_label}")
        print(f"Constant conditions kept the same; only the target dimension drills deeper.\n")

        flight_results = []

        for query_name in queries:
            adapted_query = load_query(os.path.join(query_dir, "adapted"), query_name)
            original_query = load_query(os.path.join(query_dir, "original"), query_name)
            pks = QUERY_PARTITION_KEYS[query_name]
            level_label = HIERARCHY_LABELS.get(query_name, query_name)

            print(f"\n--- {query_name}: {level_label} ---")

            # Check for pre-existing cache hits BEFORE populating
            pre_hits = {}
            for pk in pks:
                handler = cache_manager.get_handler(pk)
                _, num_gen, num_hits = partitioncache.get_partition_keys(
                    query=adapted_query,
                    cache_handler=handler.underlying_handler,
                    partition_key=pk,
                    min_component_size=1,
                    auto_detect_star_join=False,
                )
                pre_hits[pk] = {"variants": num_gen, "hits_before": num_hits}

            # Baseline (with repeat)
            baseline_times = []
            baseline_rows = None
            for _ in range(repeat):
                rows, elapsed = executor.execute(original_query)
                baseline_times.append(elapsed)
                if baseline_rows is None:
                    baseline_rows = rows
            baseline_time = statistics.mean(baseline_times)

            # Populate cache
            cache_start = time.perf_counter()
            cache_stats = populate_cache_for_query(executor, cache_manager, adapted_query, pks)
            cache_pop_time = time.perf_counter() - cache_start

            # Apply cache and run enhanced query (with repeat)
            enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                cache_manager, adapted_query, original_query, pks,
            )
            cached_times = []
            cached_rows = None
            for _ in range(repeat):
                rows, elapsed = executor.execute(enhanced_query)
                cached_times.append(elapsed)
                if cached_rows is None:
                    cached_rows = rows
            cached_time = statistics.mean(cached_times)

            speedup = baseline_time / cached_time if cached_time > 0 else float("inf")

            result = {
                "query": query_name,
                "hierarchy_level": level_label,
                "partition_keys": pks,
                "baseline_rows": len(baseline_rows),
                "baseline_time": baseline_time,
                "cache_population_time": cache_pop_time,
                "cache_apply_time": cache_apply_time,
                "cache_stats": cache_stats,
                "apply_stats": pk_stats,
                "pre_existing_hits": pre_hits,
                "cached_rows": len(cached_rows),
                "cached_time": cached_time,
                "speedup": speedup,
                "result_match": len(baseline_rows) == len(cached_rows),
            }
            if repeat > 1:
                result["baseline_stddev"] = statistics.stdev(baseline_times)
                result["cached_stddev"] = statistics.stdev(cached_times)

            flight_results.append(result)

            # Print per-PK details
            print(f"  Baseline: {len(baseline_rows)} rows in {baseline_time:.4f}s")
            print(f"  Cache population: {cache_pop_time:.4f}s")
            for pk in pks:
                hits_before = pre_hits[pk]["hits_before"]
                hits_after = pk_stats[pk]["cache_hits"]
                new_hits = hits_after - hits_before
                reused = hits_before
                marker = " [REUSED]" if reused > 0 else ""
                pop_time = cache_stats[pk].get("population_time", 0)
                print(f"    {pk}: {pk_stats[pk]['variants_checked']} variants, "
                      f"{hits_after} hits ({reused} reused, {new_hits} new), "
                      f"{pk_stats[pk]['partition_keys_found']} keys, "
                      f"pop: {pop_time:.4f}s{marker}")
            print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s "
                  f"(speedup: {speedup:.2f}x, match: {result['result_match']})")

        # Per-flight hierarchy summary table
        print(f"\n{'Level':<30} {'Pre-Hits':>10} {'New Hits':>10} {'Reuse%':>8} {'Speedup':>9}")
        print("-" * 70)
        for r in flight_results:
            total_pre = sum(r["pre_existing_hits"][pk]["hits_before"] for pk in r["partition_keys"])
            total_after = sum(r["apply_stats"][pk]["cache_hits"] for pk in r["partition_keys"])
            total_new = total_after - total_pre
            reuse_pct = total_pre / total_after * 100 if total_after > 0 else 0
            print(f"{r['hierarchy_level']:<30} {total_pre:>10} {total_new:>10} "
                  f"{reuse_pct:>7.1f}% {r['speedup']:>8.2f}x")

        all_results.extend(flight_results)

    # Overall summary
    print(f"\n{'=' * 80}")
    all_match = all(r["result_match"] for r in all_results)
    print(f"Hierarchy drill-down summary:")
    print(f"  Queries evaluated: {len(all_results)}")
    print(f"  All results correct: {all_match}")

    return all_results


def run_backend_comparison(executor, cache_manager, query_dir, db_backend, db_path=None, scale_factor=0.01, repeat=1):
    """
    Compare different cache backends on a fixed set of representative queries.

    Runs q1_1, q3_1, q5_1 with each available cache backend for the chosen database.
    """
    print("\n" + "=" * 80)
    print("TPC-H Benchmark: Backend Comparison")
    print("=" * 80)

    representative_queries = ["q1_1", "q3_1", "q5_1"]

    # Determine available backends
    if db_backend == "duckdb":
        backends = ["duckdb_bit"]
    else:
        backends = ["postgresql_array", "postgresql_bit", "postgresql_roaringbit"]

    all_results = {}

    for backend in backends:
        print(f"\n### Backend: {backend}")

        try:
            cm = CacheManager(backend, scale_factor=scale_factor, db_path=db_path)
            # Test that we can create a handler
            cm.get_handler("l_orderkey")
        except Exception as e:
            print(f"  Skipped: {e}")
            continue

        try:
            cm.clear_all()
        except Exception:
            pass

        backend_results = []

        for query_name in representative_queries:
            print(f"\n--- {query_name} ---")
            try:
                result = run_single_query(executor, cm, query_name, query_dir, repeat=repeat)
                backend_results.append(result)
                print_query_result(result)
            except Exception as e:
                print(f"  Error: {e}")
                backend_results.append({
                    "query": query_name,
                    "error": str(e),
                })

        all_results[backend] = backend_results

        try:
            cm.clear_all()
            cm.close()
        except Exception:
            pass

    # Comparison table
    print(f"\n{'=' * 80}")
    print("Backend Comparison Summary")
    print(f"\n{'Backend':<25} {'Query':<8} {'Pop Time':>10} {'Cached':>10} {'Speedup':>9}")
    print("-" * 65)
    for backend, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{backend:<25} {r['query']:<8} {'ERROR':>10} {'':>10} {'':>9}")
            else:
                print(f"{backend:<25} {r['query']:<8} "
                      f"{r['cache_population_time']:>9.4f}s "
                      f"{r['cached_time']:>9.4f}s "
                      f"{r['speedup']:>8.2f}x")

    # Flatten results for return
    flat_results = []
    for backend, results in all_results.items():
        for r in results:
            r_copy = dict(r)
            r_copy["cache_backend"] = backend
            flat_results.append(r_copy)

    return flat_results


# --- Output ---

def print_query_result(result):
    """Print results for a single query."""
    baseline_str = f"{result['baseline_time']:.4f}s"
    if "baseline_stddev" in result:
        baseline_str += f" (stddev: {result['baseline_stddev']:.4f}s)"
    print(f"  Baseline: {result['baseline_rows']} rows in {baseline_str}")
    print(f"  Cache population: {result['cache_population_time']:.4f}s")
    if "cache_apply_time" in result:
        print(f"  Cache apply: {result['cache_apply_time']:.4f}s")
    for pk in result["partition_keys"]:
        cs = result["cache_stats"].get(pk, {})
        ap = result["apply_stats"].get(pk, {})
        pop_time = cs.get("population_time", None)
        pop_str = f", pop: {pop_time:.4f}s" if pop_time is not None else ""
        print(f"    {pk}: {cs.get('fragments_generated', 0)} fragments, "
              f"{ap.get('cache_hits', 0)} hits, "
              f"{ap.get('partition_keys_found', 0)} keys{pop_str}")
    cached_str = f"{result['cached_time']:.4f}s"
    if "cached_stddev" in result:
        cached_str += f" (stddev: {result['cached_stddev']:.4f}s)"
    print(f"  Cached: {result['cached_rows']} rows in {cached_str} "
          f"(speedup: {result['speedup']:.2f}x, match: {result['result_match']})")


def print_summary(results):
    """Print overall summary."""
    has_stddev = any("baseline_stddev" in r for r in results)

    if has_stddev:
        print(f"\n{'=' * 80}")
        print(f"{'Query':<8} {'Baseline':>10} {'B.Std':>8} {'Cached':>10} {'C.Std':>8} {'Speedup':>9} {'Match':>7}")
        print("-" * 64)
        for r in results:
            b_std = f"{r.get('baseline_stddev', 0):.4f}" if "baseline_stddev" in r else "n/a"
            c_std = f"{r.get('cached_stddev', 0):.4f}" if "cached_stddev" in r else "n/a"
            print(f"{r['query']:<8} {r['baseline_time']:>9.4f}s {b_std:>8} "
                  f"{r['cached_time']:>9.4f}s {c_std:>8} "
                  f"{r['speedup']:>8.2f}x {'Y' if r['result_match'] else 'N':>6}")
    else:
        print(f"\n{'=' * 80}")
        print(f"{'Query':<8} {'Baseline':>10} {'Cached':>10} {'Speedup':>9} {'Match':>7}")
        print("-" * 48)
        for r in results:
            print(f"{r['query']:<8} {r['baseline_time']:>9.4f}s {r['cached_time']:>9.4f}s "
                  f"{r['speedup']:>8.2f}x {'Y' if r['result_match'] else 'N':>6}")

    avg_speedup = sum(r["speedup"] for r in results) / len(results) if results else 0
    all_match = all(r["result_match"] for r in results)
    print(f"\nAverage speedup: {avg_speedup:.2f}x")
    print(f"All results correct: {all_match}")


def main():
    parser = argparse.ArgumentParser(description="Run TPC-H benchmark with PartitionCache")
    parser.add_argument("--scale-factor", type=float, default=0.01,
                        help="Scale factor (must match generated data)")
    parser.add_argument("--db-backend", choices=["duckdb", "postgresql"], default="duckdb",
                        help="Database backend")
    parser.add_argument("--db-path", type=str, default=None,
                        help="DuckDB file path (default: tpch_sf{SF}.duckdb)")
    parser.add_argument("--cache-backend", type=str, default=None,
                        help="Cache backend (default: duckdb_bit for duckdb, postgresql_array for postgresql)")
    parser.add_argument("--mode", nargs="+", default=["all"],
                        help="Benchmark mode: all | flight N | cross-dimension | cold-vs-warm | hierarchy | backend-comparison")
    parser.add_argument("--repeat", type=int, default=1,
                        help="Number of repetitions for timing (default: 1)")
    parser.add_argument("--output", type=str, default=None,
                        help="Save results to JSON file")

    args = parser.parse_args()

    query_dir = os.path.join(os.path.dirname(__file__), "queries")

    # Set up executor
    executor: DuckDBExecutor | PostgreSQLExecutor
    db_path: str | None = None
    cache_type: str

    if args.db_backend == "duckdb":
        db_path = args.db_path or os.path.join(
            os.path.dirname(__file__), f"tpch_sf{args.scale_factor}.duckdb",
        )
        if not os.path.exists(db_path):  # type: ignore[arg-type]
            print(f"Database not found: {db_path}")
            print(f"Generate data first: python generate_tpch_data.py --scale-factor {args.scale_factor} --db-backend duckdb")
            sys.exit(1)

        print(f"Using DuckDB: {db_path}")
        executor = DuckDBExecutor(db_path)
        cache_type = args.cache_backend or "duckdb_bit"

    else:  # postgresql
        print("Using PostgreSQL")
        executor = PostgreSQLExecutor()
        cache_type = args.cache_backend or "postgresql_array"

    print(f"Cache backend: {cache_type}")
    print(f"Scale factor: {args.scale_factor}")
    if args.repeat > 1:
        print(f"Repeat: {args.repeat}")

    # Set up cache manager
    cache_manager = CacheManager(cache_type, scale_factor=args.scale_factor, db_path=db_path)

    try:
        mode = args.mode[0]

        if mode == "all":
            results = run_all_queries(executor, cache_manager, query_dir, repeat=args.repeat)
        elif mode == "flight":
            flight_num = int(args.mode[1]) if len(args.mode) > 1 else 3
            results = run_flight(executor, cache_manager, query_dir, flight_num, repeat=args.repeat)
        elif mode == "cross-dimension":
            results = run_cross_dimension(executor, cache_manager, query_dir, repeat=args.repeat)
        elif mode == "cold-vs-warm":
            results = run_cold_vs_warm(executor, cache_manager, query_dir, repeat=args.repeat)
        elif mode == "hierarchy":
            results = run_hierarchy(executor, cache_manager, query_dir, repeat=args.repeat)
        elif mode == "backend-comparison":
            results = run_backend_comparison(
                executor, cache_manager, query_dir,
                db_backend=args.db_backend, db_path=db_path,
                scale_factor=args.scale_factor, repeat=args.repeat,
            )
        else:
            print(f"Unknown mode: {mode}")
            sys.exit(1)

        # Save results
        if args.output:
            output_data = {
                "metadata": {
                    "mode": mode,
                    "db_backend": args.db_backend,
                    "cache_backend": cache_type,
                    "scale_factor": args.scale_factor,
                    "repeat": args.repeat,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                "results": [],
            }

            # Convert results to serializable format
            for r in results:
                sr = dict(r)
                for pk in sr.get("apply_stats", {}):
                    if "partition_keys" in sr["apply_stats"][pk]:
                        sr["apply_stats"][pk]["partition_keys"] = list(sr["apply_stats"][pk]["partition_keys"])
                output_data["results"].append(sr)

            with open(args.output, "w") as f:
                json.dump(output_data, f, indent=2, default=str)
            print(f"\nResults saved to {args.output}")

    finally:
        cache_manager.close()
        executor.close()


if __name__ == "__main__":
    main()

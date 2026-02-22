#!/usr/bin/env python3
"""
NYC Taxi Benchmark Evaluation Script for PartitionCache.

Demonstrates PartitionCache's spatial query acceleration using PostGIS-backed
NYC taxi trip data. All queries use a single partition key (trip_id) on the
taxi_trips fact table, with spatial filters via PostGIS geometry operations.

Query flights cover tourism, medical/emergency, commuter, nightlife, spatial
hierarchy drill-downs, cross-dimension reuse, and maximum complexity scenarios.

Partition key on the taxi_trips fact table:
  trip_id (unique trip identifier)

Usage:
    # Run benchmark
    python run_nyc_taxi_benchmark.py --mode all
    python run_nyc_taxi_benchmark.py --mode cross-dimension
    python run_nyc_taxi_benchmark.py --mode flight 3
    python run_nyc_taxi_benchmark.py --mode hierarchy
    python run_nyc_taxi_benchmark.py --mode spatial
    python run_nyc_taxi_benchmark.py --mode all --repeat 3
    python run_nyc_taxi_benchmark.py --mode backend-comparison
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

# --- Query flight configuration ---

QUERY_FLIGHTS = {
    1: ["q1_1", "q1_2", "q1_3"],           # Tourism
    2: ["q2_1", "q2_2", "q2_3"],           # Medical/Emergency
    3: ["q3_1", "q3_2", "q3_3"],           # Commuter
    4: ["q4_1", "q4_2", "q4_3"],           # Nightlife
    5: ["q5_1", "q5_2", "q5_3"],           # Start hierarchy
    6: ["q6_1", "q6_2", "q6_3"],           # Trip hierarchy
    7: ["q7_1", "q7_2", "q7_3", "q7_4"],  # Cross-dimension
    8: ["q8_1", "q8_2", "q8_3"],           # Max complexity
}

ALL_QUERIES = [q for flight in sorted(QUERY_FLIGHTS) for q in QUERY_FLIGHTS[flight]]

# Single partition key for all queries
QUERY_PARTITION_KEYS = {q: ["trip_id"] for q in ALL_QUERIES}

# Hierarchy level labels for Q5 and Q6
HIERARCHY_LABELS = {
    "q5_1": "Park (broadest, ~4-8%)",
    "q5_2": "Museum (mid, ~3-6%)",
    "q5_3": "Theatre (narrowest, ~1-3%)",
    "q6_1": "Anomaly (broadest)",
    "q6_2": "Anomaly + Far (mid)",
    "q6_3": "Anomaly + Far + Long (narrowest)",
}

# Spatial-heavy query subset
SPATIAL_QUERIES = ["q1_1", "q2_1", "q3_1", "q3_3", "q5_1", "q5_2", "q5_3", "q7_2"]


def load_query(query_dir, query_name):
    """Load a SQL query from file."""
    path = os.path.join(query_dir, f"{query_name}.sql")
    with open(path) as f:
        return f.read().strip()


def compute_fact_table_stats(executor):
    """
    Query COUNT(*) and MAX(trip_id) from taxi_trips.

    Returns dict mapping partition key name to total distinct count, e.g.:
        {"trip_id": 1000000}
    Also stores max_trip_id on the executor for bitsize computation.
    """
    rows, _ = executor.execute("SELECT COUNT(*), MAX(trip_id) FROM taxi_trips")
    total_count = rows[0][0]
    max_trip_id = rows[0][1]
    executor.max_trip_id = max_trip_id

    stats = {"trip_id": total_count}
    print(f"\nFact table (taxi_trips) statistics:")
    print(f"  trip_id: {total_count:,} rows, max value: {max_trip_id:,}")
    return stats


# --- PostgreSQL Execution ---

class PostgreSQLExecutor:
    """Execute queries against a PostgreSQL database with PostGIS."""

    def __init__(self):
        import psycopg
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

        self.conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER", "app_user"),
            password=os.getenv("DB_PASSWORD", ""),
            dbname=os.getenv("DB_NAME", "nyc_taxi_db"),
        )
        self.conn.autocommit = True
        self.max_trip_id = 0

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
    """Manage partition cache handlers for the trip_id partition key."""

    def __init__(self, cache_type, max_trip_id=0):
        self.cache_type = cache_type
        self.handlers = {}
        self.max_trip_id = max_trip_id

    def get_handler(self, partition_key):
        """Get or create a cache handler for a partition key."""
        if partition_key not in self.handlers:
            if self.cache_type == "postgresql_roaringbit":
                os.environ.setdefault("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "nyc_taxi_cache")
            elif self.cache_type == "postgresql_bit":
                os.environ.setdefault("PG_BIT_CACHE_TABLE_PREFIX", "nyc_taxi_cache")
                os.environ.setdefault(
                    "PG_BIT_CACHE_BITSIZE",
                    str(max(self.max_trip_id + 1000, 200000)),
                )
            elif self.cache_type == "postgresql_array":
                os.environ.setdefault("PG_ARRAY_CACHE_TABLE_PREFIX", "nyc_taxi_cache")

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

def populate_cache_for_query(executor, cache_manager, adapted_query, partition_keys, *, cache_query=None):
    """
    Populate cache for a query across all its partition keys.

    Args:
        cache_query: Query to use for hash generation. If None, uses adapted_query.
            When set, allows using original queries (with JOINs, GROUP BY, etc.)
            since clean_query() normalizes them to the same hashes.

    Returns dict with per-partition-key stats including population_time.
    """
    cache_query = cache_query or adapted_query
    stats = {}

    for pk in partition_keys:
        pk_stats = {"fragments_generated": 0, "fragments_executed": 0, "partition_keys_cached": 0}
        handler = cache_manager.get_handler(pk)

        pk_start = time.perf_counter()

        # Generate all variant fragments for this partition key
        pairs = generate_all_query_hash_pairs(
            query=cache_query,
            partition_key=pk,
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
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


def apply_cache_to_query(cache_manager, adapted_query, original_query, partition_keys, fact_stats=None, *, cache_query=None):
    """
    Apply cached restrictions for all partition keys to the original query.

    Args:
        cache_query: Query to use for hash generation. If None, uses adapted_query.

    Returns (enhanced_query, per_pk_stats, cache_apply_time).
    If fact_stats is provided, each pk_stats entry includes search_space_reduction.
    """
    cache_query = cache_query or adapted_query
    apply_start = time.perf_counter()
    enhanced = original_query
    pk_stats = {}

    for pk in partition_keys:
        handler = cache_manager.get_handler(pk)

        cached_keys, num_generated, num_hits = partitioncache.get_partition_keys(
            query=cache_query,
            cache_handler=handler.underlying_handler,
            partition_key=pk,
            min_component_size=1,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        cached_set_size = len(cached_keys) if cached_keys else 0
        entry = {
            "variants_checked": num_generated,
            "cache_hits": num_hits,
            "partition_keys_found": cached_set_size,
        }

        if fact_stats and pk in fact_stats and cached_set_size > 0:
            total = fact_stats[pk]
            reduction_pct = (1 - cached_set_size / total) * 100 if total > 0 else 0
            entry["search_space_reduction"] = {
                "total_distinct": total,
                "cached_set_size": cached_set_size,
                "reduction_pct": round(reduction_pct, 1),
            }

        pk_stats[pk] = entry

        if cached_keys:
            try:
                enhanced = partitioncache.extend_query_with_partition_keys(
                    enhanced,
                    cached_keys,
                    partition_key=pk,
                    method="IN",
                    p0_alias="t",
                )
            except Exception as e:
                print(f"    Warning: Could not extend query with {pk}: {e}")

    cache_apply_time = time.perf_counter() - apply_start
    return enhanced, pk_stats, cache_apply_time


def run_single_query(executor, cache_manager, query_name, query_dir, repeat=1, fact_stats=None):
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

    # 2. Populate cache (use original query — clean_query normalizes JOINs, GROUP BY, etc.)
    cache_start = time.perf_counter()
    cache_stats = populate_cache_for_query(executor, cache_manager, adapted_query, pks, cache_query=original_query)
    cache_pop_time = time.perf_counter() - cache_start
    result["cache_population_time"] = cache_pop_time
    result["cache_stats"] = cache_stats

    # 3. Apply cache and run enhanced query (with repeat)
    enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
        cache_manager, adapted_query, original_query, pks, fact_stats=fact_stats, cache_query=original_query,
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

def run_all_queries(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """Run all NYC Taxi queries."""
    print("\n" + "=" * 80)
    print(f"NYC Taxi Benchmark: All Queries ({len(ALL_QUERIES)} queries)")
    print("=" * 80)

    results = []
    for query_name in ALL_QUERIES:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_flight(executor, cache_manager, query_dir, flight_num, repeat=1, fact_stats=None):
    """Run a single query flight."""
    queries = QUERY_FLIGHTS.get(flight_num)
    if not queries:
        valid = ", ".join(str(f) for f in sorted(QUERY_FLIGHTS))
        print(f"Invalid flight number: {flight_num}. Choose from: {valid}")
        return []

    print(f"\n{'=' * 80}")
    print(f"NYC Taxi Benchmark: Flight Q{flight_num}")
    print(f"{'=' * 80}")

    results = []
    for query_name in queries:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_cross_dimension(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """
    Cross-dimension cache reuse evaluation.

    Runs F1-F4 sequentially to populate cache with diverse spatial and temporal
    conditions, then runs F7 (cross-dimension queries) to demonstrate cache reuse
    across different query themes sharing common spatial/temporal subexpressions.
    """
    print("\n" + "=" * 80)
    print("NYC Taxi Benchmark: Cross-Dimension Cache Reuse")
    print("=" * 80)

    results = []

    # Phase 1: Run F1-F4 sequentially to populate cache
    print("\n### Phase 1: Populate cache with F1-F4 (Tourism, Medical, Commuter, Nightlife)")
    print("Building up cache entries from diverse spatial and temporal conditions.\n")

    for flight_num in [1, 2, 3, 4]:
        queries = QUERY_FLIGHTS[flight_num]

        for i, query_name in enumerate(queries):
            adapted_query = load_query(os.path.join(query_dir, "adapted"), query_name)
            original_query = load_query(os.path.join(query_dir, "original"), query_name)
            pks = QUERY_PARTITION_KEYS[query_name]

            print(f"\n--- {query_name} (F{flight_num}.{i + 1}) ---")

            # Check for pre-existing cache hits BEFORE populating
            pre_hits = {}
            for pk in pks:
                handler = cache_manager.get_handler(pk)
                _, num_gen, num_hits = partitioncache.get_partition_keys(
                    query=adapted_query,
                    cache_handler=handler.underlying_handler,
                    partition_key=pk,
                    min_component_size=1,
                    auto_detect_partition_join=False,
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
                cache_manager, adapted_query, original_query, pks, fact_stats=fact_stats,
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

    # Phase 2: Run F7 (cross-dimension) showing reuse from F1-F4
    print("\n### Phase 2: F7 Cross-Dimension Queries")
    print("F7 queries combine conditions from multiple themes, reusing F1-F4 cache entries.\n")

    q7_queries = QUERY_FLIGHTS[7]

    for i, query_name in enumerate(q7_queries):
        adapted_query = load_query(os.path.join(query_dir, "adapted"), query_name)
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        pks = QUERY_PARTITION_KEYS[query_name]

        print(f"\n--- {query_name} (F7.{i + 1}) ---")

        # Check for pre-existing cache hits
        pre_hits = {}
        for pk in pks:
            handler = cache_manager.get_handler(pk)
            _, num_gen, num_hits = partitioncache.get_partition_keys(
                query=adapted_query,
                cache_handler=handler.underlying_handler,
                partition_key=pk,
                min_component_size=1,
                auto_detect_partition_join=False,
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
            cache_manager, adapted_query, original_query, pks, fact_stats=fact_stats,
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


def run_cold_vs_warm(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """Compare cold cache (no pre-existing entries) vs warm cache (reuse from prior queries)."""
    print("\n" + "=" * 80)
    print("NYC Taxi Benchmark: Cold vs Warm Cache")
    print("=" * 80)

    # Cold run: clear cache, run F7 flight
    print("\n### Cold Run (empty cache)")
    cache_manager.clear_all()

    cold_results = []
    for query_name in QUERY_FLIGHTS[7]:
        print(f"\n--- {query_name} (cold) ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats)
        result["mode"] = "cold"
        cold_results.append(result)
        print_query_result(result)

    # Warm run: keep cache, run F7 again
    print("\n### Warm Run (cache from cold run)")
    warm_results = []
    for query_name in QUERY_FLIGHTS[7]:
        print(f"\n--- {query_name} (warm) ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats)
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


def run_hierarchy(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """
    Hierarchical drill-down evaluation.

    Runs Q5 (start hierarchy: park -> museum -> theatre) then Q6 (trip hierarchy:
    anomaly -> anomaly+far -> anomaly+far+long) sequentially. Demonstrates
    increasing cache reuse as queries drill from broad to narrow within the same
    dimension.
    """
    print("\n" + "=" * 80)
    print("NYC Taxi Benchmark: Hierarchical Drill-Down")
    print("=" * 80)

    all_results = []

    for flight_num, flight_label in [
        (5, "Q5: Start Hierarchy (park -> museum -> theatre)"),
        (6, "Q6: Trip Hierarchy (anomaly -> anomaly+far -> anomaly+far+long)"),
    ]:
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
                    auto_detect_partition_join=False,
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
        print(f"\n{'Level':<40} {'Pre-Hits':>10} {'New Hits':>10} {'Reuse%':>8} {'Speedup':>9}")
        print("-" * 80)
        for r in flight_results:
            total_pre = sum(r["pre_existing_hits"][pk]["hits_before"] for pk in r["partition_keys"])
            total_after = sum(r["apply_stats"][pk]["cache_hits"] for pk in r["partition_keys"])
            total_new = total_after - total_pre
            reuse_pct = total_pre / total_after * 100 if total_after > 0 else 0
            print(f"{r['hierarchy_level']:<40} {total_pre:>10} {total_new:>10} "
                  f"{reuse_pct:>7.1f}% {r['speedup']:>8.2f}x")

        all_results.extend(flight_results)

    # Overall summary
    print(f"\n{'=' * 80}")
    all_match = all(r["result_match"] for r in all_results)
    print(f"Hierarchy drill-down summary:")
    print(f"  Queries evaluated: {len(all_results)}")
    print(f"  All results correct: {all_match}")

    return all_results


def run_spatial(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """
    Run only spatial-heavy queries.

    Selects the subset of queries with significant PostGIS spatial operations
    to isolate and measure spatial query acceleration.
    """
    print("\n" + "=" * 80)
    print(f"NYC Taxi Benchmark: Spatial-Heavy Queries ({len(SPATIAL_QUERIES)} queries)")
    print("=" * 80)
    print(f"Selected queries: {', '.join(SPATIAL_QUERIES)}")

    results = []
    for query_name in SPATIAL_QUERIES:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_backend_comparison(executor, cache_manager, query_dir, max_trip_id=0, repeat=1, fact_stats=None):
    """
    Compare different PostgreSQL cache backends on a fixed set of representative queries.

    Runs q1_1, q3_1, q5_1 with postgresql_array, postgresql_bit, and
    postgresql_roaringbit backends.
    """
    print("\n" + "=" * 80)
    print("NYC Taxi Benchmark: Backend Comparison")
    print("=" * 80)

    representative_queries = ["q1_1", "q3_1", "q5_1"]
    backends = ["postgresql_array", "postgresql_bit", "postgresql_roaringbit"]

    all_results = {}

    for backend in backends:
        print(f"\n### Backend: {backend}")

        try:
            cm = CacheManager(backend, max_trip_id=max_trip_id)
            # Test that we can create a handler
            cm.get_handler("trip_id")
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
                result = run_single_query(executor, cm, query_name, query_dir, repeat=repeat, fact_stats=fact_stats)
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

    # Search space reduction
    has_reduction = any(
        "search_space_reduction" in result["apply_stats"].get(pk, {})
        for pk in result["partition_keys"]
    )
    if has_reduction:
        print("  Search Space Reduction:")
        for pk in result["partition_keys"]:
            ssr = result["apply_stats"].get(pk, {}).get("search_space_reduction")
            if ssr:
                print(f"    {pk}: {ssr['total_distinct']:,} total -> "
                      f"{ssr['cached_set_size']:,} cached "
                      f"({ssr['reduction_pct']:.1f}% reduction)")

    cached_str = f"{result['cached_time']:.4f}s"
    if "cached_stddev" in result:
        cached_str += f" (stddev: {result['cached_stddev']:.4f}s)"
    print(f"  Cached: {result['cached_rows']} rows in {cached_str} "
          f"(speedup: {result['speedup']:.2f}x, match: {result['result_match']})")


def _avg_reduction(result):
    """Compute average search space reduction % across all PKs for a result."""
    reductions = []
    for pk in result.get("partition_keys", []):
        ssr = result.get("apply_stats", {}).get(pk, {}).get("search_space_reduction")
        if ssr:
            reductions.append(ssr["reduction_pct"])
    return sum(reductions) / len(reductions) if reductions else None


def print_summary(results):
    """Print overall summary."""
    has_stddev = any("baseline_stddev" in r for r in results)
    has_reduction = any(_avg_reduction(r) is not None for r in results)

    if has_stddev:
        header = f"{'Query':<8} {'Baseline':>10} {'B.Std':>8} {'Cached':>10} {'C.Std':>8} {'Speedup':>9} {'Match':>7}"
        if has_reduction:
            header += f" {'Reduction':>10}"
        print(f"\n{'=' * 80}")
        print(header)
        print("-" * len(header))
        for r in results:
            b_std = f"{r.get('baseline_stddev', 0):.4f}" if "baseline_stddev" in r else "n/a"
            c_std = f"{r.get('cached_stddev', 0):.4f}" if "cached_stddev" in r else "n/a"
            line = (f"{r['query']:<8} {r['baseline_time']:>9.4f}s {b_std:>8} "
                    f"{r['cached_time']:>9.4f}s {c_std:>8} "
                    f"{r['speedup']:>8.2f}x {'Y' if r['result_match'] else 'N':>6}")
            if has_reduction:
                avg_red = _avg_reduction(r)
                line += f" {avg_red:>9.1f}%" if avg_red is not None else f" {'n/a':>10}"
            print(line)
    else:
        header = f"{'Query':<8} {'Baseline':>10} {'Cached':>10} {'Speedup':>9} {'Match':>7}"
        if has_reduction:
            header += f" {'Reduction':>10}"
        print(f"\n{'=' * 80}")
        print(header)
        print("-" * len(header))
        for r in results:
            line = (f"{r['query']:<8} {r['baseline_time']:>9.4f}s {r['cached_time']:>9.4f}s "
                    f"{r['speedup']:>8.2f}x {'Y' if r['result_match'] else 'N':>6}")
            if has_reduction:
                avg_red = _avg_reduction(r)
                line += f" {avg_red:>9.1f}%" if avg_red is not None else f" {'n/a':>10}"
            print(line)

    avg_speedup = sum(r["speedup"] for r in results) / len(results) if results else 0
    all_match = all(r["result_match"] for r in results)
    print(f"\nAverage speedup: {avg_speedup:.2f}x")
    print(f"All results correct: {all_match}")


def main():
    parser = argparse.ArgumentParser(description="Run NYC Taxi benchmark with PartitionCache")
    parser.add_argument("--cache-backend", type=str, default="postgresql_array",
                        help="Cache backend: postgresql_array, postgresql_bit, postgresql_roaringbit")
    parser.add_argument("--mode", nargs="+", default=["all"],
                        help="Benchmark mode: all | flight N | cross-dimension | cold-vs-warm | hierarchy | spatial | backend-comparison")
    parser.add_argument("--repeat", type=int, default=1,
                        help="Number of repetitions for timing (default: 1)")
    parser.add_argument("--output", type=str, default=None,
                        help="Save results to JSON file")

    args = parser.parse_args()

    query_dir = os.path.join(os.path.dirname(__file__), "queries")

    # Set up executor (PostgreSQL only for PostGIS spatial queries)
    print("Using PostgreSQL (PostGIS)")
    executor = PostgreSQLExecutor()
    cache_type = args.cache_backend

    print(f"Cache backend: {cache_type}")
    if args.repeat > 1:
        print(f"Repeat: {args.repeat}")

    try:
        # Compute fact table stats for search space reduction metrics
        fact_stats = compute_fact_table_stats(executor)
        max_trip_id = executor.max_trip_id

        # Set up cache manager
        cache_manager = CacheManager(cache_type, max_trip_id=max_trip_id)

        mode = args.mode[0]

        if mode == "all":
            results = run_all_queries(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "flight":
            flight_num = int(args.mode[1]) if len(args.mode) > 1 else 1
            results = run_flight(executor, cache_manager, query_dir, flight_num, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "cross-dimension":
            results = run_cross_dimension(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "cold-vs-warm":
            results = run_cold_vs_warm(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "hierarchy":
            results = run_hierarchy(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "spatial":
            results = run_spatial(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "backend-comparison":
            results = run_backend_comparison(
                executor, cache_manager, query_dir,
                max_trip_id=max_trip_id, repeat=args.repeat, fact_stats=fact_stats,
            )
        else:
            print(f"Unknown mode: {mode}")
            sys.exit(1)

        # Save results
        if args.output:
            output_data = {
                "metadata": {
                    "mode": mode,
                    "db_backend": "postgresql",
                    "cache_backend": cache_type,
                    "repeat": args.repeat,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "fact_table_stats": fact_stats,
                    "max_trip_id": max_trip_id,
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

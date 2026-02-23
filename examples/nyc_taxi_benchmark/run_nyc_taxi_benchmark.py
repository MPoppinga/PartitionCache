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
    python run_nyc_taxi_benchmark.py --mode api-comparison
    python run_nyc_taxi_benchmark.py --mode all --api lazy --method IN_SUBQUERY
    python run_nyc_taxi_benchmark.py --mode backend-comparison --api both
"""

import argparse
import json
import os
import re
import statistics
import sys
import time
from datetime import UTC, datetime
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
    print("\nFact table (taxi_trips) statistics:")
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
        """Execute query and return (rows, elapsed_seconds).

        Handles multi-statement queries (e.g. from TMP_TABLE_IN/JOIN methods)
        by splitting on semicolons, executing setup statements first, then
        fetching results from the final SELECT.
        """
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            # Split multi-statement queries (CREATE TEMP TABLE; INSERT; ...; SELECT)
            statements = [s.strip() for s in query.split(";") if s.strip()]
            if len(statements) > 1:
                # Execute setup statements (CREATE, INSERT, INDEX, ANALYZE)
                for stmt in statements[:-1]:
                    cur.execute(stmt)
                # Execute final SELECT and fetch
                cur.execute(statements[-1])
            else:
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

def populate_cache_for_query(executor, cache_manager, query, partition_keys):
    """
    Populate cache for a query across all its partition keys.

    Uses the public PartitionCache API:
      1. generate_all_query_hash_pairs() decomposes the query into variant fragments
         (with keep_all_attributes=False to create variants by omitting conditions)
      2. Each fragment is executed against the database
      3. Results are stored via the PartitionCacheHelper

    Returns dict with per-partition-key stats including population_time.
    """
    stats = {}

    for pk in partition_keys:
        pk_stats = {"fragments_generated": 0, "fragments_executed": 0, "partition_keys_cached": 0}
        handler = cache_manager.get_handler(pk)

        pk_start = time.perf_counter()

        # Generate all variant fragments for this partition key.
        # keep_all_attributes=False creates variants by omitting individual conditions,
        # enabling cache reuse when queries share subsets of conditions.
        # No partition_join_table specified — all tables participate in variant generation.
        pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=pk,
            min_component_size=1,
            keep_all_attributes=False,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        # Filter out fragments where a dimension table appears in the outer FROM clause.
        # Multi-table fragments may assign osm_pois as t1, making "t1.trip_id" fail
        # (osm_pois has no trip_id column). However, EXISTS subqueries referencing
        # osm_pois are fine — they're evaluated within the fact table context.
        def fact_table_in_from(fragment_sql):
            m = re.search(r"FROM\s+(.*?)\s+WHERE", fragment_sql, re.IGNORECASE | re.DOTALL)
            if m:
                from_clause = m.group(1)
                return "taxi_trips" in from_clause and "osm_pois" not in from_clause
            return False
        pairs = [(q, h) for q, h in pairs if fact_table_in_from(q)]

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


def apply_cache_to_query(cache_manager, query, partition_keys, fact_stats=None, method="TMP_TABLE_IN"):
    """
    Apply cached restrictions for all partition keys to the query.

    Uses the public PartitionCache API:
      1. get_partition_keys() generates hashes (with fix_attributes=False internally)
         and intersects cached partition key sets
      2. extend_query_with_partition_keys() adds cache restriction to the query

    Supported methods: "IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"

    Returns (enhanced_query, per_pk_stats, cache_apply_time).
    If fact_stats is provided, each pk_stats entry includes search_space_reduction.
    """
    apply_start = time.perf_counter()
    enhanced = query
    pk_stats = {}

    for pk in partition_keys:
        handler = cache_manager.get_handler(pk)

        cached_keys, num_generated, num_hits = partitioncache.get_partition_keys(
            query=query,
            cache_handler=handler.underlying_handler,
            partition_key=pk,
            min_component_size=1,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
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
                    method=method,
                    p0_alias="t",
                )
            except Exception as e:
                print(f"    Warning: Could not extend query with {pk}: {e}")

    cache_apply_time = time.perf_counter() - apply_start
    return enhanced, pk_stats, cache_apply_time


def apply_cache_to_query_lazy(cache_manager, query, partition_keys, method="IN_SUBQUERY"):
    """
    Apply cached restrictions using the lazy API (database-side intersection).

    Uses partitioncache.apply_cache_lazy() which generates SQL subqueries
    instead of materializing partition keys in Python — PostgreSQL handles
    the intersection.

    Supported methods: "IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"

    Returns (enhanced_query, per_pk_stats, cache_apply_time).
    """
    apply_start = time.perf_counter()
    enhanced = query
    pk_stats = {}

    for pk in partition_keys:
        handler = cache_manager.get_handler(pk)

        enhanced, stats = partitioncache.apply_cache_lazy(
            query=enhanced,
            cache_handler=handler.underlying_handler,
            partition_key=pk,
            method=method,
            min_component_size=1,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
            p0_alias="t",
        )

        entry = {
            "variants_checked": stats.get("generated_variants", 0),
            "cache_hits": stats.get("cache_hits", 0),
        }
        pk_stats[pk] = entry

    cache_apply_time = time.perf_counter() - apply_start
    return enhanced, pk_stats, cache_apply_time


def run_single_query(executor, cache_manager, query_name, query_dir, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """Run a single query through the full benchmark pipeline.

    Workflow:
      1. Execute the original query as baseline
      2. Populate cache using generate_all_query_hash_pairs(keep_all_attributes=False)
         which decomposes the query into variant fragments by omitting conditions
      3. Apply cache using get_partition_keys() + extend_query_with_partition_keys()
      4. Execute the enhanced query and compare results
    """
    original_query = load_query(os.path.join(query_dir, "original"), query_name)
    pks = QUERY_PARTITION_KEYS[query_name]

    result = {
        "query": query_name,
        "partition_keys": pks,
        "api": api,
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

    # 2. Populate cache from original query with variant generation
    cache_start = time.perf_counter()
    cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks)
    cache_pop_time = time.perf_counter() - cache_start
    result["cache_population_time"] = cache_pop_time
    result["cache_stats"] = cache_stats

    # 3. Apply cache and run enhanced query (with repeat)
    if api == "lazy":
        enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(
            cache_manager, original_query, pks, method=method,
        )
    else:
        enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
            cache_manager, original_query, pks, fact_stats=fact_stats, method=method,
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

def run_all_queries(executor, cache_manager, query_dir, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """Run all NYC Taxi queries."""
    print("\n" + "=" * 80)
    print(f"NYC Taxi Benchmark: All Queries ({len(ALL_QUERIES)} queries, method={method}, api={api})")
    print("=" * 80)

    results = []
    for query_name in ALL_QUERIES:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats, method=method, api=api)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_flight(executor, cache_manager, query_dir, flight_num, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """Run a single query flight."""
    queries = QUERY_FLIGHTS.get(flight_num)
    if not queries:
        valid = ", ".join(str(f) for f in sorted(QUERY_FLIGHTS))
        print(f"Invalid flight number: {flight_num}. Choose from: {valid}")
        return []

    print(f"\n{'=' * 80}")
    print(f"NYC Taxi Benchmark: Flight Q{flight_num} (method={method}, api={api})")
    print(f"{'=' * 80}")

    results = []
    for query_name in queries:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats, method=method, api=api)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_cross_dimension(executor, cache_manager, query_dir, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
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
            original_query = load_query(os.path.join(query_dir, "original"), query_name)
            pks = QUERY_PARTITION_KEYS[query_name]

            print(f"\n--- {query_name} (F{flight_num}.{i + 1}) ---")

            # Check for pre-existing cache hits BEFORE populating
            pre_hits = {}
            for pk in pks:
                handler = cache_manager.get_handler(pk)
                _, num_gen, num_hits = partitioncache.get_partition_keys(
                    query=original_query,
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
            cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks)
            cache_pop_time = time.perf_counter() - cache_start

            # Apply and run (with repeat)
            if api == "lazy":
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(
                    cache_manager, original_query, pks, method=method,
                )
            else:
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                    cache_manager, original_query, pks, fact_stats=fact_stats, method=method,
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
                "api": api,
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
                      f"{pk_stats[pk].get('partition_keys_found', 'n/a')} keys")
            print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s "
                  f"(speedup: {speedup:.2f}x, match: {result['result_match']})")

    # Phase 2: Run F7 (cross-dimension) showing reuse from F1-F4
    print("\n### Phase 2: F7 Cross-Dimension Queries")
    print("F7 queries combine conditions from multiple themes, reusing F1-F4 cache entries.\n")

    q7_queries = QUERY_FLIGHTS[7]

    for i, query_name in enumerate(q7_queries):
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        pks = QUERY_PARTITION_KEYS[query_name]

        print(f"\n--- {query_name} (F7.{i + 1}) ---")

        # Check for pre-existing cache hits
        pre_hits = {}
        for pk in pks:
            handler = cache_manager.get_handler(pk)
            _, num_gen, num_hits = partitioncache.get_partition_keys(
                query=original_query,
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
        cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks)
        cache_pop_time = time.perf_counter() - cache_start

        # Apply and run (with repeat)
        if api == "lazy":
            enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(
                cache_manager, original_query, pks, method=method,
            )
        else:
            enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                cache_manager, original_query, pks, fact_stats=fact_stats,
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
            "api": api,
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
                  f"{pk_stats[pk].get('partition_keys_found', 'n/a')} keys{marker}")
        print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s "
              f"(speedup: {speedup:.2f}x, match: {result['result_match']})")

    # Summary
    total_cross_dim_hits = 0
    for r in results:
        if "pre_existing_hits" in r:
            for pk in r["partition_keys"]:
                total_cross_dim_hits += r["pre_existing_hits"][pk]["hits_before"]

    print(f"\n{'=' * 80}")
    print("Cross-dimension reuse summary:")
    print(f"  Total pre-existing cache hits across all queries: {total_cross_dim_hits}")
    print(f"  Queries evaluated: {len(results)}")
    all_match = all(r["result_match"] for r in results)
    print(f"  All results correct: {all_match}")

    return results


def run_cold_vs_warm(executor, cache_manager, query_dir, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
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
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats, method=method, api=api)
        result["mode"] = "cold"
        cold_results.append(result)
        print_query_result(result)

    # Warm run: keep cache, run F7 again
    print("\n### Warm Run (cache from cold run)")
    warm_results = []
    for query_name in QUERY_FLIGHTS[7]:
        print(f"\n--- {query_name} (warm) ---")
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats, method=method, api=api)
        result["mode"] = "warm"
        warm_results.append(result)
        print_query_result(result)

    # Comparison
    print(f"\n{'=' * 80}")
    print(f"{'Query':<8} {'Cold Total':>12} {'Warm Total':>12} {'Savings':>10}")
    print("-" * 44)
    for cold, warm in zip(cold_results, warm_results, strict=False):
        cold_total = cold["cache_population_time"] + cold["cached_time"]
        warm_total = warm["cache_population_time"] + warm["cached_time"]
        savings = (cold_total - warm_total) / cold_total * 100 if cold_total > 0 else 0
        print(f"{cold['query']:<8} {cold_total:>11.4f}s {warm_total:>11.4f}s {savings:>9.1f}%")

    return cold_results + warm_results


def run_hierarchy(executor, cache_manager, query_dir, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
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
        print("Constant conditions kept the same; only the target dimension drills deeper.\n")

        flight_results = []

        for query_name in queries:
            original_query = load_query(os.path.join(query_dir, "original"), query_name)
            pks = QUERY_PARTITION_KEYS[query_name]
            level_label = HIERARCHY_LABELS.get(query_name, query_name)

            print(f"\n--- {query_name}: {level_label} ---")

            # Check for pre-existing cache hits BEFORE populating
            pre_hits = {}
            for pk in pks:
                handler = cache_manager.get_handler(pk)
                _, num_gen, num_hits = partitioncache.get_partition_keys(
                    query=original_query,
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
            cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks)
            cache_pop_time = time.perf_counter() - cache_start

            # Apply cache and run enhanced query (with repeat)
            if api == "lazy":
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(
                    cache_manager, original_query, pks, method=method,
                )
            else:
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                    cache_manager, original_query, pks, method=method,
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
                      f"{pk_stats[pk].get('partition_keys_found', 'n/a')} keys, "
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
    print("Hierarchy drill-down summary:")
    print(f"  Queries evaluated: {len(all_results)}")
    print(f"  All results correct: {all_match}")

    return all_results


def run_spatial(executor, cache_manager, query_dir, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
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
        result = run_single_query(executor, cache_manager, query_name, query_dir, repeat=repeat, fact_stats=fact_stats, method=method, api=api)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_backend_comparison(executor, query_dir, max_trip_id=0, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """
    Compare different PostgreSQL cache backends on a fixed set of representative queries.

    Runs q1_1, q3_1, q5_1 with postgresql_array, postgresql_bit, and
    postgresql_roaringbit backends.

    Args:
        api: "non-lazy" (default), "lazy", or "both" to run both API paths per backend.
    """
    print("\n" + "=" * 80)
    print(f"NYC Taxi Benchmark: Backend Comparison (api={api})")
    print("=" * 80)

    representative_queries = ["q1_1", "q3_1", "q5_1"]
    backends = ["postgresql_array", "postgresql_bit", "postgresql_roaringbit"]

    # Determine which API modes to run
    if api == "both":
        api_modes = ["non-lazy", "lazy"]
    else:
        api_modes = [api]

    all_results = {}

    for backend in backends:
        for api_mode in api_modes:
            label = f"{backend}/{api_mode}" if len(api_modes) > 1 else backend
            print(f"\n### Backend: {label}")

            # Map method for lazy API
            effective_method = method
            if api_mode == "lazy" and method in ("IN", "VALUES"):
                effective_method = "IN_SUBQUERY"
                print(f"  (method auto-mapped to {effective_method} for lazy API)")

            try:
                cm = CacheManager(backend, max_trip_id=max_trip_id)
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
                    result = run_single_query(
                        executor, cm, query_name, query_dir,
                        repeat=repeat, fact_stats=fact_stats,
                        method=effective_method, api=api_mode,
                    )
                    result["cache_backend"] = backend
                    backend_results.append(result)
                    print_query_result(result)
                except Exception as e:
                    print(f"  Error: {e}")
                    backend_results.append({
                        "query": query_name,
                        "error": str(e),
                        "cache_backend": backend,
                        "api": api_mode,
                    })

            all_results[label] = backend_results

            try:
                cm.clear_all()
                cm.close()
            except Exception:
                pass

    # Comparison table
    print(f"\n{'=' * 80}")
    print("Backend Comparison Summary")
    header = f"{'Backend':<35} {'Query':<8} {'Pop Time':>10} {'Cached':>10} {'Speedup':>9}"
    if len(api_modes) > 1:
        header = f"{'Backend/API':<35} {'Query':<8} {'Pop Time':>10} {'Cached':>10} {'Speedup':>9}"
    print(f"\n{header}")
    print("-" * 75)
    for label, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{label:<35} {r['query']:<8} {'ERROR':>10} {'':>10} {'':>9}")
            else:
                print(f"{label:<35} {r['query']:<8} "
                      f"{r['cache_population_time']:>9.4f}s "
                      f"{r['cached_time']:>9.4f}s "
                      f"{r['speedup']:>8.2f}x")

    # Flatten results for return
    flat_results = []
    for _label, results in all_results.items():
        flat_results.extend(results)

    return flat_results


def run_method_comparison(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """
    Compare different query extension methods on a fixed set of queries.

    Runs representative queries with each method (IN, VALUES, TMP_TABLE_IN,
    TMP_TABLE_JOIN) to compare performance characteristics. Cache is populated
    once and reused across methods.
    """
    print("\n" + "=" * 80)
    print("NYC Taxi Benchmark: Method Comparison")
    print("=" * 80)

    representative_queries = ["q1_1", "q1_2", "q3_1"]
    methods = ["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]

    # First populate cache with all representative queries
    print("\n### Phase 1: Populate cache")
    for query_name in representative_queries:
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        pks = QUERY_PARTITION_KEYS[query_name]
        cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks)
        for pk, stats in cache_stats.items():
            print(f"  {query_name}/{pk}: {stats['fragments_generated']} fragments, "
                  f"{stats['fragments_executed']} executed")

    # Run each method
    all_results = {}
    for method_name in methods:
        print(f"\n### Method: {method_name}")
        method_results = []

        for query_name in representative_queries:
            print(f"\n--- {query_name} ({method_name}) ---")
            original_query = load_query(os.path.join(query_dir, "original"), query_name)
            pks = QUERY_PARTITION_KEYS[query_name]

            # Baseline
            rows, baseline_time = executor.execute(original_query)
            baseline_rows = len(rows)

            # Apply cache and run
            try:
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                    cache_manager, original_query, pks, fact_stats=fact_stats, method=method_name,
                )
                cached_rows_result, cached_time = executor.execute(enhanced_query)
                cached_rows = len(cached_rows_result)
                speedup = baseline_time / cached_time if cached_time > 0 else float("inf")
                match = baseline_rows == cached_rows

                result = {
                    "query": query_name,
                    "method": method_name,
                    "baseline_time": baseline_time,
                    "cached_time": cached_time,
                    "cache_apply_time": cache_apply_time,
                    "speedup": speedup,
                    "result_match": match,
                    "baseline_rows": baseline_rows,
                    "cached_rows": cached_rows,
                }
                method_results.append(result)

                print(f"  Baseline: {baseline_rows} rows in {baseline_time:.4f}s")
                print(f"  Cached: {cached_rows} rows in {cached_time:.4f}s "
                      f"(speedup: {speedup:.2f}x, match: {match})")
            except Exception as e:
                print(f"  Error: {e}")
                method_results.append({
                    "query": query_name,
                    "method": method_name,
                    "error": str(e),
                })

        all_results[method_name] = method_results

    # Comparison table
    print(f"\n{'=' * 80}")
    print("Method Comparison Summary")
    print(f"\n{'Method':<18} {'Query':<8} {'Baseline':>10} {'Cached':>10} {'Apply':>10} {'Speedup':>9} {'Match':>7}")
    print("-" * 75)
    for method_name, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{method_name:<18} {r['query']:<8} {'ERROR':>10}")
            else:
                print(f"{method_name:<18} {r['query']:<8} "
                      f"{r['baseline_time']:>9.4f}s "
                      f"{r['cached_time']:>9.4f}s "
                      f"{r['cache_apply_time']:>9.4f}s "
                      f"{r['speedup']:>8.2f}x "
                      f"{'Y' if r['result_match'] else 'N':>6}")

    # Flatten
    flat_results = []
    for _method_name, results in all_results.items():
        flat_results.extend(results)
    return flat_results


def run_api_comparison(executor, cache_manager, query_dir, repeat=1, fact_stats=None):
    """
    Compare non-lazy vs lazy API with all applicable methods.

    Tests all API+method combinations on representative queries:
      non-lazy: IN, VALUES, TMP_TABLE_IN, TMP_TABLE_JOIN
      lazy: IN_SUBQUERY, TMP_TABLE_IN, TMP_TABLE_JOIN

    Cache is populated once and reused across all combinations.
    """
    print("\n" + "=" * 80)
    print("NYC Taxi Benchmark: API Comparison (non-lazy vs lazy)")
    print("=" * 80)

    representative_queries = ["q1_1", "q1_2", "q3_1"]

    combinations = [
        ("non-lazy", "IN"),
        ("non-lazy", "VALUES"),
        ("non-lazy", "TMP_TABLE_IN"),
        ("non-lazy", "TMP_TABLE_JOIN"),
        ("lazy", "IN_SUBQUERY"),
        ("lazy", "TMP_TABLE_IN"),
        ("lazy", "TMP_TABLE_JOIN"),
    ]

    # Phase 1: Populate cache
    print("\n### Phase 1: Populate cache")
    for query_name in representative_queries:
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        pks = QUERY_PARTITION_KEYS[query_name]
        cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks)
        for pk, stats in cache_stats.items():
            print(f"  {query_name}/{pk}: {stats['fragments_generated']} fragments, "
                  f"{stats['fragments_executed']} executed")

    # Phase 2: Run baselines
    print("\n### Phase 2: Baselines")
    baselines = {}
    for query_name in representative_queries:
        original_query = load_query(os.path.join(query_dir, "original"), query_name)
        baseline_times = []
        baseline_rows = None
        for _ in range(repeat):
            rows, elapsed = executor.execute(original_query)
            baseline_times.append(elapsed)
            if baseline_rows is None:
                baseline_rows = rows
        baselines[query_name] = {
            "time": statistics.mean(baseline_times),
            "rows": len(baseline_rows),
        }
        print(f"  {query_name}: {baselines[query_name]['rows']} rows in {baselines[query_name]['time']:.4f}s")

    # Phase 3: Run each combination
    all_results = {}
    for api_mode, method_name in combinations:
        label = f"{api_mode}/{method_name}"
        print(f"\n### {label}")
        combo_results = []

        for query_name in representative_queries:
            print(f"\n--- {query_name} ({label}) ---")
            original_query = load_query(os.path.join(query_dir, "original"), query_name)
            pks = QUERY_PARTITION_KEYS[query_name]

            try:
                if api_mode == "lazy":
                    enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(
                        cache_manager, original_query, pks, method=method_name,
                    )
                else:
                    enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                        cache_manager, original_query, pks, fact_stats=fact_stats, method=method_name,
                    )

                cached_times = []
                cached_rows_result = None
                for _ in range(repeat):
                    rows, elapsed = executor.execute(enhanced_query)
                    cached_times.append(elapsed)
                    if cached_rows_result is None:
                        cached_rows_result = rows
                cached_time = statistics.mean(cached_times)
                cached_rows = len(cached_rows_result)

                baseline = baselines[query_name]
                speedup = baseline["time"] / cached_time if cached_time > 0 else float("inf")
                match = baseline["rows"] == cached_rows

                result = {
                    "query": query_name,
                    "api": api_mode,
                    "method": method_name,
                    "baseline_time": baseline["time"],
                    "cached_time": cached_time,
                    "cache_apply_time": cache_apply_time,
                    "speedup": speedup,
                    "result_match": match,
                    "baseline_rows": baseline["rows"],
                    "cached_rows": cached_rows,
                }
                combo_results.append(result)

                print(f"  Baseline: {baseline['rows']} rows in {baseline['time']:.4f}s")
                print(f"  Cached: {cached_rows} rows in {cached_time:.4f}s "
                      f"(speedup: {speedup:.2f}x, match: {match})")
            except Exception as e:
                print(f"  Error: {e}")
                combo_results.append({
                    "query": query_name,
                    "api": api_mode,
                    "method": method_name,
                    "error": str(e),
                })

        all_results[label] = combo_results

    # Comparison table
    print(f"\n{'=' * 80}")
    print("API Comparison Summary")
    print(f"\n{'API/Method':<25} {'Query':<8} {'Baseline':>10} {'Cached':>10} {'Apply':>10} {'Speedup':>9} {'Match':>7}")
    print("-" * 82)
    for label, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{label:<25} {r['query']:<8} {'ERROR':>10}")
            else:
                print(f"{label:<25} {r['query']:<8} "
                      f"{r['baseline_time']:>9.4f}s "
                      f"{r['cached_time']:>9.4f}s "
                      f"{r['cache_apply_time']:>9.4f}s "
                      f"{r['speedup']:>8.2f}x "
                      f"{'Y' if r['result_match'] else 'N':>6}")

    # Flatten
    flat_results = []
    for _label, results in all_results.items():
        flat_results.extend(results)
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
                        help="Benchmark mode: all | flight N | cross-dimension | cold-vs-warm | hierarchy | spatial | backend-comparison | method-comparison | api-comparison")
    parser.add_argument("--method", type=str, default="IN",
                        choices=["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "IN_SUBQUERY"],
                        help="Query extension method (default: IN)")
    parser.add_argument("--api", type=str, default="non-lazy",
                        choices=["non-lazy", "lazy", "both"],
                        help="API path: non-lazy (Python-side intersection), lazy (DB-side intersection), both (default: non-lazy)")
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

        method = args.method
        api = args.api

        # Auto-map invalid method/api combos
        if api == "lazy" and method in ("IN", "VALUES"):
            print(f"Note: method '{method}' not available for lazy API, using IN_SUBQUERY")
            method = "IN_SUBQUERY"
        if api == "non-lazy" and method == "IN_SUBQUERY":
            print("Note: method 'IN_SUBQUERY' not available for non-lazy API, using IN")
            method = "IN"

        print(f"Method: {method}")
        print(f"API: {api}")

        if mode == "all":
            results = run_all_queries(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "flight":
            flight_num = int(args.mode[1]) if len(args.mode) > 1 else 1
            results = run_flight(executor, cache_manager, query_dir, flight_num, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "cross-dimension":
            results = run_cross_dimension(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "cold-vs-warm":
            results = run_cold_vs_warm(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "hierarchy":
            results = run_hierarchy(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "spatial":
            results = run_spatial(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "backend-comparison":
            results = run_backend_comparison(
                executor, query_dir,
                max_trip_id=max_trip_id, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api,
            )
        elif mode == "method-comparison":
            results = run_method_comparison(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "api-comparison":
            results = run_api_comparison(executor, cache_manager, query_dir, repeat=args.repeat, fact_stats=fact_stats)
        else:
            print(f"Unknown mode: {mode}")
            sys.exit(1)

        # Save results
        if args.output:
            output_data = {
                "metadata": {
                    "mode": mode,
                    "method": method,
                    "api": api,
                    "db_backend": "postgresql",
                    "cache_backend": cache_type,
                    "repeat": args.repeat,
                    "timestamp": datetime.now(UTC).isoformat(),
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
        try:
            cache_manager.close()  # noqa: F821
        except NameError:
            pass
        executor.close()


if __name__ == "__main__":
    main()

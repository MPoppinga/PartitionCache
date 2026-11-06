#!/usr/bin/env python3
"""
Benchmark comparing spatial cache backends:
  a) Baseline (no cache)
  b) PostGIS BBox
  c) RocksDict H3 Grouped (hybrid: RocksDict + PostgreSQL)

Runs spatial queries from the OSM POI benchmark, populates each backend,
applies cache, and compares result correctness + performance.
"""

import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

# Load OSM POI environment
load_dotenv("examples/openstreetmap_poi/.env", override=True)

# =============================================================================
# Configuration
# =============================================================================

QUERY_DIR = Path("examples/openstreetmap_poi/testqueries_examples")

# Spatial queries to benchmark (subset for speed)
QUERIES = {
    "spatial_q1": QUERY_DIR / "spatial/q1.sql",
    "spatial_q2": QUERY_DIR / "spatial/q2.sql",
    "spatial_q3": QUERY_DIR / "spatial/q3.sql",
    "spatial_q4": QUERY_DIR / "spatial/q4.sql",
    "spatial_q5": QUERY_DIR / "spatial/q5.sql",
    "spatial_benchmark_qa1": QUERY_DIR / "spatial_benchmark/qa1.sql",
    "spatial_benchmark_qb1": QUERY_DIR / "spatial_benchmark/qb1.sql",
    "spatial_benchmark_qc1": QUERY_DIR / "spatial_benchmark/qc1.sql",
    "spatial_benchmark_qd1": QUERY_DIR / "spatial_benchmark/qd1.sql",
}

GEOMETRY_COLUMN = "geom"
SRID = 25832
H3_RESOLUTION = 9
PARTITION_KEY = "spatial_test"
BBOX_CELL_SIZE = 500

# =============================================================================
# Helpers
# =============================================================================


def get_pg_connection():
    import psycopg
    return psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "55432")),
        user=os.getenv("DB_USER", "osmuser"),
        password=os.getenv("DB_PASSWORD", "osmpassword"),
        dbname=os.getenv("DB_NAME", "osm_poi_db"),
        autocommit=True,
    )


def execute_query(conn, query: str) -> tuple[list, float]:
    """Execute query, return (rows, elapsed_seconds)."""
    with conn.cursor() as cur:
        start = time.perf_counter()
        statements = [s.strip() for s in query.split(";") if s.strip()]
        if len(statements) > 1:
            for stmt in statements[:-1]:
                cur.execute(stmt)
            cur.execute(statements[-1])
        else:
            cur.execute(query)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
    return rows, elapsed


def load_query(path: Path) -> str:
    return path.read_text().strip()


def generate_fragments(query: str) -> list[tuple[str, str]]:
    """Generate fragment SQL + hash pairs for a spatial query."""
    return generate_all_query_hash_pairs(
        query,
        partition_key=PARTITION_KEY,
        keep_all_attributes=True,
        follow_graph=True,
        strip_select=True,
        auto_detect_partition_join=True,
        skip_partition_key_joins=True,
        geometry_column=GEOMETRY_COLUMN,
        warn_no_partition_key=False,
    )


# =============================================================================
# Backend: PostGIS BBox
# =============================================================================


def setup_bbox_handler():
    """Create PostGIS BBox cache handler."""
    os.environ["PG_BBOX_CACHE_TABLE_PREFIX"] = "benchmark_bbox_test"
    os.environ["PG_BBOX_GEOMETRY_COLUMN"] = GEOMETRY_COLUMN
    os.environ["PG_BBOX_SRID"] = str(SRID)
    os.environ["PG_BBOX_CELL_SIZE"] = str(BBOX_CELL_SIZE)
    handler = partitioncache.get_cache_handler("postgis_bbox")
    handler.register_partition_key(PARTITION_KEY, "geometry")
    return handler


def populate_bbox(handler, conn, query: str) -> dict:
    """Populate BBox cache for a query."""
    pairs = generate_fragments(query)
    stats = {"fragments": len(pairs), "populated": 0}
    for fragment, hash_val in pairs:
        if handler.exists(hash_val, PARTITION_KEY):
            stats["populated"] += 1
            continue
        try:
            success = handler.set_cache_lazy(hash_val, fragment, PARTITION_KEY)
            if success:
                stats["populated"] += 1
        except Exception as e:
            print(f"    BBox fragment failed: {e}")
    return stats


def apply_bbox(handler, conn, query: str) -> tuple[str, dict]:
    """Apply BBox cache to query."""
    enhanced, stats = partitioncache.apply_cache(
        query=query,
        partition_key=PARTITION_KEY,
        cache_handler=handler,
        geometry_column=GEOMETRY_COLUMN,
        method="TMP_TABLE_IN",
        spatial_method="SUBDIVIDE_TMP_TABLE",
    )
    return enhanced, stats


# =============================================================================
# Backend: RocksDict H3 Grouped
# =============================================================================


def setup_h3_grouped_handler():
    """Create RocksDict H3 Grouped cache handler."""
    import shutil
    cache_dir = "/tmp/benchmark_rocksdict_h3_grouped"
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)

    os.environ["ROCKSDICT_H3_GROUPED_PATH"] = cache_dir
    os.environ["PG_H3_RESOLUTION"] = str(H3_RESOLUTION)
    os.environ["PG_H3_SRID"] = str(SRID)
    handler = partitioncache.get_cache_handler("rocksdict_h3_grouped")
    handler.register_partition_key(PARTITION_KEY, "geometry")
    return handler


def populate_h3_grouped(handler, conn, query: str) -> dict:
    """Populate RocksDict H3 Grouped cache for a query."""
    pairs = generate_fragments(query)
    stats = {"fragments": len(pairs), "populated": 0}

    for fragment, hash_val in pairs:
        if handler.exists(hash_val, PARTITION_KEY):
            stats["populated"] += 1
            continue
        try:
            # Execute fragment SQL via PostgreSQL
            with conn.cursor() as cur:
                cur.execute(fragment)
                rows = cur.fetchall()

            if not rows:
                handler.set_null(hash_val, PARTITION_KEY)
                stats["populated"] += 1
                continue

            # Convert each geometry column to H3 cell, build grouped match sets
            match_groups: list[frozenset[int]] = []
            seen: set[frozenset[int]] = set()
            for row in rows:
                cells: set[int] = set()
                for value in row:
                    if value is None:
                        continue
                    cell_id = handler.geom_to_h3_cell(value)
                    if cell_id is not None:
                        cells.add(cell_id)
                if cells:
                    group = frozenset(cells)
                    if group not in seen:
                        seen.add(group)
                        match_groups.append(group)

            if match_groups:
                handler.set_cache(hash_val, match_groups, PARTITION_KEY)
                handler.set_query(hash_val, fragment, PARTITION_KEY)
            else:
                handler.set_null(hash_val, PARTITION_KEY)
            stats["populated"] += 1
        except Exception as e:
            print(f"    H3 fragment failed: {e}")
    return stats


def apply_h3_grouped(handler, conn, query: str) -> tuple[str, dict]:
    """Apply RocksDict H3 Grouped cache to query."""
    enhanced, stats = partitioncache.apply_cache(
        query=query,
        partition_key=PARTITION_KEY,
        cache_handler=handler,
        geometry_column=GEOMETRY_COLUMN,
        method="TMP_TABLE_IN",
        spatial_method="SUBDIVIDE_TMP_TABLE",
    )
    return enhanced, stats


# =============================================================================
# Main benchmark
# =============================================================================


def run_benchmark():
    conn = get_pg_connection()

    # Setup handlers
    print("Setting up cache handlers...")
    bbox_handler = setup_bbox_handler()
    h3_handler = setup_h3_grouped_handler()

    results = []

    for qname, qpath in QUERIES.items():
        query = load_query(qpath)
        print(f"\n{'='*70}")
        print(f"Query: {qname}")
        print(f"{'='*70}")

        # Show query (abbreviated)
        q_lines = query.split("\n")
        for line in q_lines[:5]:
            print(f"  {line}")
        if len(q_lines) > 5:
            print("  ...")

        result = {"query": qname}

        # --- a) Baseline (no cache) ---
        print("\n  [Baseline] Executing without cache...")
        try:
            baseline_rows, baseline_time = execute_query(conn, query)
            baseline_count = len(baseline_rows)
            result["baseline_rows"] = baseline_count
            result["baseline_time"] = baseline_time
            print(f"    Rows: {baseline_count:,}  Time: {baseline_time:.3f}s")
        except Exception as e:
            print(f"    FAILED: {e}")
            result["baseline_error"] = str(e)
            results.append(result)
            continue

        # --- b) PostGIS BBox ---
        print("\n  [BBox] Populating cache...")
        try:
            pop_start = time.perf_counter()
            pop_stats = populate_bbox(bbox_handler, conn, query)
            pop_time = time.perf_counter() - pop_start
            result["bbox_populate_time"] = pop_time
            result["bbox_fragments"] = pop_stats["fragments"]
            print(f"    Fragments: {pop_stats['fragments']}, Populated: {pop_stats['populated']}, Time: {pop_time:.3f}s")

            print("  [BBox] Applying cache...")
            enhanced, cache_stats = apply_bbox(bbox_handler, conn, query)
            result["bbox_cache_stats"] = cache_stats

            if cache_stats.get("enhanced"):
                enhanced_rows, enhanced_time = execute_query(conn, enhanced)
                enhanced_count = len(enhanced_rows)
                result["bbox_rows"] = enhanced_count
                result["bbox_time"] = enhanced_time
                result["bbox_match"] = enhanced_count == baseline_count
                result["bbox_speedup"] = baseline_time / enhanced_time if enhanced_time > 0 else float("inf")
                status = "MATCH" if result["bbox_match"] else f"MISMATCH ({enhanced_count} vs {baseline_count})"
                print(f"    Rows: {enhanced_count:,}  Time: {enhanced_time:.3f}s  Speedup: {result['bbox_speedup']:.1f}x  {status}")
            else:
                print(f"    No cache hits (stats: {cache_stats})")
                result["bbox_rows"] = None
                result["bbox_time"] = None
                result["bbox_match"] = None
        except Exception as e:
            print(f"    BBox FAILED: {e}")
            result["bbox_error"] = str(e)

        # --- c) RocksDict H3 Grouped ---
        print("\n  [H3 Grouped] Populating cache...")
        try:
            pop_start = time.perf_counter()
            pop_stats = populate_h3_grouped(h3_handler, conn, query)
            pop_time = time.perf_counter() - pop_start
            result["h3g_populate_time"] = pop_time
            result["h3g_fragments"] = pop_stats["fragments"]
            print(f"    Fragments: {pop_stats['fragments']}, Populated: {pop_stats['populated']}, Time: {pop_time:.3f}s")

            print("  [H3 Grouped] Applying cache...")
            enhanced, cache_stats = apply_h3_grouped(h3_handler, conn, query)
            result["h3g_cache_stats"] = cache_stats

            if cache_stats.get("enhanced"):
                enhanced_rows, enhanced_time = execute_query(conn, enhanced)
                enhanced_count = len(enhanced_rows)
                result["h3g_rows"] = enhanced_count
                result["h3g_time"] = enhanced_time
                result["h3g_match"] = enhanced_count == baseline_count
                result["h3g_speedup"] = baseline_time / enhanced_time if enhanced_time > 0 else float("inf")
                status = "MATCH" if result["h3g_match"] else f"MISMATCH ({enhanced_count} vs {baseline_count})"
                print(f"    Rows: {enhanced_count:,}  Time: {enhanced_time:.3f}s  Speedup: {result['h3g_speedup']:.1f}x  {status}")
            else:
                print(f"    No cache hits (stats: {cache_stats})")
                result["h3g_rows"] = None
                result["h3g_time"] = None
                result["h3g_match"] = None
        except Exception as e:
            print(f"    H3 Grouped FAILED: {e}")
            import traceback
            traceback.print_exc()
            result["h3g_error"] = str(e)

        results.append(result)

    # --- Summary ---
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"{'Query':<25} {'Baseline':>10} {'BBox':>10} {'BBox':>7} {'H3Grp':>10} {'H3Grp':>7} {'BBox':>6} {'H3Grp':>6}")
    print(f"{'':<25} {'rows':>10} {'rows':>10} {'speed':>7} {'rows':>10} {'speed':>7} {'match':>6} {'match':>6}")
    print("-" * 95)

    for r in results:
        baseline = r.get("baseline_rows", "ERR")
        bbox_rows = r.get("bbox_rows", "N/A")
        bbox_speed = f"{r['bbox_speedup']:.1f}x" if r.get("bbox_speedup") else "N/A"
        h3g_rows = r.get("h3g_rows", "N/A")
        h3g_speed = f"{r['h3g_speedup']:.1f}x" if r.get("h3g_speedup") else "N/A"
        bbox_match = "OK" if r.get("bbox_match") else ("MISS" if r.get("bbox_match") is False else "N/A")
        h3g_match = "OK" if r.get("h3g_match") else ("MISS" if r.get("h3g_match") is False else "N/A")
        print(f"{r['query']:<25} {str(baseline):>10} {str(bbox_rows):>10} {bbox_speed:>7} {str(h3g_rows):>10} {h3g_speed:>7} {bbox_match:>6} {h3g_match:>6}")

    # Correctness summary
    bbox_correct = sum(1 for r in results if r.get("bbox_match") is True)
    bbox_total = sum(1 for r in results if r.get("bbox_match") is not None)
    h3g_correct = sum(1 for r in results if r.get("h3g_match") is True)
    h3g_total = sum(1 for r in results if r.get("h3g_match") is not None)
    print(f"\nCorrectness: BBox {bbox_correct}/{bbox_total}, H3 Grouped {h3g_correct}/{h3g_total}")

    # Cleanup
    h3_handler.close()
    bbox_handler.close()
    conn.close()


if __name__ == "__main__":
    run_benchmark()

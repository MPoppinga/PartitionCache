#!/usr/bin/env python3
"""
Benchmark: H3 Cell Lookup vs BBox vs Baseline.

Compares spatial cache filtering approaches:
  a) Baseline (no cache)
  b) PostGIS BBox (existing geometry-based filtering)
  c) H3 Cell Lookup — inline mode (expression B-tree index)
  d) H3 Cell Lookup — MV mode (materialized view JOIN)

For each query: measures execution time, runs EXPLAIN ANALYZE,
checks result correctness, and reports optimization potential.

Prerequisites:
  - PostgreSQL with h3, h3_postgis, postgis extensions
  - Materialized view: pois_h3_cells (created by pcache-manage setup h3-cells)
  - Expression index: idx_pois_h3_cell on pois
  - pip install h3 psycopg python-dotenv
"""

import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

load_dotenv("examples/openstreetmap_poi/.env", override=True)

# =============================================================================
# Configuration
# =============================================================================

QUERY_DIR = Path("examples/openstreetmap_poi/testqueries_examples")

QUERIES = {
    "spatial_q1": QUERY_DIR / "spatial/q1.sql",
    "spatial_q2": QUERY_DIR / "spatial/q2.sql",
    "spatial_q3": QUERY_DIR / "spatial/q3.sql",
    "spatial_q4": QUERY_DIR / "spatial/q4.sql",
    "spatial_q5": QUERY_DIR / "spatial/q5.sql",
    "bench_qa1": QUERY_DIR / "spatial_benchmark/qa1.sql",
    "bench_qb1": QUERY_DIR / "spatial_benchmark/qb1.sql",
    "bench_qc1": QUERY_DIR / "spatial_benchmark/qc1.sql",
    "bench_qd1": QUERY_DIR / "spatial_benchmark/qd1.sql",
}

GEOMETRY_COLUMN = "geom"
SRID = 25832
H3_RESOLUTION = 9
PARTITION_KEY_BBOX = "bench_bbox"
PARTITION_KEY_H3_INLINE = "bench_h3_inline"
PARTITION_KEY_H3_MV = "bench_h3_mv"
PARTITION_KEY_H3_COL = "bench_h3_col"
BBOX_CELL_SIZE = 500
N_RUNS = 3  # Average over N runs for timing

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


def execute_query_n(conn, query: str, n: int = N_RUNS) -> tuple[list, float, float, float]:
    """Execute query N times, return (rows, median_time, min_time, max_time)."""
    times = []
    rows = []
    for _ in range(n):
        r, t = execute_query(conn, query)
        rows = r
        times.append(t)
    times.sort()
    return rows, times[len(times) // 2], times[0], times[-1]


def explain_analyze(conn, query: str) -> tuple[list[str], dict]:
    """Run EXPLAIN (ANALYZE, BUFFERS) and parse key metrics."""
    with conn.cursor() as cur:
        statements = [s.strip() for s in query.split(";") if s.strip()]
        if len(statements) > 1:
            for stmt in statements[:-1]:
                cur.execute(stmt)
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {statements[-1]}"
        else:
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"
        cur.execute(explain_sql)
        plan_lines = [row[0] for row in cur.fetchall()]

    plan_text = "\n".join(plan_lines)
    plan_lower = plan_text.lower()

    # Scan type
    gist_used = ("index scan" in plan_lower or "bitmap index scan" in plan_lower) and "gist" in plan_lower
    btree_used = ("index scan" in plan_lower or "bitmap index scan" in plan_lower) and "btree" in plan_lower
    seq_scan = "seq scan" in plan_lower

    if gist_used:
        scan_type = "GiST"
    elif btree_used:
        scan_type = "BTree"
    elif "index" in plan_lower:
        scan_type = "Index"
    elif seq_scan:
        scan_type = "SeqScan"
    else:
        scan_type = "Other"

    # Execution time
    exec_time_ms = 0.0
    for line in plan_lines:
        m = re.search(r"Execution Time:\s+([\d.]+)\s+ms", line)
        if m:
            exec_time_ms = float(m.group(1))

    # Planning time
    plan_time_ms = 0.0
    for line in plan_lines:
        m = re.search(r"Planning Time:\s+([\d.]+)\s+ms", line)
        if m:
            plan_time_ms = float(m.group(1))

    # Buffers
    shared_hit = shared_read = 0
    for line in plan_lines:
        m = re.search(r"shared hit=(\d+)", line)
        if m:
            shared_hit += int(m.group(1))
        m = re.search(r"shared read=(\d+)", line)
        if m:
            shared_read += int(m.group(1))

    return plan_lines, {
        "scan_type": scan_type,
        "gist_used": gist_used,
        "btree_used": btree_used,
        "seq_scan": seq_scan,
        "exec_time_ms": exec_time_ms,
        "plan_time_ms": plan_time_ms,
        "shared_hit": shared_hit,
        "shared_read": shared_read,
    }


def load_query(path: Path) -> str:
    return path.read_text().strip()


def generate_fragments(query: str, partition_key: str) -> list[tuple[str, str]]:
    return generate_all_query_hash_pairs(
        query,
        partition_key=partition_key,
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
    os.environ["PG_BBOX_CACHE_TABLE_PREFIX"] = "benchmark_bbox_test"
    os.environ["PG_BBOX_GEOMETRY_COLUMN"] = GEOMETRY_COLUMN
    os.environ["PG_BBOX_SRID"] = str(SRID)
    os.environ["PG_BBOX_CELL_SIZE"] = str(BBOX_CELL_SIZE)
    handler = partitioncache.get_cache_handler("postgis_bbox")
    handler.register_partition_key(PARTITION_KEY_BBOX, "geometry")
    return handler


def populate_bbox(handler, conn, query: str) -> dict:
    pairs = generate_fragments(query, PARTITION_KEY_BBOX)
    stats = {"fragments": len(pairs), "populated": 0}
    for fragment, hash_val in pairs:
        if handler.exists(hash_val, PARTITION_KEY_BBOX):
            stats["populated"] += 1
            continue
        try:
            success = handler.set_cache_lazy(hash_val, fragment, PARTITION_KEY_BBOX)
            if success:
                stats["populated"] += 1
        except Exception as e:
            print(f"    BBox fragment failed: {e}")
    return stats


def apply_bbox_lazy(handler, conn, query: str) -> tuple[str, dict]:
    buffer_dist = _extract_max_st_dwithin(query)
    kwargs = {
        "query": query,
        "cache_handler": handler,
        "partition_key": PARTITION_KEY_BBOX,
        "method": "TMP_TABLE_IN",
        "geometry_column": GEOMETRY_COLUMN,
        "spatial_method": "SUBDIVIDE_TMP_TABLE",
        "p0_alias": "p1",
        "skip_partition_key_joins": True,
        "follow_graph": True,
        "auto_detect_partition_join": True,
    }
    if buffer_dist > 0:
        kwargs["buffer_distance"] = buffer_dist
    return partitioncache.apply_cache_lazy(**kwargs)


# =============================================================================
# Backend: RocksDict H3 Grouped (with configurable cell_mode)
# =============================================================================


def setup_h3_handler(cell_mode: str, partition_key: str, cache_dir: str):
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)

    os.environ["ROCKSDICT_H3_GROUPED_PATH"] = cache_dir
    os.environ["PG_H3_RESOLUTION"] = str(H3_RESOLUTION)
    os.environ["PG_H3_SRID"] = str(SRID)
    os.environ["H3_CELL_MODE"] = cell_mode

    if cell_mode == "mv":
        os.environ["H3_CELL_TABLE"] = "pois_h3_cells"
        os.environ["H3_CELL_COLUMN"] = "h3_cell_id"
        os.environ["H3_CELL_ID_COLUMN"] = "id"
    elif cell_mode == "column":
        os.environ["H3_CELL_COLUMN"] = "h3_cell_id"
        os.environ.pop("H3_CELL_TABLE", None)
        os.environ.pop("H3_CELL_ID_COLUMN", None)
    else:
        # inline mode — remove MV-related env vars
        os.environ.pop("H3_CELL_TABLE", None)
        os.environ.pop("H3_CELL_ID_COLUMN", None)

    handler = partitioncache.get_cache_handler("rocksdict_h3_grouped")
    handler.register_partition_key(partition_key, "geometry")
    return handler


def populate_h3(handler, conn, query: str, partition_key: str) -> dict:
    pairs = generate_fragments(query, partition_key)
    stats = {"fragments": len(pairs), "populated": 0}
    for fragment, hash_val in pairs:
        if handler.exists(hash_val, partition_key):
            stats["populated"] += 1
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(fragment)
                rows = cur.fetchall()
            if not rows:
                handler.set_null(hash_val, partition_key)
                stats["populated"] += 1
                continue
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
                handler.set_cache(hash_val, match_groups, partition_key)
            else:
                handler.set_null(hash_val, partition_key)
            stats["populated"] += 1
        except Exception as e:
            print(f"    H3 fragment failed: {e}")
    return stats


def apply_h3_lazy(handler, conn, query: str, partition_key: str) -> tuple[str, dict]:
    buffer_dist = _extract_max_st_dwithin(query)
    kwargs = {
        "query": query,
        "cache_handler": handler,
        "partition_key": partition_key,
        "method": "TMP_TABLE_IN",
        "geometry_column": GEOMETRY_COLUMN,
        "p0_alias": "p1",
        "skip_partition_key_joins": True,
        "follow_graph": True,
        "auto_detect_partition_join": True,
    }
    if buffer_dist > 0:
        kwargs["buffer_distance"] = buffer_dist
    return partitioncache.apply_cache_lazy(**kwargs)


def _extract_max_st_dwithin(query: str) -> float:
    distances = []
    for m in re.finditer(r"ST_DWithin\s*\([^)]*,\s*(\d+(?:\.\d+)?)\s*\)", query, re.IGNORECASE):
        try:
            distances.append(float(m.group(1)))
        except ValueError:
            pass
    return max(distances) if distances else 0.0


# =============================================================================
# Main benchmark
# =============================================================================


def run_benchmark():
    conn = get_pg_connection()

    # Warm up connection
    execute_query(conn, "SELECT 1")

    print("Setting up cache handlers...")
    bbox_handler = setup_bbox_handler()
    h3_inline_handler = setup_h3_handler("inline", PARTITION_KEY_H3_INLINE, "/tmp/bench_h3_inline")
    h3_mv_handler = setup_h3_handler("mv", PARTITION_KEY_H3_MV, "/tmp/bench_h3_mv")
    h3_col_handler = setup_h3_handler("column", PARTITION_KEY_H3_COL, "/tmp/bench_h3_col")

    all_results = []

    for qname, qpath in QUERIES.items():
        if not qpath.exists():
            print(f"\n  Skipping {qname}: file not found")
            continue
        query = load_query(qpath)
        print(f"\n{'='*80}")
        print(f"Query: {qname}")
        print(f"{'='*80}")
        q_lines = query.split("\n")
        for line in q_lines[:4]:
            print(f"  {line}")
        if len(q_lines) > 4:
            print("  ...")

        result: dict = {"query": qname}

        # --- Baseline ---
        print("\n  [Baseline] Running...")
        try:
            rows, med_t, min_t, max_t = execute_query_n(conn, query)
            result["baseline_rows"] = len(rows)
            result["baseline_time"] = med_t
            result["baseline_min"] = min_t
            result["baseline_max"] = max_t
            print(f"    Rows: {len(rows):,}  Time: {med_t:.3f}s (min={min_t:.3f}, max={max_t:.3f})")

            # EXPLAIN ANALYZE
            plan_lines, plan_info = explain_analyze(conn, query)
            result["baseline_plan"] = plan_info
            result["baseline_plan_lines"] = plan_lines
            print(f"    Scan: {plan_info['scan_type']}  ExecTime: {plan_info['exec_time_ms']:.1f}ms  "
                  f"Buffers(hit/read): {plan_info['shared_hit']}/{plan_info['shared_read']}")
        except Exception as e:
            print(f"    FAILED: {e}")
            result["baseline_error"] = str(e)
            all_results.append(result)
            continue

        # --- BBox ---
        print("\n  [BBox] Populating + Applying...")
        try:
            pop_stats = populate_bbox(bbox_handler, conn, query)
            result["bbox_fragments"] = pop_stats["fragments"]

            enhanced, cache_stats = apply_bbox_lazy(bbox_handler, conn, query)
            result["bbox_cache_stats"] = cache_stats

            if cache_stats.get("enhanced"):
                rows_e, med_t, min_t, max_t = execute_query_n(conn, enhanced)
                result["bbox_rows"] = len(rows_e)
                result["bbox_time"] = med_t
                result["bbox_min"] = min_t
                result["bbox_max"] = max_t
                result["bbox_match"] = len(rows_e) == result["baseline_rows"]
                result["bbox_speedup"] = result["baseline_time"] / med_t if med_t > 0 else float("inf")

                plan_lines, plan_info = explain_analyze(conn, enhanced)
                result["bbox_plan"] = plan_info
                result["bbox_plan_lines"] = plan_lines
                result["bbox_enhanced_sql"] = enhanced

                status = "MATCH" if result["bbox_match"] else f"MISMATCH ({len(rows_e)} vs {result['baseline_rows']})"
                print(f"    Rows: {len(rows_e):,}  Time: {med_t:.3f}s  Speedup: {result['bbox_speedup']:.1f}x  {status}")
                print(f"    Scan: {plan_info['scan_type']}  GiST: {plan_info['gist_used']}  "
                      f"ExecTime: {plan_info['exec_time_ms']:.1f}ms  "
                      f"Buffers: {plan_info['shared_hit']}/{plan_info['shared_read']}")
            else:
                print(f"    No cache hits")
                result["bbox_rows"] = None
        except Exception as e:
            print(f"    BBox FAILED: {e}")
            import traceback
            traceback.print_exc()
            result["bbox_error"] = str(e)

        # --- H3 Inline ---
        print("\n  [H3 Inline] Populating + Applying...")
        try:
            pop_stats = populate_h3(h3_inline_handler, conn, query, PARTITION_KEY_H3_INLINE)
            result["h3_inline_fragments"] = pop_stats["fragments"]

            enhanced, cache_stats = apply_h3_lazy(h3_inline_handler, conn, query, PARTITION_KEY_H3_INLINE)
            result["h3_inline_cache_stats"] = cache_stats

            if cache_stats.get("enhanced"):
                rows_e, med_t, min_t, max_t = execute_query_n(conn, enhanced)
                result["h3_inline_rows"] = len(rows_e)
                result["h3_inline_time"] = med_t
                result["h3_inline_min"] = min_t
                result["h3_inline_max"] = max_t
                result["h3_inline_match"] = len(rows_e) >= result["baseline_rows"]
                result["h3_inline_superset_size"] = len(rows_e) - result["baseline_rows"]
                result["h3_inline_speedup"] = result["baseline_time"] / med_t if med_t > 0 else float("inf")

                plan_lines, plan_info = explain_analyze(conn, enhanced)
                result["h3_inline_plan"] = plan_info
                result["h3_inline_plan_lines"] = plan_lines
                result["h3_inline_enhanced_sql"] = enhanced

                superset = f" (+{result['h3_inline_superset_size']})" if result["h3_inline_superset_size"] > 0 else ""
                match_str = "SUPERSET" if result["h3_inline_match"] and result["h3_inline_superset_size"] > 0 else ("MATCH" if result["h3_inline_match"] else "MISMATCH")
                print(f"    Rows: {len(rows_e):,}{superset}  Time: {med_t:.3f}s  Speedup: {result['h3_inline_speedup']:.1f}x  {match_str}")
                print(f"    Scan: {plan_info['scan_type']}  BTree: {plan_info['btree_used']}  "
                      f"ExecTime: {plan_info['exec_time_ms']:.1f}ms  "
                      f"Buffers: {plan_info['shared_hit']}/{plan_info['shared_read']}")
            else:
                print(f"    No cache hits")
                result["h3_inline_rows"] = None
        except Exception as e:
            print(f"    H3 Inline FAILED: {e}")
            import traceback
            traceback.print_exc()
            result["h3_inline_error"] = str(e)

        # --- H3 MV ---
        print("\n  [H3 MV] Populating + Applying...")
        try:
            pop_stats = populate_h3(h3_mv_handler, conn, query, PARTITION_KEY_H3_MV)
            result["h3_mv_fragments"] = pop_stats["fragments"]

            enhanced, cache_stats = apply_h3_lazy(h3_mv_handler, conn, query, PARTITION_KEY_H3_MV)
            result["h3_mv_cache_stats"] = cache_stats

            if cache_stats.get("enhanced"):
                rows_e, med_t, min_t, max_t = execute_query_n(conn, enhanced)
                result["h3_mv_rows"] = len(rows_e)
                result["h3_mv_time"] = med_t
                result["h3_mv_min"] = min_t
                result["h3_mv_max"] = max_t
                result["h3_mv_match"] = len(rows_e) >= result["baseline_rows"]
                result["h3_mv_superset_size"] = len(rows_e) - result["baseline_rows"]
                result["h3_mv_speedup"] = result["baseline_time"] / med_t if med_t > 0 else float("inf")

                plan_lines, plan_info = explain_analyze(conn, enhanced)
                result["h3_mv_plan"] = plan_info
                result["h3_mv_plan_lines"] = plan_lines
                result["h3_mv_enhanced_sql"] = enhanced

                superset = f" (+{result['h3_mv_superset_size']})" if result["h3_mv_superset_size"] > 0 else ""
                match_str = "SUPERSET" if result["h3_mv_match"] and result["h3_mv_superset_size"] > 0 else ("MATCH" if result["h3_mv_match"] else "MISMATCH")
                print(f"    Rows: {len(rows_e):,}{superset}  Time: {med_t:.3f}s  Speedup: {result['h3_mv_speedup']:.1f}x  {match_str}")
                print(f"    Scan: {plan_info['scan_type']}  BTree: {plan_info['btree_used']}  "
                      f"ExecTime: {plan_info['exec_time_ms']:.1f}ms  "
                      f"Buffers: {plan_info['shared_hit']}/{plan_info['shared_read']}")
            else:
                print(f"    No cache hits")
                result["h3_mv_rows"] = None
        except Exception as e:
            print(f"    H3 MV FAILED: {e}")
            import traceback
            traceback.print_exc()
            result["h3_mv_error"] = str(e)

        # --- H3 Column (with filter_all_tables optimization) ---
        print("\n  [H3 Column] Populating + Applying...")
        try:
            pop_stats = populate_h3(h3_col_handler, conn, query, PARTITION_KEY_H3_COL)
            result["h3_col_fragments"] = pop_stats["fragments"]

            enhanced, cache_stats = apply_h3_lazy(h3_col_handler, conn, query, PARTITION_KEY_H3_COL)
            result["h3_col_cache_stats"] = cache_stats

            if cache_stats.get("enhanced"):
                rows_e, med_t, min_t, max_t = execute_query_n(conn, enhanced)
                result["h3_col_rows"] = len(rows_e)
                result["h3_col_time"] = med_t
                result["h3_col_min"] = min_t
                result["h3_col_max"] = max_t
                result["h3_col_match"] = len(rows_e) >= result["baseline_rows"]
                result["h3_col_superset_size"] = len(rows_e) - result["baseline_rows"]
                result["h3_col_speedup"] = result["baseline_time"] / med_t if med_t > 0 else float("inf")

                plan_lines, plan_info = explain_analyze(conn, enhanced)
                result["h3_col_plan"] = plan_info
                result["h3_col_plan_lines"] = plan_lines
                result["h3_col_enhanced_sql"] = enhanced

                superset = f" (+{result['h3_col_superset_size']})" if result["h3_col_superset_size"] > 0 else ""
                match_str = "SUPERSET" if result["h3_col_match"] and result["h3_col_superset_size"] > 0 else ("MATCH" if result["h3_col_match"] else "MISMATCH")
                print(f"    Rows: {len(rows_e):,}{superset}  Time: {med_t:.3f}s  Speedup: {result['h3_col_speedup']:.1f}x  {match_str}")
                print(f"    Scan: {plan_info['scan_type']}  BTree: {plan_info['btree_used']}  "
                      f"ExecTime: {plan_info['exec_time_ms']:.1f}ms  "
                      f"Buffers: {plan_info['shared_hit']}/{plan_info['shared_read']}")
            else:
                print(f"    No cache hits")
                result["h3_col_rows"] = None
        except Exception as e:
            print(f"    H3 Column FAILED: {e}")
            import traceback
            traceback.print_exc()
            result["h3_col_error"] = str(e)

        all_results.append(result)

    # ==========================================================================
    # Summary Table
    # ==========================================================================
    print(f"\n{'='*160}")
    print("PERFORMANCE SUMMARY")
    print(f"{'='*160}")
    print(f"{'Query':<15} {'Baseline':>10} {'BBox':>10} {'BBox':>7} {'H3 Inl':>10} {'H3 Inl':>7} {'H3 MV':>10} {'H3 MV':>7} {'H3 Col':>10} {'H3 Col':>7} {'BBox':>6} {'H3Inl':>6} {'H3MV':>6} {'H3Col':>6}")
    print(f"{'':<15} {'time(s)':>10} {'time(s)':>10} {'speed':>7} {'time(s)':>10} {'speed':>7} {'time(s)':>10} {'speed':>7} {'time(s)':>10} {'speed':>7} {'scan':>6} {'scan':>6} {'scan':>6} {'scan':>6}")
    print("-" * 160)

    for r in all_results:
        base_t = f"{r['baseline_time']:.3f}" if "baseline_time" in r else "ERR"
        bbox_t = f"{r['bbox_time']:.3f}" if r.get("bbox_time") else "N/A"
        bbox_s = f"{r['bbox_speedup']:.1f}x" if r.get("bbox_speedup") else "N/A"
        h3i_t = f"{r['h3_inline_time']:.3f}" if r.get("h3_inline_time") else "N/A"
        h3i_s = f"{r['h3_inline_speedup']:.1f}x" if r.get("h3_inline_speedup") else "N/A"
        h3m_t = f"{r['h3_mv_time']:.3f}" if r.get("h3_mv_time") else "N/A"
        h3m_s = f"{r['h3_mv_speedup']:.1f}x" if r.get("h3_mv_speedup") else "N/A"
        h3c_t = f"{r['h3_col_time']:.3f}" if r.get("h3_col_time") else "N/A"
        h3c_s = f"{r['h3_col_speedup']:.1f}x" if r.get("h3_col_speedup") else "N/A"
        bbox_scan = r.get("bbox_plan", {}).get("scan_type", "N/A")
        h3i_scan = r.get("h3_inline_plan", {}).get("scan_type", "N/A")
        h3m_scan = r.get("h3_mv_plan", {}).get("scan_type", "N/A")
        h3c_scan = r.get("h3_col_plan", {}).get("scan_type", "N/A")
        print(f"{r['query']:<15} {base_t:>10} {bbox_t:>10} {bbox_s:>7} {h3i_t:>10} {h3i_s:>7} {h3m_t:>10} {h3m_s:>7} {h3c_t:>10} {h3c_s:>7} {bbox_scan:>6} {h3i_scan:>6} {h3m_scan:>6} {h3c_scan:>6}")

    # Correctness Table
    print(f"\n{'='*120}")
    print("CORRECTNESS (H3 results are supersets — more rows is expected)")
    print(f"{'='*120}")
    print(f"{'Query':<15} {'Base rows':>10} {'BBox rows':>10} {'BBox':>6} {'H3Inl rows':>10} {'H3Inl +':>8} {'H3MV rows':>10} {'H3MV +':>8} {'H3Col rows':>10} {'H3Col +':>8}")
    print("-" * 120)

    for r in all_results:
        base_r = str(r.get("baseline_rows", "ERR"))
        bbox_r = str(r.get("bbox_rows", "N/A"))
        bbox_ok = "OK" if r.get("bbox_match") else ("MISS" if r.get("bbox_match") is False else "N/A")
        h3i_r = str(r.get("h3_inline_rows", "N/A"))
        h3i_extra = f"+{r['h3_inline_superset_size']}" if r.get("h3_inline_superset_size", 0) > 0 else "0"
        h3m_r = str(r.get("h3_mv_rows", "N/A"))
        h3m_extra = f"+{r['h3_mv_superset_size']}" if r.get("h3_mv_superset_size", 0) > 0 else "0"
        h3c_r = str(r.get("h3_col_rows", "N/A"))
        h3c_extra = f"+{r['h3_col_superset_size']}" if r.get("h3_col_superset_size", 0) > 0 else "0"
        if r.get("h3_inline_rows") is None:
            h3i_extra = "N/A"
        if r.get("h3_mv_rows") is None:
            h3m_extra = "N/A"
        if r.get("h3_col_rows") is None:
            h3c_extra = "N/A"
        print(f"{r['query']:<15} {base_r:>10} {bbox_r:>10} {bbox_ok:>6} {h3i_r:>10} {h3i_extra:>8} {h3m_r:>10} {h3m_extra:>8} {h3c_r:>10} {h3c_extra:>8}")

    # EXPLAIN ANALYZE details
    print(f"\n{'='*100}")
    print("EXPLAIN ANALYZE DETAILS")
    print(f"{'='*100}")
    for r in all_results:
        for method, label in [("baseline", "Baseline"), ("bbox", "BBox"), ("h3_inline", "H3 Inline"), ("h3_mv", "H3 MV"), ("h3_col", "H3 Column")]:
            plan_key = f"{method}_plan"
            lines_key = f"{method}_plan_lines"
            if plan_key in r and lines_key in r:
                info = r[plan_key]
                print(f"\n--- {r['query']} / {label} ---")
                print(f"  ExecTime: {info['exec_time_ms']:.1f}ms  PlanTime: {info['plan_time_ms']:.1f}ms  "
                      f"Scan: {info['scan_type']}  Buffers(hit/read): {info['shared_hit']}/{info['shared_read']}")
                # Print first 25 lines of plan
                for line in r[lines_key][:25]:
                    print(f"  {line}")
                if len(r[lines_key]) > 25:
                    print(f"  ... ({len(r[lines_key]) - 25} more lines)")

    # Save JSON results
    output_path = Path("examples/benchmark/h3_cell_lookup_results.json")
    # Strip plan_lines for JSON (too verbose)
    json_results = []
    for r in all_results:
        jr = {k: v for k, v in r.items() if not k.endswith("_plan_lines") and not k.endswith("_enhanced_sql")}
        json_results.append(jr)
    with open(output_path, "w") as f:
        json.dump(json_results, f, indent=2, default=str)
    print(f"\nResults saved to {output_path}")

    # Cleanup
    h3_inline_handler.close()
    h3_mv_handler.close()
    h3_col_handler.close()
    bbox_handler.close()
    conn.close()


if __name__ == "__main__":
    run_benchmark()

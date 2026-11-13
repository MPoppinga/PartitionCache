#!/usr/bin/env python3
"""
Standalone Spatial Filter Analysis Script.

Performs detailed EXPLAIN ANALYZE comparison between original and enhanced
spatial queries, inspecting GiST index usage, search space reduction, and
spatial filter geometry quality.

Usage:
    python analyze_spatial_filter.py \
        --env examples/openstreetmap_poi/.env \
        --queries examples/openstreetmap_poi/testqueries_examples/spatial/ \
        --backend postgis_bbox

    # Compare both backends
    python analyze_spatial_filter.py \
        --env examples/openstreetmap_poi/.env \
        --queries examples/openstreetmap_poi/testqueries_examples/spatial/ \
        --backend both
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg
from dotenv import load_dotenv

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

# =============================================================================
# Configuration
# =============================================================================

SPATIAL_BACKENDS = {
    "postgis_h3": {
        "table_prefix_env": "PG_H3_CACHE_TABLE_PREFIX",
        "table_prefix_default": "osm_partitioncache_cache_h3",
        "label": "H3",
    },
    "postgis_bbox": {
        "table_prefix_env": "PG_BBOX_CACHE_TABLE_PREFIX",
        "table_prefix_default": "osm_partitioncache_cache_bbox",
        "label": "BBox",
    },
}


# =============================================================================
# Database Helpers
# =============================================================================


def get_connection(env_file: str) -> psycopg.Connection:
    """Create PostgreSQL connection from env file."""
    load_dotenv(env_file, override=True)
    conn = psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "app_user"),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=os.getenv("DB_NAME", "benchmark_db"),
    )
    conn.autocommit = True
    return conn


def execute_query(conn: psycopg.Connection, query: str) -> tuple[list, float]:
    """Execute query and return (rows, elapsed_ms)."""
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
        elapsed_ms = (time.perf_counter() - start) * 1000
    return rows, elapsed_ms


def explain_analyze(conn: psycopg.Connection, query: str) -> tuple[list[str], dict]:
    """Run EXPLAIN ANALYZE and parse results.

    For multi-statement queries, executes setup statements first, then
    EXPLAIN ANALYZE on the final SELECT.

    Returns (plan_lines, parsed_info).
    """
    with conn.cursor() as cur:
        statements = [s.strip() for s in query.split(";") if s.strip()]
        if len(statements) > 1:
            for stmt in statements[:-1]:
                cur.execute(stmt)
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {statements[-1]}"
        else:
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"

        cur.execute(explain_query)
        rows = cur.fetchall()

    plan_lines = [row[0] for row in rows]
    info = parse_explain_plan(plan_lines)
    return plan_lines, info


def parse_explain_plan(plan_lines: list[str]) -> dict:
    """Parse EXPLAIN ANALYZE output into structured metrics."""
    plan_text = "\n".join(plan_lines)
    plan_lower = plan_text.lower()

    gist_index_used = "index scan" in plan_lower and "gist" in plan_lower
    bitmap_gist = "bitmap index scan" in plan_lower and "gist" in plan_lower
    seq_scan_detected = "seq scan" in plan_lower

    if gist_index_used or bitmap_gist:
        scan_type = "GiST Index" if gist_index_used else "Bitmap GiST"
    elif "index scan" in plan_lower or "index only scan" in plan_lower:
        scan_type = "Index Scan"
    elif "bitmap" in plan_lower:
        scan_type = "Bitmap Scan"
    elif seq_scan_detected:
        scan_type = "Seq Scan"
    else:
        scan_type = "Other"

    # Extract estimated vs actual rows from root node
    estimated_rows = 0.0
    actual_rows = 0.0
    for line in plan_lines:
        m = re.search(r"rows=(\d+)\s+width=", line)
        if m and estimated_rows == 0:
            estimated_rows = float(m.group(1))
        m = re.search(r"actual time=[\d.]+\.\.[\d.]+\s+rows=(\d+)", line)
        if m and actual_rows == 0:
            actual_rows = float(m.group(1))

    # Execution time
    execution_time_ms = 0.0
    for line in plan_lines:
        m = re.search(r"Execution Time:\s+([\d.]+)\s+ms", line)
        if m:
            execution_time_ms = float(m.group(1))

    # Shared/read buffers
    total_shared_hit = 0
    total_shared_read = 0
    for line in plan_lines:
        m = re.search(r"Buffers:\s*shared hit=(\d+)", line)
        if m:
            total_shared_hit += int(m.group(1))
        m = re.search(r"shared read=(\d+)", line)
        if m:
            total_shared_read += int(m.group(1))

    return {
        "gist_index_used": gist_index_used or bitmap_gist,
        "seq_scan_detected": seq_scan_detected,
        "scan_type": scan_type,
        "estimated_rows": estimated_rows,
        "actual_rows": actual_rows,
        "execution_time_ms": execution_time_ms,
        "shared_hit_buffers": total_shared_hit,
        "shared_read_buffers": total_shared_read,
    }


def extract_max_st_dwithin_distance(query: str) -> float:
    """Extract the maximum ST_DWithin distance from a SQL query."""
    distances = []
    for match in re.finditer(r"ST_DWithin\s*\([^)]*,\s*(\d+(?:\.\d+)?)\s*\)", query, re.IGNORECASE):
        try:
            distances.append(float(match.group(1)))
        except ValueError:
            pass
    return max(distances) if distances else 0.0


# =============================================================================
# Cache and Query Helpers
# =============================================================================


def create_cache_handler(backend: str, partition_key: str):
    """Create a spatial cache handler."""
    return partitioncache.create_cache_helper(
        backend,
        partition_key,
        "geometry",
    )


def populate_cache(conn: psycopg.Connection, handler, query: str, partition_key: str, geometry_column: str = "geom") -> dict:
    """Populate spatial cache for a query. Returns stats."""
    pairs = generate_all_query_hash_pairs(
        query=query,
        partition_key=partition_key,
        min_component_size=1,
        keep_all_attributes=True,
        strip_select=True,
        auto_detect_partition_join=True,
        skip_partition_key_joins=True,
        follow_graph=True,
        geometry_column=geometry_column,
    )

    stats = {"fragments": len(pairs), "populated": 0, "hashes": []}
    for fragment, hash_val in pairs:
        stats["hashes"].append(hash_val)
        if handler.underlying_handler.exists(hash_val, partition_key=partition_key):
            stats["populated"] += 1
            continue

        try:
            success = handler.underlying_handler.set_entry_lazy(
                key=hash_val,
                query=fragment,
                query_text=fragment,
                partition_key=partition_key,
            )
            if success:
                stats["populated"] += 1
        except Exception as e:
            print(f"    Warning: Fragment failed: {e}")

    return stats


def get_enhanced_query(
    handler, query: str, partition_key: str, geometry_column: str = "geom",
    method: str = "IN_SUBQUERY", spatial_method: str = "SUBDIVIDE_TMP_TABLE",
) -> tuple[str, dict]:
    """Apply cache and get enhanced query using lazy API.

    Automatically extracts buffer_distance from ST_DWithin calls in the query
    as a fallback when compute_buffer_distance() returns 0.
    """
    # Extract buffer_distance from the query's ST_DWithin calls
    buffer_dist = extract_max_st_dwithin_distance(query)

    kwargs: dict = {
        "query": query,
        "cache_handler": handler.underlying_handler,
        "partition_key": partition_key,
        "method": method,
        "min_component_size": 1,
        "auto_detect_partition_join": True,
        "p0_alias": "p1",
        "skip_partition_key_joins": True,
        "follow_graph": True,
        "geometry_column": geometry_column,
        "spatial_method": spatial_method,
    }
    if buffer_dist > 0:
        kwargs["buffer_distance"] = buffer_dist

    enhanced, stats = partitioncache.apply_cache_lazy(**kwargs)
    return enhanced, stats


def query_cache_geometry_stats(conn: psycopg.Connection, table: str, hashes: list[str]) -> list[dict]:
    """Query geometry statistics from the spatial cache table."""
    stats = []
    for qh in hashes:
        try:
            rows, _ = execute_query(
                conn,
                f"SELECT ST_GeometryType(partition_keys), "
                f"ST_NumGeometries(partition_keys), "
                f"ST_Area(ST_Envelope(partition_keys)), "
                f"ST_NPoints(partition_keys), "
                f"partition_keys_count "
                f"FROM {table} WHERE query_hash = '{qh}'",
            )
            if rows and rows[0][0]:
                stats.append({
                    "query_hash": qh,
                    "geom_type": rows[0][0],
                    "num_geometries": rows[0][1],
                    "bbox_area": rows[0][2],
                    "num_points": rows[0][3],
                    "partition_keys_count": rows[0][4],
                })
        except Exception:
            pass
    return stats


def get_total_rows(conn: psycopg.Connection, table: str = "pois") -> int:
    """Get total row count from fact table."""
    rows, _ = execute_query(conn, f"SELECT COUNT(*) FROM {table}")
    return rows[0][0]


# =============================================================================
# Analysis
# =============================================================================


def analyze_query(
    conn: psycopg.Connection,
    query_name: str,
    query_sql: str,
    backend: str,
    partition_key: str,
    geometry_column: str,
    cache_table: str,
    total_fact_rows: int,
    spatial_method: str = "SUBDIVIDE_TMP_TABLE",
) -> dict:
    """Run full analysis for a single query with a given backend."""
    result: dict = {"query": query_name, "backend": backend}

    # 1. Run original query + EXPLAIN
    orig_rows, orig_ms = execute_query(conn, query_sql)
    _, orig_plan = explain_analyze(conn, query_sql)
    result["original"] = {
        "rows": len(orig_rows),
        "time_ms": orig_ms,
        "plan": orig_plan,
    }

    # 2. Create handler and populate cache
    handler = create_cache_handler(backend, partition_key)
    pop_stats = populate_cache(conn, handler, query_sql, partition_key, geometry_column)
    result["cache_population"] = pop_stats

    # 3. Query geometry stats from cache
    geom_stats = query_cache_geometry_stats(conn, cache_table, pop_stats["hashes"])
    result["geometry_stats"] = geom_stats

    # 4. Get enhanced query
    enhanced_sql, apply_stats = get_enhanced_query(handler, query_sql, partition_key, geometry_column, spatial_method=spatial_method)
    result["apply_stats"] = apply_stats

    # 5. Check spatial filter type in enhanced query
    enhanced_lower = enhanced_sql.lower()
    result["uses_st_intersects"] = "st_intersects" in enhanced_lower
    result["uses_st_dwithin"] = "st_dwithin" in enhanced_lower

    # 6. Run enhanced query + EXPLAIN
    try:
        enh_rows, enh_ms = execute_query(conn, enhanced_sql)
        _, enh_plan = explain_analyze(conn, enhanced_sql)
        result["enhanced"] = {
            "rows": len(enh_rows),
            "time_ms": enh_ms,
            "plan": enh_plan,
        }
    except Exception as e:
        result["enhanced"] = {"error": str(e)}

    # 7. Compute metrics
    if "error" not in result.get("enhanced", {}):
        orig_time = result["original"]["time_ms"]
        enh_time = result["enhanced"]["time_ms"]
        result["speedup"] = orig_time / enh_time if enh_time > 0 else float("inf")
        result["result_match"] = result["original"]["rows"] == result["enhanced"]["rows"]

        # Search space reduction (based on actual rows from EXPLAIN)
        enh_actual = result["enhanced"]["plan"]["actual_rows"]
        if total_fact_rows > 0 and enh_actual > 0:
            result["reduction_pct"] = round((1 - enh_actual / total_fact_rows) * 100, 1)

    # Cleanup handler
    try:
        handler.close()
    except Exception:
        pass

    return result


# =============================================================================
# Output
# =============================================================================


def print_comparison_table(results: list[dict]):
    """Print the main comparison table."""
    print(f"\n{'=' * 120}")
    print("Query Performance Comparison")
    print(f"{'=' * 120}")
    header = (
        f"{'Query':<12} {'Backend':<10} {'Original (ms)':>14} {'Enhanced (ms)':>14} "
        f"{'Speedup':>9} {'Scan Type':<12} {'GiST':>5} {'Rows':>12} {'Reduction':>10} {'Match':>6}"
    )
    print(header)
    print("-" * 120)

    for r in results:
        if "error" in r.get("enhanced", {}):
            print(f"{r['query']:<12} {r['backend']:<10} {r['original']['time_ms']:>13.1f} {'ERROR':>14}")
            continue

        enh = r["enhanced"]
        enh_plan = enh["plan"]
        gist = "Yes" if enh_plan["gist_index_used"] else "No"
        match = "Y" if r.get("result_match") else "N"
        rows_str = f"{int(enh_plan['actual_rows'])}/{r['original']['rows']}"
        reduction = f"{r.get('reduction_pct', 0):.1f}%" if "reduction_pct" in r else "n/a"

        print(
            f"{r['query']:<12} {r['backend']:<10} "
            f"{r['original']['time_ms']:>13.1f} "
            f"{enh['time_ms']:>13.1f} "
            f"{r.get('speedup', 0):>8.1f}x "
            f"{enh_plan['scan_type']:<12} "
            f"{gist:>5} "
            f"{rows_str:>12} "
            f"{reduction:>10} "
            f"{match:>6}"
        )


def print_geometry_stats_table(results: list[dict]):
    """Print spatial filter quality table."""
    print(f"\n{'=' * 110}")
    print("Spatial Filter Geometry Quality")
    print(f"{'=' * 110}")
    header = (
        f"{'Query':<12} {'Backend':<10} {'Fragments':>10} "
        f"{'Geom Type':<20} {'Num Geoms':>10} {'BBox Area (m^2)':>16} {'Points':>8}"
    )
    print(header)
    print("-" * 110)

    for r in results:
        for gs in r.get("geometry_stats", []):
            area_str = f"{gs['bbox_area']:>15,.0f}" if gs["bbox_area"] else f"{'n/a':>15}"
            print(
                f"{r['query']:<12} {r['backend']:<10} "
                f"{r['cache_population']['fragments']:>10} "
                f"{gs['geom_type']:<20} "
                f"{gs['num_geometries']:>10} "
                f"{area_str} "
                f"{gs['num_points']:>8}"
            )


def print_spatial_filter_check(results: list[dict]):
    """Print spatial filter type verification."""
    print(f"\n{'=' * 70}")
    print("Spatial Filter Type Verification (Double-Buffer Check)")
    print(f"{'=' * 70}")
    header = f"{'Query':<12} {'Backend':<10} {'ST_Intersects':>14} {'ST_DWithin':>12} {'Status':<15}"
    print(header)
    print("-" * 70)

    for r in results:
        intersects = "Yes" if r.get("uses_st_intersects") else "No"
        dwithin = "Yes" if r.get("uses_st_dwithin") else "No"
        # For BBox: expect ST_Intersects=Yes, ST_DWithin=No (no double-buffering)
        # For H3: expect ST_DWithin=Yes (H3 doesn't include buffer)
        if r["backend"] == "postgis_bbox":
            status = "OK" if r.get("uses_st_intersects") and not r.get("uses_st_dwithin") else "WARNING"
        else:
            status = "OK"  # H3 uses DWithin, which is expected
        print(f"{r['query']:<12} {r['backend']:<10} {intersects:>14} {dwithin:>12} {status:<15}")


# =============================================================================
# Main
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Spatial Filter Analysis: EXPLAIN ANALYZE and GiST diagnostics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--env", type=str, required=True, help="Path to .env file (e.g., examples/openstreetmap_poi/.env)")
    parser.add_argument("--queries", type=str, required=True, help="Directory containing spatial query SQL files")
    parser.add_argument(
        "--backend",
        type=str,
        default="both",
        choices=["postgis_h3", "postgis_bbox", "both"],
        help="Spatial backend to analyze (default: both)",
    )
    parser.add_argument("--partition-key", type=str, default=None, help="Partition key name (auto-generated if not set)")
    parser.add_argument("--geometry-column", type=str, default="geom", help="Geometry column name (default: geom)")
    parser.add_argument("--fact-table", type=str, default="pois", help="Fact table name (default: pois)")
    parser.add_argument(
        "--spatial-method",
        type=str,
        default="SUBDIVIDE_TMP_TABLE",
        choices=["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE"],
        help="Spatial filter method (default: SUBDIVIDE_TMP_TABLE)",
    )
    parser.add_argument("--output", type=str, default=None, help="Save results to JSON file")

    args = parser.parse_args()

    # Resolve paths
    project_root = Path(__file__).parent.parent.parent
    env_path = Path(args.env)
    if not env_path.is_absolute():
        env_path = project_root / env_path

    query_dir = Path(args.queries)
    if not query_dir.is_absolute():
        query_dir = project_root / query_dir

    if not env_path.exists():
        print(f"Error: .env file not found: {env_path}")
        sys.exit(1)
    if not query_dir.exists():
        print(f"Error: Query directory not found: {query_dir}")
        sys.exit(1)

    # Load environment
    load_dotenv(str(env_path), override=True)

    # Determine backends to test
    backends = ["postgis_h3", "postgis_bbox"] if args.backend == "both" else [args.backend]

    # Load queries
    query_files = sorted(query_dir.glob("q*.sql"))
    if not query_files:
        print(f"Error: No query files found in {query_dir}")
        sys.exit(1)

    queries = {}
    for qf in query_files:
        name = qf.stem  # e.g., "q1"
        queries[name] = qf.read_text().strip()

    spatial_method = args.spatial_method

    print("Spatial Filter Analysis")
    print(f"  Backends: {', '.join(backends)}")
    print(f"  Queries: {', '.join(queries.keys())}")
    print(f"  Geometry column: {args.geometry_column}")
    print(f"  Fact table: {args.fact_table}")
    print(f"  Spatial method: {spatial_method}")

    # Connect
    conn = get_connection(str(env_path))

    try:
        # Get total rows
        total_rows = get_total_rows(conn, args.fact_table)
        print(f"  Total fact table rows: {total_rows:,}")

        all_results = []

        for backend in backends:
            backend_cfg = SPATIAL_BACKENDS[backend]
            table_prefix = os.getenv(backend_cfg["table_prefix_env"], backend_cfg["table_prefix_default"])

            pk_name = args.partition_key or f"spatial_{backend_cfg['label'].lower()}"
            cache_table = f"{table_prefix}_cache_{pk_name}"

            print(f"\n{'=' * 80}")
            print(f"Backend: {backend} (pk={pk_name}, table={cache_table})")
            print(f"{'=' * 80}")

            for q_name, q_sql in queries.items():
                print(f"\n--- {q_name} ---")
                try:
                    result = analyze_query(
                        conn=conn,
                        query_name=q_name,
                        query_sql=q_sql,
                        backend=backend,
                        partition_key=pk_name,
                        geometry_column=args.geometry_column,
                        cache_table=cache_table,
                        total_fact_rows=total_rows,
                        spatial_method=spatial_method,
                    )
                    all_results.append(result)

                    # Print per-query summary
                    if "error" not in result.get("enhanced", {}):
                        enh = result["enhanced"]
                        print(f"  Original: {result['original']['rows']} rows in {result['original']['time_ms']:.1f}ms")
                        print(f"  Enhanced: {enh['rows']} rows in {enh['time_ms']:.1f}ms "
                              f"(speedup: {result.get('speedup', 0):.1f}x, match: {result.get('result_match')})")
                        print(f"  Scan: {enh['plan']['scan_type']}, "
                              f"GiST: {'Yes' if enh['plan']['gist_index_used'] else 'No'}")
                    else:
                        print(f"  Enhanced: ERROR - {result['enhanced']['error']}")
                except Exception as e:
                    print(f"  Error: {e}")
                    import traceback
                    traceback.print_exc()

        # Summary tables
        if all_results:
            print_comparison_table(all_results)
            print_geometry_stats_table(all_results)
            print_spatial_filter_check(all_results)

            # Overall summary
            print(f"\n{'=' * 80}")
            print("Overall Summary")
            for backend in backends:
                br = [r for r in all_results if r["backend"] == backend and "error" not in r.get("enhanced", {})]
                if br:
                    avg_speedup = sum(r.get("speedup", 0) for r in br) / len(br)
                    all_match = all(r.get("result_match", False) for r in br)
                    gist_count = sum(1 for r in br if r["enhanced"]["plan"]["gist_index_used"])
                    print(f"  {backend}: avg speedup={avg_speedup:.1f}x, "
                          f"all correct={all_match}, "
                          f"GiST used={gist_count}/{len(br)}")

        # Save results
        if args.output:
            output_data = {
                "total_fact_rows": total_rows,
                "backends": backends,
                "queries": list(queries.keys()),
                "results": all_results,
            }
            with open(args.output, "w") as f:
                json.dump(output_data, f, indent=2, default=str)
            print(f"\nResults saved to {args.output}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

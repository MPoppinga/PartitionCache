#!/usr/bin/env python3
"""
Unified Benchmark Runner for PartitionCache.

Config-driven benchmark platform supporting multiple workloads (SSB, TPC-H,
NYC Taxi, OSM POI) with a single script. Each workload is defined by a YAML
config file specifying tables, partition keys, query mappings, and cache setup.

Usage:
    python run_benchmark.py --config config/ssb.yaml --db-backend duckdb --mode all
    python run_benchmark.py --config config/tpch.yaml --db-backend duckdb --mode flight 3
    python run_benchmark.py --config config/nyc_taxi.yaml --mode api-comparison
    python run_benchmark.py --config config/osm_poi.yaml --mode all
"""

import argparse
import copy
import json
import os
import re
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import yaml

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

# =============================================================================
# Configuration
# =============================================================================


@dataclass
class BenchmarkConfig:
    """Typed access to YAML benchmark configuration."""

    name: str
    fact_table: str
    p0_alias: str
    env_file: str
    query_dir: str
    data_setup: dict
    executors: dict
    partition_keys: dict
    query_partition_keys: dict[str, list[str]]
    query_flights: dict[int, list[str]]
    hierarchy_labels: dict[str, str]
    spatial_queries: list[str]
    query_processing: dict
    fragment_filter: dict
    cache: dict
    backends: dict[str, list[str]]
    fact_stats: dict
    modes: dict
    query_subdirectory_pattern: str = ""

    # Computed fields
    all_queries: list[str] = field(default_factory=list)

    def __post_init__(self):
        # Normalize query_flights keys to int
        self.query_flights = {int(k): v for k, v in self.query_flights.items()}
        self.all_queries = [q for f in sorted(self.query_flights) for q in self.query_flights[f]]

    @classmethod
    def from_yaml(cls, path: str) -> "BenchmarkConfig":
        with open(path) as f:
            data = yaml.safe_load(f)
        known_fields = {f.name for f in cls.__dataclass_fields__.values() if f.name != "all_queries"}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


# =============================================================================
# Executors
# =============================================================================


class DuckDBExecutor:
    """Execute queries against a DuckDB database."""

    def __init__(self, db_path: str):
        import duckdb

        self.conn = duckdb.connect(str(db_path), read_only=False)
        self.conn.execute("SET max_expression_depth TO 10000")

    def execute(self, query: str) -> tuple[list, float]:
        """Execute query and return (rows, elapsed_seconds)."""
        start = time.perf_counter()
        # Handle multi-statement queries (CREATE TEMP TABLE; ...; SELECT)
        statements = [s.strip() for s in query.split(";") if s.strip()]
        if len(statements) > 1:
            for stmt in statements[:-1]:
                self.conn.execute(stmt)
            result = self.conn.execute(statements[-1]).fetchall()
        else:
            result = self.conn.execute(query).fetchall()
        elapsed = time.perf_counter() - start
        return result, elapsed

    def execute_fragment(self, fragment: str) -> set:
        """Execute a cache fragment query and return set of partition key values."""
        rows = self.conn.execute(fragment).fetchall()
        return {row[0] for row in rows}

    def close(self):
        self.conn.close()


class PostgreSQLExecutor:
    """Execute queries against a PostgreSQL database with multi-statement support."""

    def __init__(self, env_file: str | None = None):
        import psycopg
        from dotenv import load_dotenv

        if env_file:
            load_dotenv(env_file, override=True)

        self.conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER", "app_user"),
            password=os.getenv("DB_PASSWORD", ""),
            dbname=os.getenv("DB_NAME", "benchmark_db"),
        )
        self.conn.autocommit = True

    def execute(self, query: str) -> tuple[list, float]:
        """Execute query and return (rows, elapsed_seconds).

        Handles multi-statement queries (e.g. from TMP_TABLE_IN/JOIN methods)
        by splitting on semicolons, executing setup statements first, then
        fetching results from the final SELECT.
        """
        with self.conn.cursor() as cur:
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

    def execute_fragment(self, fragment: str) -> set:
        """Execute a cache fragment query and return set of partition key values."""
        with self.conn.cursor() as cur:
            cur.execute(fragment)
            rows = cur.fetchall()
        return {row[0] for row in rows}

    def explain_analyze(self, query: str) -> tuple[list[str], float]:
        """Run EXPLAIN ANALYZE and return (plan_lines, actual_time_ms).

        For multi-statement queries (TMP_TABLE_IN/JOIN), executes setup statements
        first, then runs EXPLAIN ANALYZE on the final SELECT.
        """
        with self.conn.cursor() as cur:
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

        # Extract actual total time from the root node (last "Execution Time:" line)
        actual_time = 0.0
        for line in plan_lines:
            m = re.search(r"Execution Time:\s+([\d.]+)\s+ms", line)
            if m:
                actual_time = float(m.group(1))

        return plan_lines, actual_time

    def close(self):
        self.conn.close()


# =============================================================================
# Cache Manager
# =============================================================================


class CacheManager:
    """Manage partition cache handlers for multiple partition keys, driven by config."""

    def __init__(self, cache_type: str, config: BenchmarkConfig, scale_factor: float = 0.01, db_path: str | None = None, max_pk_value: int = 0, pk_max_values: dict | None = None):
        self.cache_type = cache_type
        self.config = config
        self.handlers: dict = {}
        self.db_path = db_path
        self.scale_factor = scale_factor
        self.max_pk_value = max_pk_value
        self.pk_max_values = pk_max_values or {}

    def get_handler(self, partition_key: str):
        """Get or create a cache handler for a partition key."""
        if partition_key not in self.handlers:
            pk_config = self.config.partition_keys.get(partition_key, {})
            datatype = pk_config.get("datatype", "integer")
            cache_cfg = self.config.cache

            # Configure environment for bitsize (per partition key if max values available)
            bitsize = self._compute_bitsize(cache_cfg, partition_key)
            prefix = cache_cfg.get("table_prefix", "benchmark_cache")

            if self.cache_type == "duckdb_bit":
                cache_db = str(self.db_path).replace(".duckdb", "_cache.duckdb") if self.db_path else f"{prefix}.duckdb"
                os.environ["DUCKDB_BIT_PATH"] = cache_db
                os.environ["DUCKDB_BIT_BITSIZE"] = str(bitsize)
            elif self.cache_type == "postgresql_roaringbit":
                os.environ.setdefault("PG_ROARINGBIT_CACHE_TABLE_PREFIX", prefix)
            elif self.cache_type == "postgresql_bit":
                os.environ.setdefault("PG_BIT_CACHE_TABLE_PREFIX", prefix)
                os.environ.setdefault("PG_BIT_CACHE_BITSIZE", str(bitsize))
            elif self.cache_type == "postgresql_array":
                os.environ.setdefault("PG_ARRAY_CACHE_TABLE_PREFIX", prefix)
            elif self.cache_type == "rocksdict_roaringbit":
                cache_dir = f"{prefix}_rocksdict"
                os.environ.setdefault("ROCKSDICT_ROARINGBIT_PATH", cache_dir)

            # Spatial partition keys use the spatial cache backend
            if datatype == "geometry":
                spatial_backend = pk_config.get("spatial_backend", "rocksdict_h3_grouped")
                geometry_column = pk_config.get("geometry_column", "geom")
                srid = str(pk_config.get("srid", 4326))

                if spatial_backend == "postgis_bbox":
                    os.environ.setdefault("PG_BBOX_CACHE_TABLE_PREFIX", prefix + "_bbox")
                    os.environ.setdefault("PG_BBOX_GEOMETRY_COLUMN", geometry_column)
                    os.environ.setdefault("PG_BBOX_SRID", srid)
                    cell_size = str(pk_config.get("bbox_cell_size", 0.01))
                    os.environ.setdefault("PG_BBOX_CELL_SIZE", cell_size)
                elif spatial_backend == "rocksdict_h3_grouped":
                    cache_dir = f"{prefix}_rocksdict_h3"
                    os.environ.setdefault("ROCKSDICT_H3_GROUPED_PATH", cache_dir)
                    os.environ.setdefault("PG_H3_SRID", srid)
                    resolution = str(pk_config.get("h3_resolution", 9))
                    os.environ.setdefault("PG_H3_RESOLUTION", resolution)

                self.handlers[partition_key] = partitioncache.create_cache_helper(
                    spatial_backend,
                    partition_key,
                    datatype,
                )
            else:
                self.handlers[partition_key] = partitioncache.create_cache_helper(
                    self.cache_type,
                    partition_key,
                    datatype,
                )
        return self.handlers[partition_key]

    def _compute_bitsize(self, cache_cfg: dict, partition_key: str = "") -> int:
        strategy = cache_cfg.get("bitsize_strategy", "fixed")
        min_bitsize = cache_cfg.get("min_bitsize", 200000)

        if strategy == "fixed":
            return cache_cfg.get("fixed_bitsize", 20000000)
        elif strategy == "scale_factor":
            multiplier = cache_cfg.get("scale_factor_multiplier", 1600000)
            return max(int(self.scale_factor * multiplier), min_bitsize)
        elif strategy == "auto":
            # Per-partition-key bitsize from actual max values
            pk_max = self.pk_max_values.get(partition_key, 0)
            if pk_max > 0:
                return max(pk_max + 1000, min_bitsize)
            return min_bitsize
        elif strategy == "data_driven":
            return max(self.max_pk_value + 1000, min_bitsize)
        return 20000000

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


# =============================================================================
# Query Loading
# =============================================================================


def load_query(config: BenchmarkConfig, query_name: str) -> str:
    """Load a SQL query from file, resolving the path based on config."""
    project_root = Path(__file__).parent.parent.parent
    query_dir = project_root / config.query_dir

    if config.query_subdirectory_pattern:
        # OSM POI pattern: queries organized by partition key subdirectory
        # query_name format: "zipcode_q1" -> subdirectory="zipcode", file="q1.sql"
        parts = query_name.rsplit("_", 1)
        if len(parts) == 2:
            subdir, qfile = parts
            path = query_dir / subdir / f"{qfile}.sql"
        else:
            path = query_dir / f"{query_name}.sql"
    else:
        # Standard pattern: queries/original/{query_name}.sql
        path = query_dir / "original" / f"{query_name}.sql"

    with open(path) as f:
        return f.read().strip()


def load_adapted_query(config: BenchmarkConfig, query_name: str) -> str | None:
    """Load an adapted query for cache population (SSB/TPC-H pattern).

    Returns None if adapted queries are not configured or the file doesn't exist.
    """
    qp = config.query_processing
    if not qp.get("use_adapted_queries", False):
        return None

    project_root = Path(__file__).parent.parent.parent
    query_dir = project_root / config.query_dir
    subdir = qp.get("adapted_subdirectory", "adapted")
    path = query_dir / subdir / f"{query_name}.sql"

    if not path.exists():
        return None
    with open(path) as f:
        return f.read().strip()


# =============================================================================
# Fragment Filter
# =============================================================================


def _extract_max_st_dwithin_distance(query: str) -> float:
    """Extract the maximum ST_DWithin distance from a SQL query.

    Scans for ST_DWithin(..., ..., <distance>) calls and returns the largest
    distance value found. Used as fallback buffer_distance for spatial queries
    where compute_buffer_distance() returns 0 (e.g., single-table queries
    with a fixed point rather than inter-table distance constraints).
    """
    distances = []
    for match in re.finditer(r"ST_DWithin\s*\([^)]*,\s*(\d+(?:\.\d+)?)\s*\)", query, re.IGNORECASE):
        try:
            distances.append(float(match.group(1)))
        except ValueError:
            pass
    return max(distances) if distances else 0.0


def create_fragment_filter(config: BenchmarkConfig):
    """Create a fragment filter function based on config."""
    require = config.fragment_filter.get("require_in_from", [])
    exclude = config.fragment_filter.get("exclude_from_from", [])
    if not require and not exclude:
        return None

    def filter_fn(fragment_sql: str) -> bool:
        m = re.search(r"FROM\s+(.*?)\s+WHERE", fragment_sql, re.IGNORECASE | re.DOTALL)
        if not m:
            return False
        from_clause = m.group(1)
        return all(t in from_clause for t in require) and not any(t in from_clause for t in exclude)

    return filter_fn


# =============================================================================
# Table Validation
# =============================================================================


def validate_tables(executor, config: BenchmarkConfig) -> bool:
    """Check that all required tables exist. Print helpful error on failure."""
    required = config.data_setup.get("required_tables", [])
    for table in required:
        try:
            executor.execute(f"SELECT 1 FROM {table} LIMIT 0")
        except Exception:
            print(f"Error: Table '{table}' not found.")
            example_cmd = config.data_setup.get("example_command", "")
            if example_cmd:
                print(f"Generate data first:\n  {example_cmd}")
            return False
    return True


# =============================================================================
# Fact Table Stats
# =============================================================================


def compute_fact_table_stats(executor, config: BenchmarkConfig) -> dict:
    """Compute fact table statistics based on config strategy."""
    stats_cfg = config.fact_stats
    strategy = stats_cfg.get("strategy", "count_distinct")
    table = stats_cfg.get("table", config.fact_table)
    stats = {}

    if strategy == "count_with_max":
        count_col = stats_cfg.get("count_column", "*")
        max_col = stats_cfg.get("max_column", None)
        if max_col:
            rows, _ = executor.execute(f"SELECT COUNT({count_col}), MAX({max_col}) FROM {table}")
            total_count = rows[0][0]
            max_val = rows[0][1]
            # Store max value for bitsize computation
            executor.max_pk_value = max_val
            # Use the first partition key name as the stats key
            pk_names = list(config.partition_keys.keys())
            if pk_names:
                stats[pk_names[0]] = total_count
            print(f"\nFact table ({table}) statistics:")
            print(f"  {pk_names[0] if pk_names else 'pk'}: {total_count:,} rows, max value: {max_val:,}")
        else:
            rows, _ = executor.execute(f"SELECT COUNT({count_col}) FROM {table}")
            total_count = rows[0][0]
            pk_names = list(config.partition_keys.keys())
            if pk_names:
                stats[pk_names[0]] = total_count
    else:
        # count_distinct: compute COUNT(DISTINCT pk) for each partition key
        print(f"\nFact table ({table}) partition key cardinalities:")
        for pk in config.partition_keys:
            pk_cfg = config.partition_keys[pk]
            if pk_cfg.get("datatype") == "geometry":
                continue  # Skip geometry partition keys for count_distinct
            rows, _ = executor.execute(f"SELECT COUNT(DISTINCT {pk}) FROM {table}")
            stats[pk] = rows[0][0]
            print(f"  {pk}: {stats[pk]:,} distinct values")

    return stats


def compute_pk_max_values(executor, config: BenchmarkConfig) -> dict:
    """Compute MAX(pk) for each integer partition key from the fact table."""
    table = config.fact_stats.get("table", config.fact_table)
    pk_max = {}
    for pk, pk_cfg in config.partition_keys.items():
        if pk_cfg.get("datatype") in ("geometry", "text"):
            continue
        try:
            rows, _ = executor.execute(f"SELECT MAX({pk}) FROM {table}")
            if rows and rows[0][0] is not None:
                pk_max[pk] = int(rows[0][0])
        except Exception:
            pass
    return pk_max


# =============================================================================
# EXPLAIN ANALYZE Helpers
# =============================================================================


def parse_explain_plan(plan_lines: list[str]) -> dict:
    """Parse EXPLAIN ANALYZE output and extract key metrics.

    Returns dict with:
        gist_index_used: bool
        seq_scan_detected: bool
        scan_type: str (summary of primary scan method)
        estimated_rows: float (from planner)
        actual_rows: float (from execution)
        execution_time_ms: float
        plan_text: str (full plan)
    """
    plan_text = "\n".join(plan_lines)
    plan_lower = plan_text.lower()

    gist_index_used = "index scan" in plan_lower and "gist" in plan_lower
    bitmap_gist = "bitmap index scan" in plan_lower and "gist" in plan_lower
    seq_scan_detected = "seq scan" in plan_lower

    # Determine primary scan type
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

    # Extract estimated vs actual rows from root node (first line with rows= pattern)
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

    return {
        "gist_index_used": gist_index_used or bitmap_gist,
        "seq_scan_detected": seq_scan_detected,
        "scan_type": scan_type,
        "estimated_rows": estimated_rows,
        "actual_rows": actual_rows,
        "execution_time_ms": execution_time_ms,
        "plan_text": plan_text,
    }


def print_explain_summary(label: str, plan_info: dict):
    """Print a compact EXPLAIN ANALYZE summary."""
    gist = "Yes" if plan_info["gist_index_used"] else "No"
    seq = "Yes" if plan_info["seq_scan_detected"] else "No"
    print(f"  [{label}] Scan: {plan_info['scan_type']}, GiST: {gist}, SeqScan: {seq}, "
          f"Est. rows: {plan_info['estimated_rows']:.0f}, "
          f"Actual rows: {plan_info['actual_rows']:.0f}, "
          f"Exec: {plan_info['execution_time_ms']:.1f}ms")


def query_spatial_cache_stats(executor, config: BenchmarkConfig, partition_key: str, query_hashes: list[str]) -> list[dict]:
    """Query geometry statistics from the spatial cache table for given hashes.

    Returns list of dicts with geometry type, num geometries, bbox area, and count.
    """
    pk_config = config.partition_keys.get(partition_key, {})
    spatial_backend = pk_config.get("spatial_backend", "")
    if spatial_backend not in ("rocksdict_h3_grouped", "postgis_bbox"):
        return []

    prefix = config.cache.get("table_prefix", "benchmark_cache")
    if spatial_backend == "rocksdict_h3_grouped":
        prefix += "_h3"
    elif spatial_backend == "postgis_bbox":
        prefix += "_bbox"

    table = f"{prefix}_cache_{partition_key}"

    stats = []
    for qh in query_hashes:
        try:
            rows, _ = executor.execute(
                f"SELECT ST_GeometryType(partition_keys), "
                f"ST_NumGeometries(partition_keys), "
                f"ST_Area(ST_Envelope(partition_keys)), "
                f"ST_NPoints(partition_keys), "
                f"partition_keys_count "
                f"FROM {table} WHERE query_hash = '{qh}'"
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


# =============================================================================
# Benchmark Core Functions
# =============================================================================


def _populate_rocksdict_h3_grouped(executor, handler, fragment: str, hash_val: str, partition_key: str) -> None:
    """Execute a spatial fragment and store results as grouped H3 match sets.

    Executes the fragment SQL, converts each geometry column in each row to an H3 cell
    via the handler's geom_to_h3_cell(), and stores as list[frozenset[int]].
    """
    from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler

    h3_handler: RocksDictH3GroupedCacheHandler = handler.underlying_handler  # type: ignore[assignment]

    with executor.conn.cursor() as cur:
        cur.execute(fragment)
        rows = cur.fetchall()

    if not rows:
        h3_handler.set_null(hash_val, partition_key)
        return

    match_groups: list[frozenset[int]] = []
    seen: set[frozenset[int]] = set()

    for row in rows:
        cells: set[int] = set()
        for value in row:
            if value is None:
                continue
            # Convert geometry (WKB bytes) to H3 cell ID
            cell_id = h3_handler.geom_to_h3_cell(value)
            if cell_id is not None:
                cells.add(cell_id)
        if cells:
            group = frozenset(cells)
            if group not in seen:
                seen.add(group)
                match_groups.append(group)

    if match_groups:
        h3_handler.set_cache(hash_val, match_groups, partition_key)
        h3_handler.set_query(hash_val, fragment, partition_key)
    else:
        h3_handler.set_null(hash_val, partition_key)


def populate_cache_for_query(executor, cache_manager: CacheManager, query: str, partition_keys: list[str], config: BenchmarkConfig) -> dict:
    """Populate cache for a query across all its partition keys.

    Returns dict with per-partition-key stats including population_time.
    """
    stats = {}
    qp = config.query_processing
    fragment_filter = create_fragment_filter(config)

    for pk in partition_keys:
        pk_stats = {"fragments_generated": 0, "fragments_executed": 0, "partition_keys_cached": 0}
        handler = cache_manager.get_handler(pk)
        pk_config = config.partition_keys.get(pk, {})

        pk_start = time.perf_counter()

        # Build kwargs for generate_all_query_hash_pairs
        gen_kwargs: dict = {
            "query": query,
            "partition_key": pk,
            "min_component_size": 1,
            "keep_all_attributes": qp.get("keep_all_attributes", False),
            "strip_select": qp.get("strip_select", True),
            "auto_detect_partition_join": qp.get("auto_detect_partition_join", False),
            "skip_partition_key_joins": qp.get("skip_partition_key_joins", True),
            "follow_graph": qp.get("follow_graph", False),
        }

        # Spatial partition keys: must skip partition key joins and add geometry column
        if pk_config.get("datatype") == "geometry":
            gen_kwargs["geometry_column"] = pk_config.get("geometry_column", "geom")
            gen_kwargs["skip_partition_key_joins"] = True

        pairs = generate_all_query_hash_pairs(**gen_kwargs)

        # Apply fragment filter if configured
        if fragment_filter:
            pairs = [(q, h) for q, h in pairs if fragment_filter(q)]

        pk_stats["fragments_generated"] = len(pairs)

        # Spatial partition keys use lazy insertion (server-side H3 conversion)
        is_spatial_pk = pk_config.get("datatype") == "geometry"
        spatial_backend = pk_config.get("spatial_backend", "rocksdict_h3_grouped") if is_spatial_pk else None

        for fragment, hash_val in pairs:
            if handler.underlying_handler.exists(hash_val, partition_key=pk):
                continue

            try:
                if is_spatial_pk and spatial_backend == "rocksdict_h3_grouped":
                    # RocksDict H3 Grouped: execute fragment, convert geometry columns
                    # to H3 cells, store as grouped match sets
                    _populate_rocksdict_h3_grouped(executor, handler, fragment, hash_val, pk)
                    pk_stats["fragments_executed"] += 1
                elif is_spatial_pk:
                    # PostGIS spatial: use set_entry_lazy which wraps fragment with H3 cell conversion
                    # server-side, avoiding moving raw geometry bytes through Python
                    success = handler.underlying_handler.set_entry_lazy(
                        key=hash_val,
                        query=fragment,
                        query_text=fragment,
                        partition_key=pk,
                    )
                    if success:
                        pk_stats["fragments_executed"] += 1
                else:
                    result_set = executor.execute_fragment(fragment)
                    pk_stats["fragments_executed"] += 1
                    pk_stats["partition_keys_cached"] = max(pk_stats["partition_keys_cached"], len(result_set))

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


def apply_cache_to_query(
    cache_manager: CacheManager,
    query: str,
    partition_keys: list[str],
    config: BenchmarkConfig,
    fact_stats: dict | None = None,
    method: str = "TMP_TABLE_IN",
    cache_query: str | None = None,
    spatial_method: str = "SUBDIVIDE_TMP_TABLE",
) -> tuple[str, dict, float]:
    """Apply cached restrictions for all partition keys to the query.

    Args:
        cache_query: Query to use for cache hash matching (adapted query for SSB/TPC-H).
                     If None, uses the original query.

    Returns (enhanced_query, per_pk_stats, cache_apply_time).
    """
    qp = config.query_processing
    apply_start = time.perf_counter()
    # For TMP_TABLE methods with multiple PKs, we need to track setup SQL separately
    # because extend_query_with_partition_keys returns setup_sql + SELECT concatenated,
    # and chaining that through another call breaks sqlglot parsing.
    setup_statements: list[str] = []
    select_query = query
    pk_stats = {}
    lookup_query = cache_query if cache_query else query

    for pk in partition_keys:
        handler = cache_manager.get_handler(pk)
        pk_config = config.partition_keys.get(pk, {})

        get_kwargs: dict = {
            "query": lookup_query,
            "cache_handler": handler.underlying_handler,
            "partition_key": pk,
            "min_component_size": 1,
            "auto_detect_partition_join": qp.get("auto_detect_partition_join", False),
        }
        if qp.get("skip_partition_key_joins"):
            get_kwargs["skip_partition_key_joins"] = True
        if not qp.get("follow_graph", True):
            get_kwargs["follow_graph"] = False

        # Spatial partition keys: must skip partition key joins and add geometry column
        if pk_config.get("datatype") == "geometry":
            get_kwargs["geometry_column"] = pk_config.get("geometry_column", "geom")
            get_kwargs["skip_partition_key_joins"] = True

        cached_keys, num_generated, num_hits = partitioncache.get_partition_keys(**get_kwargs)

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

        if pk_config.get("datatype") == "geometry":
            # Spatial partition keys: use apply_cache() which handles spatial filter (ST_DWithin)
            # instead of extend_query_with_partition_keys which would inject p1.<partition_key> IN (...)
            try:
                geometry_col = pk_config.get("geometry_column", "geom")
                # Extract max ST_DWithin distance as fallback buffer_distance for queries
                # where compute_buffer_distance returns 0 (e.g., single-table with fixed point)
                buffer_dist = _extract_max_st_dwithin_distance(select_query)
                result, spatial_stats = partitioncache.apply_cache(
                    query=select_query,
                    cache_handler=handler.underlying_handler,
                    partition_key=pk,
                    method=method,
                    p0_alias=config.p0_alias,
                    min_component_size=1,
                    auto_detect_partition_join=qp.get("auto_detect_partition_join", False),
                    follow_graph=qp.get("follow_graph", True),
                    skip_partition_key_joins=True,
                    geometry_column=geometry_col,
                    buffer_distance=buffer_dist,
                    spatial_method=spatial_method,
                )
                if spatial_stats.get("enhanced"):
                    # For TMP_TABLE methods, separate setup SQL from the SELECT query
                    if method in ("TMP_TABLE_IN", "TMP_TABLE_JOIN"):
                        parts = [s.strip() for s in result.split(";") if s.strip()]
                        select_query = parts[-1]
                        setup_statements.extend(parts[:-1])
                    else:
                        select_query = result
            except Exception as e:
                print(f"    Warning: Could not apply spatial cache for {pk}: {e}")
                import traceback
                traceback.print_exc()
        elif cached_keys:
            try:
                result = partitioncache.extend_query_with_partition_keys(
                    select_query,
                    cached_keys,
                    partition_key=pk,
                    method=method,
                    p0_alias=config.p0_alias,
                )
                # For TMP_TABLE methods, separate setup SQL from the SELECT query
                if method in ("TMP_TABLE_IN", "TMP_TABLE_JOIN"):
                    # Split off setup statements (CREATE/INSERT/INDEX/ANALYZE) from the final SELECT
                    parts = [s.strip() for s in result.split(";") if s.strip()]
                    # Last part is the SELECT, everything before is setup
                    select_query = parts[-1]
                    setup_statements.extend(parts[:-1])
                else:
                    select_query = result
            except Exception as e:
                print(f"    Warning: Could not extend query with {pk}: {e}")

    # Reassemble: setup statements + final SELECT
    if setup_statements:
        enhanced = ";".join(setup_statements) + ";" + select_query
    else:
        enhanced = select_query

    cache_apply_time = time.perf_counter() - apply_start
    return enhanced, pk_stats, cache_apply_time


def apply_cache_to_query_lazy(
    cache_manager: CacheManager,
    query: str,
    partition_keys: list[str],
    config: BenchmarkConfig,
    method: str = "IN_SUBQUERY",
    cache_query: str | None = None,
    spatial_method: str = "SUBDIVIDE_TMP_TABLE",
) -> tuple[str, dict, float]:
    """Apply cached restrictions using the lazy API (database-side intersection).

    Args:
        cache_query: Query to use for cache hash matching (adapted query for SSB/TPC-H).
                     If None, uses the original query.

    Returns (enhanced_query, per_pk_stats, cache_apply_time).
    """
    qp = config.query_processing
    apply_start = time.perf_counter()
    enhanced = query
    pk_stats = {}

    # When using adapted queries, lazy API can't be used directly because
    # apply_cache_lazy() uses the same query for hash generation and extension.
    # Fall back to non-lazy path with adapted query for hash matching.
    if cache_query and cache_query != query:
        return apply_cache_to_query(cache_manager, query, partition_keys, config, method=method, cache_query=cache_query, spatial_method=spatial_method)

    for pk in partition_keys:
        handler = cache_manager.get_handler(pk)
        pk_config = config.partition_keys.get(pk, {})

        lazy_kwargs: dict = {
            "query": enhanced,
            "cache_handler": handler.underlying_handler,
            "partition_key": pk,
            "method": method,
            "min_component_size": 1,
            "auto_detect_partition_join": qp.get("auto_detect_partition_join", False),
            "p0_alias": config.p0_alias,
        }
        if qp.get("skip_partition_key_joins"):
            lazy_kwargs["skip_partition_key_joins"] = True
        if not qp.get("follow_graph", True):
            lazy_kwargs["follow_graph"] = False
        if pk_config.get("datatype") == "geometry":
            lazy_kwargs["geometry_column"] = pk_config.get("geometry_column", "geom")
            lazy_kwargs["skip_partition_key_joins"] = True

        lazy_kwargs["spatial_method"] = spatial_method
        enhanced, stats = partitioncache.apply_cache_lazy(**lazy_kwargs)

        entry = {
            "variants_checked": stats.get("generated_variants", 0),
            "cache_hits": stats.get("cache_hits", 0),
        }
        pk_stats[pk] = entry

    cache_apply_time = time.perf_counter() - apply_start
    return enhanced, pk_stats, cache_apply_time


def run_single_query(
    executor,
    cache_manager: CacheManager,
    query_name: str,
    config: BenchmarkConfig,
    repeat: int = 1,
    fact_stats: dict | None = None,
    method: str = "TMP_TABLE_IN",
    api: str = "non-lazy",
    explain: bool = False,
    spatial_method: str = "SUBDIVIDE_TMP_TABLE",
) -> dict:
    """Run a single query through the full benchmark pipeline."""
    original_query = load_query(config, query_name)
    adapted_query = load_adapted_query(config, query_name)
    # Use adapted query for cache operations if available (SSB/TPC-H multi-table originals)
    cache_query = adapted_query if adapted_query else original_query
    pks = config.query_partition_keys[query_name]

    result: dict = {
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

    # 1b. EXPLAIN ANALYZE on baseline (if requested and PostgreSQL)
    if explain and hasattr(executor, "explain_analyze"):
        try:
            plan_lines, _ = executor.explain_analyze(original_query)
            result["baseline_explain"] = parse_explain_plan(plan_lines)
        except Exception as e:
            print(f"  Warning: EXPLAIN ANALYZE failed for baseline: {e}")

    # 2. Populate cache (using adapted query if available)
    cache_start = time.perf_counter()
    cache_stats = populate_cache_for_query(executor, cache_manager, cache_query, pks, config)
    cache_pop_time = time.perf_counter() - cache_start
    result["cache_population_time"] = cache_pop_time
    result["cache_stats"] = cache_stats

    # 2b. Query spatial cache statistics (geometry type, bbox area, etc.)
    if explain:
        spatial_cache_stats = {}
        for pk in pks:
            pk_config = config.partition_keys.get(pk, {})
            if pk_config.get("datatype") == "geometry" and hasattr(executor, "explain_analyze"):
                # Collect all hashes that were populated for this PK
                qp = config.query_processing
                gen_kwargs: dict = {
                    "query": cache_query,
                    "partition_key": pk,
                    "min_component_size": 1,
                    "keep_all_attributes": qp.get("keep_all_attributes", False),
                    "strip_select": qp.get("strip_select", True),
                    "auto_detect_partition_join": qp.get("auto_detect_partition_join", False),
                    "skip_partition_key_joins": True,
                    "follow_graph": qp.get("follow_graph", False),
                }
                if pk_config.get("geometry_column"):
                    gen_kwargs["geometry_column"] = pk_config["geometry_column"]

                pairs = generate_all_query_hash_pairs(**gen_kwargs)
                hashes = [h for _, h in pairs]
                stats = query_spatial_cache_stats(executor, config, pk, hashes)
                if stats:
                    spatial_cache_stats[pk] = stats
        if spatial_cache_stats:
            result["spatial_cache_stats"] = spatial_cache_stats

    # 3. Apply cache and run enhanced query (with repeat)
    # Cache application also uses adapted query for hash matching, but extends the original query
    if api == "lazy":
        enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(cache_manager, original_query, pks, config, method=method, cache_query=cache_query, spatial_method=spatial_method)
    else:
        enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
            cache_manager, original_query, pks, config, fact_stats=fact_stats, method=method, cache_query=cache_query, spatial_method=spatial_method
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

    # 3b. EXPLAIN ANALYZE on enhanced query (if requested and PostgreSQL)
    if explain and hasattr(executor, "explain_analyze"):
        try:
            plan_lines, _ = executor.explain_analyze(enhanced_query)
            result["enhanced_explain"] = parse_explain_plan(plan_lines)
        except Exception as e:
            print(f"  Warning: EXPLAIN ANALYZE failed for enhanced query: {e}")

        # Check for ST_Intersects vs ST_DWithin in the enhanced query
        enhanced_lower = enhanced_query.lower()
        result["uses_st_intersects"] = "st_intersects" in enhanced_lower
        result["uses_st_dwithin"] = "st_dwithin" in enhanced_lower

    # 4. Compute metrics
    result["speedup"] = result["baseline_time"] / result["cached_time"] if result["cached_time"] > 0 else float("inf")
    result["result_match"] = len(baseline_rows) == len(cached_rows)

    return result


# =============================================================================
# Benchmark Mode Functions
# =============================================================================


def run_all_queries(executor, cache_manager, config, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy", explain=False, spatial_method="SUBDIVIDE_TMP_TABLE"):
    """Run all benchmark queries."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: All Queries ({len(config.all_queries)} queries, method={method}, api={api})")
    print("=" * 80)

    results = []
    for query_name in config.all_queries:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, config, repeat=repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_flight(executor, cache_manager, config, flight_num, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy", explain=False, spatial_method="SUBDIVIDE_TMP_TABLE"):
    """Run a single query flight."""
    queries = config.query_flights.get(flight_num)
    if not queries:
        valid = ", ".join(str(f) for f in sorted(config.query_flights))
        print(f"Invalid flight number: {flight_num}. Choose from: {valid}")
        return []

    print(f"\n{'=' * 80}")
    print(f"{config.name}: Flight Q{flight_num} (method={method}, api={api})")
    print(f"{'=' * 80}")

    results = []
    for query_name in queries:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, config, repeat=repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def _run_cross_dimension_phase(executor, cache_manager, config, flights, phase_label, results, repeat, fact_stats, method, api):
    """Run a cross-dimension phase for the given flights."""
    qp = config.query_processing
    for flight_num in flights:
        queries = config.query_flights[flight_num]
        for i, query_name in enumerate(queries):
            original_query = load_query(config, query_name)
            pks = config.query_partition_keys[query_name]

            print(f"\n--- {query_name} (F{flight_num}.{i + 1}) ---")

            # Check for pre-existing cache hits BEFORE populating
            pre_hits = {}
            for pk in pks:
                handler = cache_manager.get_handler(pk)
                get_kwargs: dict = {
                    "query": original_query,
                    "cache_handler": handler.underlying_handler,
                    "partition_key": pk,
                    "min_component_size": 1,
                    "auto_detect_partition_join": qp.get("auto_detect_partition_join", False),
                }
                _, num_gen, num_hits = partitioncache.get_partition_keys(**get_kwargs)
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
            cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks, config)
            cache_pop_time = time.perf_counter() - cache_start

            # Apply and run (with repeat)
            if api == "lazy":
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(cache_manager, original_query, pks, config, method=method)
            else:
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                    cache_manager, original_query, pks, config, fact_stats=fact_stats, method=method
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
                marker = " *** CROSS-DIM REUSE ***" if shared > 0 and phase_label == "Phase 2" else ""
                print(
                    f"    {pk}: {pk_stats[pk]['variants_checked']} variants, "
                    f"{hits_after} hits ({shared} pre-existing, {new_hits} new), "
                    f"{pk_stats[pk].get('partition_keys_found', 'n/a')} keys{marker}"
                )
            print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s " f"(speedup: {speedup:.2f}x, match: {result['result_match']})")


def run_cross_dimension(executor, cache_manager, config, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """Cross-dimension cache reuse evaluation."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: Cross-Dimension Cache Reuse")
    print("=" * 80)

    results = []
    cd_cfg = config.modes.get("cross_dimension", {})
    phase1_flights = cd_cfg.get("phase1_flights", [3])
    phase2_flights = cd_cfg.get("phase2_flights", [4])

    print(f"\n### Phase 1: Populate cache with flights {phase1_flights}")
    _run_cross_dimension_phase(executor, cache_manager, config, phase1_flights, "Phase 1", results, repeat, fact_stats, method, api)

    print(f"\n### Phase 2: Cross-dimension queries from flights {phase2_flights}")
    _run_cross_dimension_phase(executor, cache_manager, config, phase2_flights, "Phase 2", results, repeat, fact_stats, method, api)

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


def run_cold_vs_warm(executor, cache_manager, config, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy", explain=False, spatial_method="SUBDIVIDE_TMP_TABLE"):
    """Compare cold cache vs warm cache."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: Cold vs Warm Cache")
    print("=" * 80)

    cw_cfg = config.modes.get("cold_warm", {})
    flight_num = cw_cfg.get("flight", 3)
    flight_queries = config.query_flights.get(flight_num, [])

    # Cold run
    print("\n### Cold Run (empty cache)")
    cache_manager.clear_all()

    cold_results = []
    for query_name in flight_queries:
        print(f"\n--- {query_name} (cold) ---")
        result = run_single_query(executor, cache_manager, query_name, config, repeat=repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        result["mode"] = "cold"
        cold_results.append(result)
        print_query_result(result)

    # Warm run
    print("\n### Warm Run (cache from cold run)")
    warm_results = []
    for query_name in flight_queries:
        print(f"\n--- {query_name} (warm) ---")
        result = run_single_query(executor, cache_manager, query_name, config, repeat=repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        result["mode"] = "warm"
        warm_results.append(result)
        print_query_result(result)

    # Comparison
    print(f"\n{'=' * 80}")
    print(f"{'Query':<25} {'Cold Total':>12} {'Warm Total':>12} {'Savings':>10}")
    print("-" * 61)
    for cold, warm in zip(cold_results, warm_results, strict=False):
        cold_total = cold["cache_population_time"] + cold["cached_time"]
        warm_total = warm["cache_population_time"] + warm["cached_time"]
        savings = (cold_total - warm_total) / cold_total * 100 if cold_total > 0 else 0
        print(f"{cold['query']:<25} {cold_total:>11.4f}s {warm_total:>11.4f}s {savings:>9.1f}%")

    return cold_results + warm_results


def run_hierarchy(executor, cache_manager, config, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """Hierarchical drill-down evaluation."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: Hierarchical Drill-Down")
    print("=" * 80)

    h_cfg = config.modes.get("hierarchy", {})
    hierarchy_flights = h_cfg.get("flights", [5, 6])
    qp = config.query_processing

    if not hierarchy_flights:
        print("  No hierarchy flights configured for this benchmark.")
        return []

    all_results = []

    for flight_num in hierarchy_flights:
        queries = config.query_flights.get(flight_num, [])
        if not queries:
            continue

        # Build flight label from hierarchy labels
        first_label = config.hierarchy_labels.get(queries[0], queries[0])
        last_label = config.hierarchy_labels.get(queries[-1], queries[-1])
        flight_label = f"Q{flight_num}: {first_label} -> ... -> {last_label}"

        print(f"\n### {flight_label}")
        print("Constant conditions kept the same; only the target dimension drills deeper.\n")

        flight_results = []

        for query_name in queries:
            original_query = load_query(config, query_name)
            pks = config.query_partition_keys[query_name]
            level_label = config.hierarchy_labels.get(query_name, query_name)

            print(f"\n--- {query_name}: {level_label} ---")

            # Check for pre-existing cache hits BEFORE populating
            pre_hits = {}
            for pk in pks:
                handler = cache_manager.get_handler(pk)
                get_kwargs: dict = {
                    "query": original_query,
                    "cache_handler": handler.underlying_handler,
                    "partition_key": pk,
                    "min_component_size": 1,
                    "auto_detect_partition_join": qp.get("auto_detect_partition_join", False),
                }
                _, num_gen, num_hits = partitioncache.get_partition_keys(**get_kwargs)
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
            cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks, config)
            cache_pop_time = time.perf_counter() - cache_start

            # Apply cache and run enhanced query (with repeat)
            if api == "lazy":
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(cache_manager, original_query, pks, config, method=method)
            else:
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                    cache_manager, original_query, pks, config, method=method
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
                print(
                    f"    {pk}: {pk_stats[pk]['variants_checked']} variants, "
                    f"{hits_after} hits ({reused} reused, {new_hits} new), "
                    f"{pk_stats[pk].get('partition_keys_found', 'n/a')} keys, "
                    f"pop: {pop_time:.4f}s{marker}"
                )
            print(f"  Cached: {len(cached_rows)} rows in {cached_time:.4f}s " f"(speedup: {speedup:.2f}x, match: {result['result_match']})")

        # Per-flight hierarchy summary table
        print(f"\n{'Level':<40} {'Pre-Hits':>10} {'New Hits':>10} {'Reuse%':>8} {'Speedup':>9}")
        print("-" * 80)
        for r in flight_results:
            total_pre = sum(r["pre_existing_hits"][pk]["hits_before"] for pk in r["partition_keys"])
            total_after = sum(r["apply_stats"][pk]["cache_hits"] for pk in r["partition_keys"])
            total_new = total_after - total_pre
            reuse_pct = total_pre / total_after * 100 if total_after > 0 else 0
            print(f"{r['hierarchy_level']:<40} {total_pre:>10} {total_new:>10} " f"{reuse_pct:>7.1f}% {r['speedup']:>8.2f}x")

        all_results.extend(flight_results)

    # Overall summary
    print(f"\n{'=' * 80}")
    all_match = all(r["result_match"] for r in all_results)
    print("Hierarchy drill-down summary:")
    print(f"  Queries evaluated: {len(all_results)}")
    print(f"  All results correct: {all_match}")

    return all_results


def run_spatial(executor, cache_manager, config, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy", explain=False, spatial_method="SUBDIVIDE_TMP_TABLE"):
    """Run spatial-heavy queries."""
    if not config.spatial_queries:
        print("  No spatial queries configured for this benchmark.")
        return []

    print(f"\n{'=' * 80}")
    print(f"{config.name}: Spatial-Heavy Queries ({len(config.spatial_queries)} queries)")
    print("=" * 80)
    print(f"Selected queries: {', '.join(config.spatial_queries)}")

    results = []
    for query_name in config.spatial_queries:
        print(f"\n--- {query_name} ---")
        result = run_single_query(executor, cache_manager, query_name, config, repeat=repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        results.append(result)
        print_query_result(result)

    print_summary(results)
    return results


def run_spatial_backend_comparison(executor, config, scale_factor=0.01, db_path=None, max_pk_value=0, pk_max_values=None, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy", explain=False, spatial_method="SUBDIVIDE_TMP_TABLE"):
    """Compare H3 vs BBox spatial backends on the same spatial queries."""
    if not config.spatial_queries:
        print("  No spatial queries configured for this benchmark.")
        return []

    # Find all spatial partition keys grouped by backend
    spatial_pks = {}
    for pk_name, pk_cfg in config.partition_keys.items():
        if pk_cfg.get("datatype") == "geometry":
            backend = pk_cfg.get("spatial_backend", "rocksdict_h3_grouped")
            spatial_pks[pk_name] = {"backend": backend, "config": pk_cfg}

    if len(spatial_pks) < 2:
        print("  Need at least 2 spatial partition keys (e.g., spatial_h3 and spatial_bbox) for comparison.")
        print(f"  Found: {list(spatial_pks.keys())}")
        return []

    print(f"\n{'=' * 80}")
    print(f"{config.name}: Spatial Backend Comparison")
    print("=" * 80)
    backend_labels = [f"{pk} ({info['backend']})" for pk, info in spatial_pks.items()]
    print(f"Spatial backends: {', '.join(backend_labels)}")
    print(f"Spatial queries: {', '.join(config.spatial_queries)}")

    all_results: dict[str, list[dict]] = {}

    for pk_name, pk_info in spatial_pks.items():
        backend_label = f"{pk_name} ({pk_info['backend']})"
        print(f"\n### Backend: {backend_label}")

        # Create a temporary config variant where spatial queries map to this PK
        temp_config = copy.deepcopy(config)
        for sq in config.spatial_queries:
            temp_config.query_partition_keys[sq] = [pk_name]

        # Create a cache manager that uses the non-spatial cache type as base
        # but spatial handlers will be created for geometry PKs
        cache_type = config.backends.get("postgresql", ["postgresql_array"])[0]
        cm = CacheManager(cache_type, temp_config, scale_factor=scale_factor, db_path=db_path, max_pk_value=max_pk_value, pk_max_values=pk_max_values)

        try:
            backend_results = []
            for query_name in config.spatial_queries:
                print(f"\n--- {query_name} ---")
                try:
                    result = run_single_query(
                        executor, cm, query_name, temp_config, repeat=repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method
                    )
                    result["spatial_backend"] = pk_info["backend"]
                    result["spatial_pk"] = pk_name
                    backend_results.append(result)
                    print_query_result(result)
                except Exception as e:
                    print(f"  Error: {e}")
                    backend_results.append({"query": query_name, "error": str(e), "spatial_backend": pk_info["backend"], "spatial_pk": pk_name})

            all_results[backend_label] = backend_results
        finally:
            try:
                cm.clear_all()
                cm.close()
            except Exception:
                pass

    # Comparison table
    print(f"\n{'=' * 80}")
    print("Spatial Backend Comparison Summary")
    has_explain = any("enhanced_explain" in r for results in all_results.values() for r in results if "error" not in r)
    if has_explain:
        header = f"{'Backend':<35} {'Query':<25} {'Pop Time':>10} {'Cached':>10} {'Speedup':>9} {'Scan Type':<12} {'GiST':>5} {'Match':>6}"
        print(f"\n{header}")
        print("-" * 114)
    else:
        header = f"{'Backend':<35} {'Query':<25} {'Pop Time':>10} {'Cached':>10} {'Speedup':>9}"
        print(f"\n{header}")
        print("-" * 91)
    for label, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{label:<35} {r['query']:<25} {'ERROR':>10}")
            else:
                line = (f"{label:<35} {r['query']:<25} "
                        f"{r['cache_population_time']:>9.4f}s "
                        f"{r['cached_time']:>9.4f}s "
                        f"{r['speedup']:>8.2f}x")
                if has_explain and "enhanced_explain" in r:
                    ep = r["enhanced_explain"]
                    gist = "Yes" if ep["gist_index_used"] else "No"
                    match = "Y" if r.get("result_match") else "N"
                    line += f" {ep['scan_type']:<12} {gist:>5} {match:>6}"
                print(line)

    flat_results = []
    for _label, results in all_results.items():
        flat_results.extend(results)
    return flat_results


def run_spatial_method_comparison(
    executor,
    config,
    scale_factor=0.01,
    db_path=None,
    max_pk_value=0,
    pk_max_values=None,
    repeat=1,
    fact_stats=None,
    explain=False,
):
    """Compare SUBDIVIDE_INLINE vs SUBDIVIDE_TMP_TABLE for each spatial backend.

    For each spatial partition key (H3, BBox), runs all spatial queries with both
    spatial methods using lazy API (IN_SUBQUERY). Prints per-query and summary tables.
    """
    if not config.spatial_queries:
        print("  No spatial queries configured for this benchmark.")
        return []

    # Find all spatial partition keys
    spatial_pks = {}
    for pk_name, pk_cfg in config.partition_keys.items():
        if pk_cfg.get("datatype") == "geometry":
            backend = pk_cfg.get("spatial_backend", "rocksdict_h3_grouped")
            spatial_pks[pk_name] = {"backend": backend, "config": pk_cfg}

    if not spatial_pks:
        print("  No spatial partition keys configured.")
        return []

    spatial_methods = ["SUBDIVIDE_TMP_TABLE", "SUBDIVIDE_INLINE"]

    print(f"\n{'=' * 100}")
    print(f"{config.name}: Spatial Method Comparison")
    print("=" * 100)
    backend_labels = [f"{pk} ({info['backend']})" for pk, info in spatial_pks.items()]
    print(f"Spatial backends: {', '.join(backend_labels)}")
    print(f"Spatial methods: {', '.join(spatial_methods)}")
    print(f"Spatial queries: {', '.join(config.spatial_queries)}")
    print("API: lazy (IN_SUBQUERY)")

    all_results: dict[str, list[dict]] = {}

    for pk_name, pk_info in spatial_pks.items():
        for sm in spatial_methods:
            label = f"{pk_name} ({pk_info['backend']}) / {sm}"
            print(f"\n### {label}")

            # Create a temporary config where spatial queries map to this PK
            temp_config = copy.deepcopy(config)
            for sq in config.spatial_queries:
                temp_config.query_partition_keys[sq] = [pk_name]

            cache_type = config.backends.get("postgresql", ["postgresql_array"])[0]
            cm = CacheManager(
                cache_type, temp_config, scale_factor=scale_factor,
                db_path=db_path, max_pk_value=max_pk_value, pk_max_values=pk_max_values,
            )

            try:
                combo_results = []
                for query_name in config.spatial_queries:
                    print(f"\n--- {query_name} ---")
                    try:
                        result = run_single_query(
                            executor, cm, query_name, temp_config,
                            repeat=repeat, fact_stats=fact_stats,
                            method="IN_SUBQUERY", api="lazy",
                            explain=explain, spatial_method=sm,
                        )
                        result["spatial_backend"] = pk_info["backend"]
                        result["spatial_pk"] = pk_name
                        result["spatial_method"] = sm
                        combo_results.append(result)
                        print_query_result(result)
                    except Exception as e:
                        print(f"  Error: {e}")
                        import traceback
                        traceback.print_exc()
                        combo_results.append({
                            "query": query_name, "error": str(e),
                            "spatial_backend": pk_info["backend"],
                            "spatial_pk": pk_name, "spatial_method": sm,
                        })

                all_results[label] = combo_results
            finally:
                try:
                    cm.clear_all()
                    cm.close()
                except Exception:
                    pass

    # Comparison table
    print(f"\n{'=' * 120}")
    print("Spatial Method Comparison Summary")
    has_explain_data = any(
        "enhanced_explain" in r
        for results in all_results.values()
        for r in results if "error" not in r
    )
    if has_explain_data:
        header = (
            f"{'Backend/Method':<45} {'Query':<25} {'Baseline':>10} {'Cached':>10} "
            f"{'Speedup':>9} {'Scan Type':<12} {'GiST':>5} {'Match':>6}"
        )
        print(f"\n{header}")
        print("-" * 125)
    else:
        header = f"{'Backend/Method':<45} {'Query':<25} {'Baseline':>10} {'Cached':>10} {'Speedup':>9} {'Match':>6}"
        print(f"\n{header}")
        print("-" * 108)

    for label, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{label:<45} {r['query']:<25} {'ERROR':>10}")
            else:
                line = (
                    f"{label:<45} {r['query']:<25} "
                    f"{r['baseline_time']:>9.4f}s "
                    f"{r['cached_time']:>9.4f}s "
                    f"{r['speedup']:>8.2f}x"
                )
                if has_explain_data and "enhanced_explain" in r:
                    ep = r["enhanced_explain"]
                    gist = "Yes" if ep["gist_index_used"] else "No"
                    match = "Y" if r.get("result_match") else "N"
                    line += f" {ep['scan_type']:<12} {gist:>5} {match:>6}"
                else:
                    match = "Y" if r.get("result_match") else "N"
                    line += f" {match:>6}"
                print(line)

    # Per-method average summary
    print(f"\n{'=' * 80}")
    print("Per-Backend/Method Averages")
    print(f"\n{'Label':<45} {'Avg Speedup':>12} {'All Correct':>12}")
    print("-" * 72)
    for label, results in all_results.items():
        valid = [r for r in results if "error" not in r]
        if valid:
            avg_speedup = sum(r["speedup"] for r in valid) / len(valid)
            all_match = all(r.get("result_match", False) for r in valid)
            print(f"{label:<45} {avg_speedup:>11.2f}x {'Yes' if all_match else 'No':>12}")

    flat_results = []
    for _label, results in all_results.items():
        flat_results.extend(results)
    return flat_results


def run_backend_comparison(executor, config, db_backend, scale_factor=0.01, db_path=None, max_pk_value=0, pk_max_values=None, repeat=1, fact_stats=None, method="TMP_TABLE_IN", api="non-lazy"):
    """Compare different cache backends on representative queries."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: Backend Comparison (api={api})")
    print("=" * 80)

    representative = config.modes.get("representative_queries", config.all_queries[:3])
    backends = config.backends.get(db_backend, [])

    if not backends:
        print(f"  No backends configured for {db_backend}.")
        return []

    # Determine which API modes to run
    api_modes = ["non-lazy", "lazy"] if api == "both" else [api]

    all_results = {}

    for backend in backends:
        for api_mode in api_modes:
            label = f"{backend}/{api_mode}" if len(api_modes) > 1 else backend
            print(f"\n### Backend: {label}")

            effective_method = method
            if api_mode == "lazy" and method in ("IN", "VALUES"):
                effective_method = "IN_SUBQUERY"
                print(f"  (method auto-mapped to {effective_method} for lazy API)")

            try:
                cm = CacheManager(backend, config, scale_factor=scale_factor, db_path=db_path, max_pk_value=max_pk_value, pk_max_values=pk_max_values)
                # Test handler creation
                first_pk = list(config.partition_keys.keys())[0]
                cm.get_handler(first_pk)
            except Exception as e:
                print(f"  Skipped: {e}")
                continue

            try:
                cm.clear_all()
            except Exception:
                pass

            backend_results = []
            for query_name in representative:
                print(f"\n--- {query_name} ---")
                try:
                    result = run_single_query(
                        executor, cm, query_name, config, repeat=repeat, fact_stats=fact_stats, method=effective_method, api=api_mode
                    )
                    result["cache_backend"] = backend
                    backend_results.append(result)
                    print_query_result(result)
                except Exception as e:
                    print(f"  Error: {e}")
                    backend_results.append({"query": query_name, "error": str(e), "cache_backend": backend, "api": api_mode})

            all_results[label] = backend_results

            try:
                cm.clear_all()
                cm.close()
            except Exception:
                pass

    # Comparison table
    print(f"\n{'=' * 80}")
    print("Backend Comparison Summary")
    header = f"{'Backend':<35} {'Query':<25} {'Pop Time':>10} {'Cached':>10} {'Speedup':>9}"
    print(f"\n{header}")
    print("-" * 92)
    for label, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{label:<35} {r['query']:<25} {'ERROR':>10} {'':>10} {'':>9}")
            else:
                print(f"{label:<35} {r['query']:<25} " f"{r['cache_population_time']:>9.4f}s " f"{r['cached_time']:>9.4f}s " f"{r['speedup']:>8.2f}x")

    flat_results = []
    for _label, results in all_results.items():
        flat_results.extend(results)
    return flat_results


def run_method_comparison(executor, cache_manager, config, repeat=1, fact_stats=None):
    """Compare different query extension methods."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: Method Comparison")
    print("=" * 80)

    representative = config.modes.get("representative_queries", config.all_queries[:3])
    methods = ["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]

    # Populate cache first
    print("\n### Phase 1: Populate cache")
    for query_name in representative:
        original_query = load_query(config, query_name)
        pks = config.query_partition_keys[query_name]
        cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks, config)
        for pk, stats in cache_stats.items():
            print(f"  {query_name}/{pk}: {stats['fragments_generated']} fragments, " f"{stats['fragments_executed']} executed")

    # Run each method
    all_results = {}
    for method_name in methods:
        print(f"\n### Method: {method_name}")
        method_results = []

        for query_name in representative:
            print(f"\n--- {query_name} ({method_name}) ---")
            original_query = load_query(config, query_name)
            pks = config.query_partition_keys[query_name]

            rows, baseline_time = executor.execute(original_query)
            baseline_rows = len(rows)

            try:
                enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                    cache_manager, original_query, pks, config, fact_stats=fact_stats, method=method_name
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
                print(f"  Cached: {cached_rows} rows in {cached_time:.4f}s " f"(speedup: {speedup:.2f}x, match: {match})")
            except Exception as e:
                print(f"  Error: {e}")
                method_results.append({"query": query_name, "method": method_name, "error": str(e)})

        all_results[method_name] = method_results

    # Comparison table
    print(f"\n{'=' * 80}")
    print("Method Comparison Summary")
    print(f"\n{'Method':<18} {'Query':<25} {'Baseline':>10} {'Cached':>10} {'Apply':>10} {'Speedup':>9} {'Match':>7}")
    print("-" * 92)
    for method_name, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{method_name:<18} {r['query']:<25} {'ERROR':>10}")
            else:
                print(
                    f"{method_name:<18} {r['query']:<25} "
                    f"{r['baseline_time']:>9.4f}s "
                    f"{r['cached_time']:>9.4f}s "
                    f"{r['cache_apply_time']:>9.4f}s "
                    f"{r['speedup']:>8.2f}x "
                    f"{'Y' if r['result_match'] else 'N':>6}"
                )

    flat_results = []
    for _method_name, results in all_results.items():
        flat_results.extend(results)
    return flat_results


def run_api_comparison(executor, cache_manager, config, repeat=1, fact_stats=None, spatial_method="SUBDIVIDE_TMP_TABLE"):
    """Compare non-lazy vs lazy API with all applicable methods."""
    print(f"\n{'=' * 80}")
    print(f"{config.name}: API Comparison (non-lazy vs lazy)")
    print("=" * 80)

    representative = config.modes.get("representative_queries", config.all_queries[:3])

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
    for query_name in representative:
        original_query = load_query(config, query_name)
        pks = config.query_partition_keys[query_name]
        cache_stats = populate_cache_for_query(executor, cache_manager, original_query, pks, config)
        for pk, stats in cache_stats.items():
            print(f"  {query_name}/{pk}: {stats['fragments_generated']} fragments, " f"{stats['fragments_executed']} executed")

    # Phase 2: Run baselines
    print("\n### Phase 2: Baselines")
    baselines = {}
    for query_name in representative:
        original_query = load_query(config, query_name)
        baseline_times = []
        baseline_rows = None
        for _ in range(repeat):
            rows, elapsed = executor.execute(original_query)
            baseline_times.append(elapsed)
            if baseline_rows is None:
                baseline_rows = rows
        baselines[query_name] = {"time": statistics.mean(baseline_times), "rows": len(baseline_rows)}
        print(f"  {query_name}: {baselines[query_name]['rows']} rows in {baselines[query_name]['time']:.4f}s")

    # Phase 3: Run each combination
    all_results = {}
    for api_mode, method_name in combinations:
        label = f"{api_mode}/{method_name}"
        print(f"\n### {label}")
        combo_results = []

        for query_name in representative:
            print(f"\n--- {query_name} ({label}) ---")
            original_query = load_query(config, query_name)
            pks = config.query_partition_keys[query_name]

            try:
                if api_mode == "lazy":
                    enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query_lazy(
                        cache_manager, original_query, pks, config, method=method_name, spatial_method=spatial_method
                    )
                else:
                    enhanced_query, pk_stats, cache_apply_time = apply_cache_to_query(
                        cache_manager, original_query, pks, config, fact_stats=fact_stats, method=method_name, spatial_method=spatial_method
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
                print(f"  Cached: {cached_rows} rows in {cached_time:.4f}s " f"(speedup: {speedup:.2f}x, match: {match})")
            except Exception as e:
                print(f"  Error: {e}")
                combo_results.append({"query": query_name, "api": api_mode, "method": method_name, "error": str(e)})

        all_results[label] = combo_results

    # Comparison table
    print(f"\n{'=' * 80}")
    print("API Comparison Summary")
    print(f"\n{'API/Method':<25} {'Query':<25} {'Baseline':>10} {'Cached':>10} {'Apply':>10} {'Speedup':>9} {'Match':>7}")
    print("-" * 99)
    for label, results in all_results.items():
        for r in results:
            if "error" in r:
                print(f"{label:<25} {r['query']:<25} {'ERROR':>10}")
            else:
                print(
                    f"{label:<25} {r['query']:<25} "
                    f"{r['baseline_time']:>9.4f}s "
                    f"{r['cached_time']:>9.4f}s "
                    f"{r['cache_apply_time']:>9.4f}s "
                    f"{r['speedup']:>8.2f}x "
                    f"{'Y' if r['result_match'] else 'N':>6}"
                )

    flat_results = []
    for _label, results in all_results.items():
        flat_results.extend(results)
    return flat_results


# =============================================================================
# Output Functions
# =============================================================================


def print_query_result(result: dict):
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
        print(f"    {pk}: {cs.get('fragments_generated', 0)} fragments, " f"{ap.get('cache_hits', 0)} hits, " f"{ap.get('partition_keys_found', 0)} keys{pop_str}")

    # Search space reduction
    has_reduction = any("search_space_reduction" in result["apply_stats"].get(pk, {}) for pk in result["partition_keys"])
    if has_reduction:
        print("  Search Space Reduction:")
        for pk in result["partition_keys"]:
            ssr = result["apply_stats"].get(pk, {}).get("search_space_reduction")
            if ssr:
                print(f"    {pk}: {ssr['total_distinct']:,} total -> " f"{ssr['cached_set_size']:,} cached " f"({ssr['reduction_pct']:.1f}% reduction)")

    # Spatial cache stats
    if "spatial_cache_stats" in result:
        print("  Spatial Cache Geometry Stats:")
        for pk, stats_list in result["spatial_cache_stats"].items():
            for s in stats_list:
                area_str = f"{s['bbox_area']:,.0f} m²" if s["bbox_area"] else "n/a"
                print(f"    {pk}/{s['query_hash'][:12]}...: "
                      f"type={s['geom_type']}, "
                      f"geoms={s['num_geometries']}, "
                      f"points={s['num_points']}, "
                      f"bbox_area={area_str}, "
                      f"count={s['partition_keys_count']}")

    cached_str = f"{result['cached_time']:.4f}s"
    if "cached_stddev" in result:
        cached_str += f" (stddev: {result['cached_stddev']:.4f}s)"
    print(f"  Cached: {result['cached_rows']} rows in {cached_str} " f"(speedup: {result['speedup']:.2f}x, match: {result['result_match']})")

    # EXPLAIN ANALYZE summaries
    if "baseline_explain" in result:
        print_explain_summary("Baseline", result["baseline_explain"])
    if "enhanced_explain" in result:
        print_explain_summary("Enhanced", result["enhanced_explain"])
    if result.get("uses_st_intersects") is not None:
        intersects = "Yes" if result.get("uses_st_intersects") else "No"
        dwithin = "Yes" if result.get("uses_st_dwithin") else "No"
        print(f"  Spatial filter: ST_Intersects={intersects}, ST_DWithin={dwithin}")


def _avg_reduction(result: dict) -> float | None:
    """Compute average search space reduction % across all PKs for a result."""
    reductions = []
    for pk in result.get("partition_keys", []):
        ssr = result.get("apply_stats", {}).get(pk, {}).get("search_space_reduction")
        if ssr:
            reductions.append(ssr["reduction_pct"])
    return sum(reductions) / len(reductions) if reductions else None


def print_summary(results: list[dict]):
    """Print overall summary."""
    if not results:
        return

    has_stddev = any("baseline_stddev" in r for r in results)
    has_reduction = any(_avg_reduction(r) is not None for r in results)

    if has_stddev:
        header = f"{'Query':<25} {'Baseline':>10} {'B.Std':>8} {'Cached':>10} {'C.Std':>8} {'Speedup':>9} {'Match':>7}"
        if has_reduction:
            header += f" {'Reduction':>10}"
        print(f"\n{'=' * 80}")
        print(header)
        print("-" * len(header))
        for r in results:
            b_std = f"{r.get('baseline_stddev', 0):.4f}" if "baseline_stddev" in r else "n/a"
            c_std = f"{r.get('cached_stddev', 0):.4f}" if "cached_stddev" in r else "n/a"
            line = (
                f"{r['query']:<25} {r['baseline_time']:>9.4f}s {b_std:>8} "
                f"{r['cached_time']:>9.4f}s {c_std:>8} "
                f"{r['speedup']:>8.2f}x {'Y' if r['result_match'] else 'N':>6}"
            )
            if has_reduction:
                avg_red = _avg_reduction(r)
                line += f" {avg_red:>9.1f}%" if avg_red is not None else f" {'n/a':>10}"
            print(line)
    else:
        header = f"{'Query':<25} {'Baseline':>10} {'Cached':>10} {'Speedup':>9} {'Match':>7}"
        if has_reduction:
            header += f" {'Reduction':>10}"
        print(f"\n{'=' * 80}")
        print(header)
        print("-" * len(header))
        for r in results:
            line = f"{r['query']:<25} {r['baseline_time']:>9.4f}s {r['cached_time']:>9.4f}s " f"{r['speedup']:>8.2f}x {'Y' if r['result_match'] else 'N':>6}"
            if has_reduction:
                avg_red = _avg_reduction(r)
                line += f" {avg_red:>9.1f}%" if avg_red is not None else f" {'n/a':>10}"
            print(line)

    avg_speedup = sum(r["speedup"] for r in results) / len(results) if results else 0
    all_match = all(r["result_match"] for r in results)
    print(f"\nAverage speedup: {avg_speedup:.2f}x")
    print(f"All results correct: {all_match}")


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Unified PartitionCache Benchmark Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python run_benchmark.py --config config/ssb.yaml --db-backend duckdb --mode all
  python run_benchmark.py --config config/tpch.yaml --db-backend duckdb --mode flight 3
  python run_benchmark.py --config config/nyc_taxi.yaml --mode api-comparison
  python run_benchmark.py --config config/osm_poi.yaml --mode all
""",
    )
    parser.add_argument("--config", type=str, required=True, help="Path to YAML config file")
    parser.add_argument(
        "--mode",
        nargs="+",
        default=["all"],
        help="Benchmark mode: all | flight N | cross-dimension | cold-vs-warm | hierarchy | spatial | spatial-backend-comparison | spatial-method-comparison | backend-comparison | method-comparison | api-comparison",
    )
    parser.add_argument("--db-backend", type=str, default=None, help="Database backend: duckdb, postgresql (default: first enabled in config)")
    parser.add_argument("--cache-backend", type=str, default=None, help="Cache backend override (default: first in config's backends list)")
    parser.add_argument("--scale-factor", type=float, default=0.01, help="Scale factor for SSB/TPC-H (default: 0.01)")
    parser.add_argument("--db-path", type=str, default=None, help="DuckDB file path override")
    parser.add_argument(
        "--method",
        type=str,
        default="TMP_TABLE_IN",
        choices=["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "IN_SUBQUERY"],
        help="Query extension method (default: TMP_TABLE_IN)",
    )
    parser.add_argument(
        "--api",
        type=str,
        default="non-lazy",
        choices=["non-lazy", "lazy", "both"],
        help="API path: non-lazy, lazy, both (default: non-lazy)",
    )
    parser.add_argument("--repeat", type=int, default=1, help="Number of repetitions for timing (default: 1)")
    parser.add_argument("--explain", action="store_true", help="Capture EXPLAIN ANALYZE for enhanced queries (PostgreSQL only)")
    parser.add_argument(
        "--spatial-method",
        type=str,
        default="SUBDIVIDE_TMP_TABLE",
        choices=["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE"],
        help="Spatial filter method: SUBDIVIDE_TMP_TABLE (temp table + GiST, default) or SUBDIVIDE_INLINE (no temp table)",
    )
    parser.add_argument("--output", type=str, default=None, help="Save results to JSON file")

    args = parser.parse_args()

    # Resolve config path: try as-is first, then relative to script directory
    script_dir = Path(__file__).parent
    config_path = Path(args.config)
    if not config_path.is_absolute():
        if config_path.exists():
            config_path = config_path.resolve()
        else:
            config_path = script_dir / config_path

    config = BenchmarkConfig.from_yaml(str(config_path))
    print(f"Benchmark: {config.name}")

    # Determine DB backend
    db_backend = args.db_backend
    if not db_backend:
        for backend_name in ["duckdb", "postgresql"]:
            if config.executors.get(backend_name, {}).get("enabled", False):
                db_backend = backend_name
                break
    if not db_backend:
        print("Error: No database backend available. Check config executors.")
        sys.exit(1)

    if not config.executors.get(db_backend, {}).get("enabled", False):
        print(f"Error: Database backend '{db_backend}' is not enabled in config.")
        sys.exit(1)

    # Load environment from config's env_file
    project_root = Path(__file__).parent.parent.parent
    env_file_path = project_root / config.env_file
    if env_file_path.exists():
        from dotenv import load_dotenv

        load_dotenv(str(env_file_path), override=True)

    # Set up executor
    db_path = None
    if db_backend == "duckdb":
        template = config.executors["duckdb"].get("default_path_template", "")
        db_path = args.db_path or str(project_root / template.format(scale_factor=args.scale_factor))

        if not os.path.exists(db_path):
            print(f"Database not found: {db_path}")
            example_cmd = config.data_setup.get("example_command", "")
            if example_cmd:
                print(f"Generate data first:\n  {example_cmd}")
            sys.exit(1)

        print(f"Using DuckDB: {db_path}")
        executor = DuckDBExecutor(db_path)
    else:
        print("Using PostgreSQL")
        executor = PostgreSQLExecutor(str(env_file_path) if env_file_path.exists() else None)

    # Determine cache backend
    default_backends = config.backends.get(db_backend, [])
    cache_type = args.cache_backend or (default_backends[0] if default_backends else "postgresql_array")

    print(f"Cache backend: {cache_type}")
    print(f"Scale factor: {args.scale_factor}")
    if args.repeat > 1:
        print(f"Repeat: {args.repeat}")

    try:
        # Validate tables
        if not validate_tables(executor, config):
            sys.exit(1)

        # Compute fact table stats
        fact_stats = compute_fact_table_stats(executor, config)
        max_pk_value = getattr(executor, "max_pk_value", 0)

        # Compute per-partition-key max values for auto bitsize
        pk_max_values = compute_pk_max_values(executor, config)

        # Set up cache manager
        cache_manager = CacheManager(cache_type, config, scale_factor=args.scale_factor, db_path=db_path, max_pk_value=max_pk_value, pk_max_values=pk_max_values)

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

        # Validate spatial mode
        if mode == "spatial" and not config.spatial_queries:
            print(f"Error: Spatial mode not available for {config.name} (no spatial_queries configured).")
            sys.exit(1)

        explain = args.explain
        spatial_method = args.spatial_method

        # Dispatch mode
        if mode == "all":
            results = run_all_queries(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        elif mode == "flight":
            flight_num = int(args.mode[1]) if len(args.mode) > 1 else 1
            results = run_flight(executor, cache_manager, config, flight_num, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        elif mode == "cross-dimension":
            results = run_cross_dimension(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "cold-vs-warm":
            results = run_cold_vs_warm(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        elif mode == "hierarchy":
            results = run_hierarchy(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api)
        elif mode == "spatial":
            results = run_spatial(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats, method=method, api=api, explain=explain, spatial_method=spatial_method)
        elif mode == "spatial-backend-comparison":
            results = run_spatial_backend_comparison(
                executor,
                config,
                scale_factor=args.scale_factor,
                db_path=db_path,
                max_pk_value=max_pk_value,
                pk_max_values=pk_max_values,
                repeat=args.repeat,
                fact_stats=fact_stats,
                method=method,
                api=api,
                explain=explain,
                spatial_method=spatial_method,
            )
        elif mode == "spatial-method-comparison":
            results = run_spatial_method_comparison(
                executor,
                config,
                scale_factor=args.scale_factor,
                db_path=db_path,
                max_pk_value=max_pk_value,
                pk_max_values=pk_max_values,
                repeat=args.repeat,
                fact_stats=fact_stats,
                explain=explain,
            )
        elif mode == "backend-comparison":
            results = run_backend_comparison(
                executor,
                config,
                db_backend=db_backend,
                scale_factor=args.scale_factor,
                db_path=db_path,
                max_pk_value=max_pk_value,
                pk_max_values=pk_max_values,
                repeat=args.repeat,
                fact_stats=fact_stats,
                method=method,
                api=api,
            )
        elif mode == "method-comparison":
            results = run_method_comparison(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats)
        elif mode == "api-comparison":
            results = run_api_comparison(executor, cache_manager, config, repeat=args.repeat, fact_stats=fact_stats, spatial_method=spatial_method)
        else:
            print(f"Unknown mode: {mode}")
            sys.exit(1)

        # Save results
        if args.output:
            output_data = {
                "metadata": {
                    "benchmark": config.name,
                    "mode": mode,
                    "method": method,
                    "api": api,
                    "db_backend": db_backend,
                    "cache_backend": cache_type,
                    "scale_factor": args.scale_factor,
                    "repeat": args.repeat,
                    "spatial_method": spatial_method,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "fact_table_stats": fact_stats,
                },
                "results": [],
            }

            for r in results:
                sr = dict(r)
                for pk in sr.get("apply_stats", {}):
                    if "partition_keys" in sr["apply_stats"][pk]:
                        sr["apply_stats"][pk]["partition_keys"] = list(sr["apply_stats"][pk]["partition_keys"])
                # Exclude full plan text from JSON (too verbose), keep summary fields
                for key in ("baseline_explain", "enhanced_explain"):
                    if key in sr and "plan_text" in sr[key]:
                        sr[key] = {k: v for k, v in sr[key].items() if k != "plan_text"}
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

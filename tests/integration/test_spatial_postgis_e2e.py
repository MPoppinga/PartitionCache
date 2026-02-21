"""
End-to-end integration tests for PostGIS spatial cache handlers (H3 and BBox).

These tests verify the complete spatial cache pipeline:
1. Non-lazy: apply_cache() with geometry_column → get_spatial_filter() → WKB → ST_DWithin
2. Lazy: apply_cache_lazy() with geometry_column → get_spatial_filter_lazy() → SQL subquery → ST_DWithin

Requires:
- Running PostgreSQL with PostGIS and h3-pg extensions
- Pre-populated POI database with spatial data and cache entries
- Environment variables from examples/openstreetmap_poi/.env

These tests are designed to catch issues like:
- SRID mismatches between WKB geometry and ST_GeomFromWKB() calls
- Geography great circle edge distortion with large polygons
- H3 cell ID intersection being too restrictive for multi-table spatial queries
- PostgreSQL aggregate/set-returning function conflicts

Usage:
    # From project root, with POI database running:
    source examples/openstreetmap_poi/.env
    python -m pytest tests/integration/test_spatial_postgis_e2e.py -v
"""

import os
from pathlib import Path

import pytest

try:
    import psycopg2

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

# Path to spatial test queries
SPATIAL_QUERIES_DIR = Path(__file__).parent.parent.parent / "examples" / "openstreetmap_poi" / "testqueries_examples" / "spatial"
ENV_FILE = Path(__file__).parent.parent.parent / "examples" / "openstreetmap_poi" / ".env"


def _load_env_if_needed():
    """Load .env file if DB connection vars are not already set."""
    if os.getenv("DB_NAME") and os.getenv("DB_HOST"):
        return
    if ENV_FILE.exists():
        try:
            from dotenv import load_dotenv

            load_dotenv(ENV_FILE, override=True)
        except ImportError:
            pass


def _get_db_connection():
    """Create a psycopg2 connection to the POI database."""
    _load_env_if_needed()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "osm_poi_db"),
        user=os.getenv("DB_USER", "osmuser"),
        password=os.getenv("DB_PASSWORD", "osmpassword"),
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=os.getenv("DB_PORT", "55432"),
    )


def _skip_if_no_database():
    """Skip tests if database is not reachable."""
    try:
        conn = _get_db_connection()
        conn.close()
    except Exception as e:
        pytest.skip(f"POI database not available: {e}")


def _skip_if_no_queries():
    """Skip tests if spatial query files are not found."""
    if not SPATIAL_QUERIES_DIR.exists():
        pytest.skip(f"Spatial query directory not found: {SPATIAL_QUERIES_DIR}")
    sql_files = sorted(SPATIAL_QUERIES_DIR.glob("*.sql"))
    if not sql_files:
        pytest.skip(f"No .sql files found in {SPATIAL_QUERIES_DIR}")


def _load_queries() -> list[tuple[str, str]]:
    """Load all spatial query files. Returns list of (filename, sql_text)."""
    sql_files = sorted(SPATIAL_QUERIES_DIR.glob("*.sql"))
    queries = []
    for f in sql_files:
        queries.append((f.name, f.read_text().strip()))
    return queries


# Backend configurations
BACKEND_CONFIGS = {
    "postgis_h3": {
        "partition_key": "spatial_h3",
        "geometry_column": "geom",
        "buffer_distance": 500.0,
    },
    "postgis_bbox": {
        "partition_key": "spatial_bbox",
        "geometry_column": "geom",
        "buffer_distance": 500.0,
    },
}

SAMPLE_POIS_SQL = """
    INSERT INTO pois (name, subtype, geom) VALUES
        ('Eis Cafe Milano', 'ice_cream', ST_SetSRID(ST_MakePoint(549000, 5802000), 25832)),
        ('Eis Dolomiti', 'ice_cream', ST_SetSRID(ST_MakePoint(549050, 5802020), 25832)),
        ('Eis am Markt', 'ice_cream', ST_SetSRID(ST_MakePoint(549080, 5802050), 25832)),
        ('Markt Apotheke', 'pharmacy', ST_SetSRID(ST_MakePoint(549100, 5802010), 25832)),
        ('Rats Apotheke', 'pharmacy', ST_SetSRID(ST_MakePoint(549120, 5802080), 25832)),
        ('Stadt Apotheke', 'pharmacy', ST_SetSRID(ST_MakePoint(549060, 5802100), 25832)),
        ('ALDI Nord', 'supermarket', ST_SetSRID(ST_MakePoint(549030, 5802070), 25832)),
        ('Edeka Markt', 'supermarket', ST_SetSRID(ST_MakePoint(549150, 5802030), 25832)),
        ('ALDI Sued', 'supermarket', ST_SetSRID(ST_MakePoint(549070, 5802130), 25832)),
        ('Stadtbad', 'swimming_pool', ST_SetSRID(ST_MakePoint(549110, 5802060), 25832)),
        ('Schwimmbad Am See', 'swimming_pool', ST_SetSRID(ST_MakePoint(549040, 5802110), 25832))
"""


def _seed_pois_if_empty(conn) -> None:
    """Seed sample POI rows when CI starts from an empty pois table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pois")
        row = cur.fetchone()
        if row and row[0] == 0:
            cur.execute(SAMPLE_POIS_SQL)
            conn.commit()


@pytest.fixture(scope="module")
def poi_connection():
    """Module-scoped database connection to the POI database."""
    _skip_if_no_database()
    _skip_if_no_queries()
    conn = _get_db_connection()
    _seed_pois_if_empty(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def spatial_queries():
    """Load spatial test queries."""
    return _load_queries()


def _run_query(conn, sql_query: str) -> list:
    """Execute a query and return all rows."""
    with conn.cursor() as cur:
        cur.execute(sql_query)
        return cur.fetchall()


def _has_cache_entries(backend_name: str, partition_key: str) -> bool:
    """Check if the cache handler has entries by verifying the partition datatype is registered."""
    try:
        handler = partitioncache.get_cache_handler(backend_name, singleton=True)
        # _get_partition_datatype returns None if partition key not registered
        datatype = handler._get_partition_datatype(partition_key)  # noqa: SLF001
        return datatype is not None
    except Exception:
        return False


def _ensure_cache_entries(backend_name: str, partition_key: str, queries: list[tuple[str, str]]) -> bool:
    """Populate cache entries on-demand so spatial E2E can run in CI."""
    if _has_cache_entries(backend_name, partition_key):
        return True

    try:
        handler = partitioncache.get_cache_handler(backend_name, singleton=True)
    except Exception:
        return False

    try:
        handler.register_partition_key(partition_key, "geometry")
    except Exception:
        pass

    for _query_name, sql_query in queries:
        try:
            hash_pairs = generate_all_query_hash_pairs(
                sql_query.strip(),
                partition_key=partition_key,
                min_component_size=1,
                skip_partition_key_joins=True,
                geometry_column="geom",
            )
            for fragment_query, query_hash in hash_pairs:
                handler.set_cache_lazy(query_hash, fragment_query, partition_key)
                handler.set_query(query_hash, fragment_query, partition_key)
        except Exception:
            # If one query fails to prewarm we still try others.
            continue

    return _has_cache_entries(backend_name, partition_key)


@pytest.mark.skipif(not PSYCOPG2_AVAILABLE, reason="psycopg2 not installed")
class TestPostGISSpatialE2E:
    """
    End-to-end tests for PostGIS spatial cache handlers.

    Tests both apply_cache (non-lazy) and apply_cache_lazy (lazy) paths
    for H3 and BBox backends against real spatial queries.

    The key invariant is: enhanced query results >= baseline query results.
    Spatial filtering is a superset operation — it can only ADD the constraint
    that geometry must be within the cache's spatial filter region, which
    should include all baseline results (assuming cache was populated from
    the same or broader queries).
    """

    @pytest.fixture(autouse=True)
    def setup(self, poi_connection, spatial_queries):
        self.conn = poi_connection
        self.queries = spatial_queries

    @pytest.mark.parametrize("backend_name", ["postgis_h3", "postgis_bbox"])
    def test_apply_cache_spatial(self, backend_name):
        """Test non-lazy apply_cache() with spatial filter for all queries."""
        config = BACKEND_CONFIGS[backend_name]

        _ensure_cache_entries(backend_name, config["partition_key"], self.queries)
        if not _has_cache_entries(backend_name, config["partition_key"]):
            pytest.skip(f"No cache entries for {backend_name} partition key '{config['partition_key']}'")

        handler = partitioncache.get_cache_handler(backend_name, singleton=True)

        for query_name, sql_query in self.queries:
            # Baseline: run original query
            rows_baseline = _run_query(self.conn, sql_query)

            # Non-lazy: apply_cache() with spatial params
            enhanced_query, stats = partitioncache.apply_cache(
                query=sql_query,
                cache_handler=handler,
                partition_key=config["partition_key"],
                geometry_column=config["geometry_column"],
                buffer_distance=config["buffer_distance"],
                min_component_size=1,
            )

            if stats["enhanced"]:
                rows_enhanced = _run_query(self.conn, enhanced_query)
                assert len(rows_enhanced) >= len(rows_baseline), (
                    f"[{backend_name}] {query_name}: apply_cache returned fewer rows "
                    f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                    f"Stats: {stats}"
                )
                print(
                    f"  [{backend_name}] {query_name} apply_cache: "
                    f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                    f"(hits={stats['cache_hits']}, variants={stats['generated_variants']})"
                )
            else:
                print(
                    f"  [{backend_name}] {query_name} apply_cache: not enhanced "
                    f"(hits={stats['cache_hits']}, variants={stats['generated_variants']})"
                )

    @pytest.mark.parametrize("backend_name", ["postgis_h3", "postgis_bbox"])
    def test_apply_cache_lazy_spatial(self, backend_name):
        """Test lazy apply_cache_lazy() with spatial filter for all queries."""
        config = BACKEND_CONFIGS[backend_name]

        _ensure_cache_entries(backend_name, config["partition_key"], self.queries)
        if not _has_cache_entries(backend_name, config["partition_key"]):
            pytest.skip(f"No cache entries for {backend_name} partition key '{config['partition_key']}'")

        handler = partitioncache.get_cache_handler(backend_name, singleton=True)

        for query_name, sql_query in self.queries:
            # Baseline
            rows_baseline = _run_query(self.conn, sql_query)

            # Lazy: apply_cache_lazy() with spatial params
            enhanced_query, stats = partitioncache.apply_cache_lazy(
                query=sql_query,
                cache_handler=handler,
                partition_key=config["partition_key"],
                geometry_column=config["geometry_column"],
                buffer_distance=config["buffer_distance"],
                method="TMP_TABLE_IN",
                min_component_size=1,
            )

            if stats["enhanced"]:
                rows_enhanced = _run_query(self.conn, enhanced_query)
                assert len(rows_enhanced) >= len(rows_baseline), (
                    f"[{backend_name}] {query_name}: apply_cache_lazy returned fewer rows "
                    f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                    f"Stats: {stats}"
                )
                print(
                    f"  [{backend_name}] {query_name} apply_cache_lazy: "
                    f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                    f"(hits={stats['cache_hits']}, variants={stats['generated_variants']})"
                )
            else:
                print(
                    f"  [{backend_name}] {query_name} apply_cache_lazy: not enhanced "
                    f"(hits={stats['cache_hits']}, variants={stats['generated_variants']})"
                )

    @pytest.mark.parametrize("backend_name", ["postgis_h3", "postgis_bbox"])
    def test_lazy_and_nonlazy_consistency(self, backend_name):
        """Verify that lazy and non-lazy paths return the same result count."""
        config = BACKEND_CONFIGS[backend_name]

        _ensure_cache_entries(backend_name, config["partition_key"], self.queries)
        if not _has_cache_entries(backend_name, config["partition_key"]):
            pytest.skip(f"No cache entries for {backend_name} partition key '{config['partition_key']}'")

        handler = partitioncache.get_cache_handler(backend_name, singleton=True)

        for query_name, sql_query in self.queries:
            # Non-lazy
            enhanced_nonlazy, stats_nonlazy = partitioncache.apply_cache(
                query=sql_query,
                cache_handler=handler,
                partition_key=config["partition_key"],
                geometry_column=config["geometry_column"],
                buffer_distance=config["buffer_distance"],
                min_component_size=1,
            )

            # Lazy
            enhanced_lazy, stats_lazy = partitioncache.apply_cache_lazy(
                query=sql_query,
                cache_handler=handler,
                partition_key=config["partition_key"],
                geometry_column=config["geometry_column"],
                buffer_distance=config["buffer_distance"],
                method="TMP_TABLE_IN",
                min_component_size=1,
            )

            # Both should have same enhancement status
            assert stats_nonlazy["enhanced"] == stats_lazy["enhanced"], (
                f"[{backend_name}] {query_name}: enhancement status differs "
                f"(nonlazy={stats_nonlazy['enhanced']}, lazy={stats_lazy['enhanced']})"
            )

            if stats_nonlazy["enhanced"] and stats_lazy["enhanced"]:
                rows_nonlazy = _run_query(self.conn, enhanced_nonlazy)
                rows_lazy = _run_query(self.conn, enhanced_lazy)
                assert len(rows_nonlazy) == len(rows_lazy), (
                    f"[{backend_name}] {query_name}: result count differs "
                    f"(nonlazy={len(rows_nonlazy)}, lazy={len(rows_lazy)})"
                )
                print(
                    f"  [{backend_name}] {query_name} consistency: "
                    f"nonlazy={len(rows_nonlazy)}, lazy={len(rows_lazy)} (match)"
                )

    @pytest.mark.parametrize("backend_name", ["postgis_h3", "postgis_bbox"])
    def test_spatial_filter_returns_valid_wkb(self, backend_name):
        """Test that get_spatial_filter() returns valid WKB with correct SRID."""
        config = BACKEND_CONFIGS[backend_name]

        _ensure_cache_entries(backend_name, config["partition_key"], self.queries)
        if not _has_cache_entries(backend_name, config["partition_key"]):
            pytest.skip(f"No cache entries for {backend_name} partition key '{config['partition_key']}'")

        handler = partitioncache.get_cache_handler(backend_name, singleton=True)

        for query_name, sql_query in self.queries:
            # Generate hashes
            from partitioncache.query_processor import generate_all_hashes

            hashes = generate_all_hashes(
                sql_query,
                partition_key=config["partition_key"],
                min_component_size=1,
                skip_partition_key_joins=True,
                geometry_column=config["geometry_column"],
            )

            if not hashes:
                continue

            result = handler.get_spatial_filter(
                keys=set(hashes),
                partition_key=config["partition_key"],
                buffer_distance=config["buffer_distance"],
            )

            if result is not None:
                wkb, srid = result
                assert isinstance(wkb, bytes), f"WKB should be bytes, got {type(wkb)}"
                assert len(wkb) > 0, "WKB should not be empty"
                assert isinstance(srid, int), f"SRID should be int, got {type(srid)}"
                assert srid > 0, f"SRID should be positive, got {srid}"

                # Verify WKB is valid by loading it in PostGIS
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT ST_IsValid(ST_GeomFromWKB(%s, %s)), ST_SRID(ST_GeomFromWKB(%s, %s))",
                        (psycopg2.Binary(wkb), srid, psycopg2.Binary(wkb), srid),
                    )
                    row = cur.fetchone()
                    assert row is not None
                    is_valid, returned_srid = row
                    # ST_IsValid may be None for empty geometries
                    if is_valid is not None:
                        assert is_valid, f"WKB geometry is not valid for {backend_name}/{query_name}"
                    assert returned_srid == srid, (
                        f"SRID mismatch: returned {returned_srid}, expected {srid} for {backend_name}/{query_name}"
                    )

                print(f"  [{backend_name}] {query_name}: valid WKB ({len(wkb)} bytes, SRID={srid})")

    @pytest.mark.parametrize("backend_name", ["postgis_h3", "postgis_bbox"])
    def test_apply_cache_auto_buffer_distance(self, backend_name):
        """Test that buffer_distance=None auto-derives from ST_DWithin distances.

        Compares results from auto-derived buffer against the explicit buffer baseline.
        The auto-derived buffer should produce results that are a superset of baseline.
        """
        config = BACKEND_CONFIGS[backend_name]

        _ensure_cache_entries(backend_name, config["partition_key"], self.queries)
        if not _has_cache_entries(backend_name, config["partition_key"]):
            pytest.skip(f"No cache entries for {backend_name} partition key '{config['partition_key']}'")

        handler = partitioncache.get_cache_handler(backend_name, singleton=True)

        for query_name, sql_query in self.queries:
            # Baseline: run original query
            rows_baseline = _run_query(self.conn, sql_query)

            # Auto-derive buffer_distance by passing None
            enhanced_query, stats = partitioncache.apply_cache(
                query=sql_query,
                cache_handler=handler,
                partition_key=config["partition_key"],
                geometry_column=config["geometry_column"],
                buffer_distance=None,  # Auto-derive from ST_DWithin
                min_component_size=1,
            )

            if stats["enhanced"]:
                rows_enhanced = _run_query(self.conn, enhanced_query)
                assert len(rows_enhanced) >= len(rows_baseline), (
                    f"[{backend_name}] {query_name}: auto-buffer apply_cache returned fewer rows "
                    f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                    f"Stats: {stats}"
                )
                print(
                    f"  [{backend_name}] {query_name} auto-buffer apply_cache: "
                    f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                    f"(hits={stats['cache_hits']}, variants={stats['generated_variants']})"
                )
            else:
                print(
                    f"  [{backend_name}] {query_name} auto-buffer apply_cache: not enhanced "
                    f"(hits={stats['cache_hits']}, variants={stats['generated_variants']})"
                )

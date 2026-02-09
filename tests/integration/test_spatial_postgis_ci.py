"""
Self-contained PostGIS spatial cache integration tests for CI.

These tests seed their own spatial data (pois table with PostGIS geometry)
inside the CI database and test the full spatial cache pipeline for both
postgis_h3 and postgis_bbox backends.

Unlike test_spatial_postgis_e2e.py (which requires an external OSM POI database),
these tests are fully self-contained and work in CI with the PostGIS-enabled
Docker image.

Requires:
- PostgreSQL with PostGIS and h3-pg extensions
- Empty pois table (schema created by init-pg-cron.sql / create_clean_test_database.py)
- Standard DB_* environment variables

Usage:
    python -m pytest tests/integration/test_spatial_postgis_ci.py -v
"""

import os

import pytest

import partitioncache
from partitioncache.query_processor import generate_all_query_hash_pairs

# --------------------------------------------------------------------------- #
#  Skip conditions
# --------------------------------------------------------------------------- #

try:
    import psycopg

    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False


def _get_conn_str() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "integration_user")
    password = os.getenv("DB_PASSWORD", "integration_password")
    dbname = os.getenv("DB_NAME", "")
    if not dbname:
        pytest.skip("DB_NAME not set")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def _check_postgis_available(conn) -> bool:
    """Return True if PostGIS extension is available."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        return cur.fetchone() is not None


def _check_h3_available(conn) -> bool:
    """Return True if h3 extension is available."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'h3'")
        return cur.fetchone() is not None


def _check_pois_table_exists(conn) -> bool:
    """Return True if pois table exists (may be empty)."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'pois'")
        row = cur.fetchone()
        return row is not None and row[0] > 0


# --------------------------------------------------------------------------- #
#  Sample POI data — seeded by the test fixture, not by init scripts
# --------------------------------------------------------------------------- #

# ~11 points near Hannover city center in SRID 25832 (ETRS89 / UTM zone 32N).
# Base coordinates: ~549000 E, ~5802000 N (approx 52.37 N 9.74 E in WGS84).
# Points spread within ~200m — different subtypes at non-overlapping positions.
# BBox spatial filter buffering (buffer_distance + cell_size) ensures the
# envelope intersection is non-empty despite the spread.
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


# --------------------------------------------------------------------------- #
#  Test queries — same 3-way self-join pattern as the OSM spatial queries
# --------------------------------------------------------------------------- #

SPATIAL_QUERIES = [
    (
        "q1_ice_pharmacy_aldi",
        """
        SELECT p1.name AS ice_cream_name,
               p2.name AS pharmacy_name,
               p3.name AS supermarket_name
        FROM pois AS p1,
             pois AS p2,
             pois AS p3
        WHERE p1.name LIKE '%Eis %'
          AND p2.subtype = 'pharmacy'
          AND p3.subtype = 'supermarket'
          AND p3.name LIKE '%ALDI%'
          AND ST_DWithin(p1.geom, p3.geom, 300)
          AND ST_DWithin(p1.geom, p2.geom, 400)
        """,
    ),
    (
        "q2_ice_pool_edeka",
        """
        SELECT p1.name AS ice_cream_name,
               p2.name AS swimming_pool,
               p3.name AS supermarket_name
        FROM pois AS p1,
             pois AS p2,
             pois AS p3
        WHERE p1.name LIKE '%Eis %'
          AND p2.subtype = 'swimming_pool'
          AND p3.subtype = 'supermarket'
          AND p3.name ILIKE '%Edeka%'
          AND ST_DWithin(p1.geom, p3.geom, 400)
          AND ST_DWithin(p1.geom, p2.geom, 400)
        """,
    ),
    (
        "q3_ice_pool_aldi",
        """
        SELECT p1.name AS ice_cream_name,
               p2.name AS swimming_pool,
               p3.name AS supermarket_name
        FROM pois AS p1,
             pois AS p2,
             pois AS p3
        WHERE p1.name LIKE '%Eis %'
          AND p2.subtype = 'swimming_pool'
          AND p3.subtype = 'supermarket'
          AND p3.name ILIKE '%ALDI%'
          AND ST_DWithin(p1.geom, p3.geom, 300)
          AND ST_DWithin(p1.geom, p2.geom, 400)
        """,
    ),
]

# --------------------------------------------------------------------------- #
#  Backend configurations
# --------------------------------------------------------------------------- #

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


# --------------------------------------------------------------------------- #
#  Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def ci_db_connection():
    """Module-scoped database connection that seeds pois data for the test session.

    Follows the same pattern as conftest.py fixtures for other test tables:
    the test owns its data rather than relying on init scripts.
    """
    if not PSYCOPG_AVAILABLE:
        pytest.skip("psycopg not installed")

    conn_str = _get_conn_str()
    try:
        conn = psycopg.connect(conn_str, autocommit=True)
    except Exception as e:
        pytest.skip(f"Cannot connect to database: {e}")

    if not _check_postgis_available(conn):
        conn.close()
        pytest.skip("PostGIS extension not available")

    if not _check_pois_table_exists(conn):
        conn.close()
        pytest.skip("pois table not found (schema should be created by init script)")

    # Seed pois data — truncate first for idempotency
    with conn.cursor() as cur:
        cur.execute("TRUNCATE pois RESTART IDENTITY")
        cur.execute(SAMPLE_POIS_SQL)

    yield conn

    # Clean up: leave table empty for the next test run
    with conn.cursor() as cur:
        cur.execute("TRUNCATE pois RESTART IDENTITY")
    conn.close()


@pytest.fixture(scope="module")
def h3_available(ci_db_connection):
    """Check if h3 extension is available (needed for postgis_h3 backend)."""
    return _check_h3_available(ci_db_connection)


def _populate_cache(handler, partition_key: str, geometry_column: str):
    """Populate cache for all test queries using set_cache_lazy."""
    handler.register_partition_key(partition_key, "geometry")

    for _query_name, sql_query in SPATIAL_QUERIES:
        hash_pairs = generate_all_query_hash_pairs(
            sql_query.strip(),
            partition_key=partition_key,
            min_component_size=1,
            skip_partition_key_joins=True,
            geometry_column=geometry_column,
        )
        for fragment_query, query_hash in hash_pairs:
            handler.set_cache_lazy(query_hash, fragment_query, partition_key)
            handler.set_query(query_hash, fragment_query, partition_key)


def _cleanup_cache_tables(handler, partition_key: str):
    """Drop cache tables created during tests."""
    try:
        table_name = f"{handler.tableprefix}_cache_{partition_key}"
        queries_table = f"{handler.tableprefix}_queries"
        metadata_table = f"{handler.tableprefix}_partition_metadata"
        handler.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")  # noqa: S608
        handler.cursor.execute(f"DROP TABLE IF EXISTS {queries_table} CASCADE")  # noqa: S608
        handler.cursor.execute(f"DROP TABLE IF EXISTS {metadata_table} CASCADE")  # noqa: S608
        handler.db.commit()
    except Exception:
        try:
            handler.db.rollback()
        except Exception:
            pass


def _run_query(conn, sql_query: str) -> list:
    """Execute a query and return all rows."""
    with conn.cursor() as cur:
        cur.execute(sql_query)
        return cur.fetchall()


def _run_multi_statement(conn, sql_text: str) -> list:
    """Execute potentially multi-statement SQL (e.g. TMP_TABLE_IN with CREATE + SELECT).

    Returns results from the last SELECT statement.
    """
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    results: list = []
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
            if stmt.upper().lstrip().startswith("SELECT"):
                results = cur.fetchall()
    return results


# --------------------------------------------------------------------------- #
#  Tests
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(not PSYCOPG_AVAILABLE, reason="psycopg not installed")
class TestSpatialPostGISCI:
    """Self-contained PostGIS spatial cache tests for CI."""

    # ---- H3 backend tests ---- #

    def test_postgis_h3_cache_populate_and_apply(self, ci_db_connection, h3_available):
        """Full H3 pipeline: populate cache → apply_cache → verify results."""
        if not h3_available:
            pytest.skip("h3 extension not available")

        backend_name = "postgis_h3"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()
                rows_baseline = _run_query(ci_db_connection, sql_query)

                enhanced_query, stats = partitioncache.apply_cache(
                    query=sql_query,
                    cache_handler=handler,
                    partition_key=config["partition_key"],
                    geometry_column=config["geometry_column"],
                    buffer_distance=config["buffer_distance"],
                    min_component_size=1,
                )

                if stats["enhanced"]:
                    rows_enhanced = _run_multi_statement(ci_db_connection, enhanced_query)
                    assert len(rows_enhanced) >= len(rows_baseline), (
                        f"[{backend_name}] {query_name}: apply_cache returned fewer rows "
                        f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                        f"Stats: {stats}"
                    )
                    print(
                        f"  [{backend_name}] {query_name}: "
                        f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                        f"(hits={stats['cache_hits']})"
                    )
                else:
                    print(f"  [{backend_name}] {query_name}: not enhanced (stats={stats})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    def test_postgis_h3_lazy_apply(self, ci_db_connection, h3_available):
        """H3 lazy pipeline: populate cache → apply_cache_lazy → verify results."""
        if not h3_available:
            pytest.skip("h3 extension not available")

        backend_name = "postgis_h3"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()
                rows_baseline = _run_query(ci_db_connection, sql_query)

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
                    rows_enhanced = _run_multi_statement(ci_db_connection, enhanced_query)
                    assert len(rows_enhanced) >= len(rows_baseline), (
                        f"[{backend_name}] {query_name}: apply_cache_lazy returned fewer rows "
                        f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                        f"Stats: {stats}"
                    )
                    print(
                        f"  [{backend_name}] {query_name} lazy: "
                        f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                        f"(hits={stats['cache_hits']})"
                    )
                else:
                    print(f"  [{backend_name}] {query_name} lazy: not enhanced (stats={stats})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    # ---- BBox backend tests ---- #

    def test_postgis_bbox_cache_populate_and_apply(self, ci_db_connection):
        """Full BBox pipeline: populate cache → apply_cache → verify results."""
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()
                rows_baseline = _run_query(ci_db_connection, sql_query)

                enhanced_query, stats = partitioncache.apply_cache(
                    query=sql_query,
                    cache_handler=handler,
                    partition_key=config["partition_key"],
                    geometry_column=config["geometry_column"],
                    buffer_distance=config["buffer_distance"],
                    min_component_size=1,
                )

                if stats["enhanced"]:
                    rows_enhanced = _run_multi_statement(ci_db_connection, enhanced_query)
                    assert len(rows_enhanced) >= len(rows_baseline), (
                        f"[{backend_name}] {query_name}: apply_cache returned fewer rows "
                        f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                        f"Stats: {stats}"
                    )
                    print(
                        f"  [{backend_name}] {query_name}: "
                        f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                        f"(hits={stats['cache_hits']})"
                    )
                else:
                    print(f"  [{backend_name}] {query_name}: not enhanced (stats={stats})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    def test_postgis_bbox_lazy_apply(self, ci_db_connection):
        """BBox lazy pipeline: populate cache → apply_cache_lazy → verify results."""
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()
                rows_baseline = _run_query(ci_db_connection, sql_query)

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
                    rows_enhanced = _run_multi_statement(ci_db_connection, enhanced_query)
                    assert len(rows_enhanced) >= len(rows_baseline), (
                        f"[{backend_name}] {query_name}: apply_cache_lazy returned fewer rows "
                        f"({len(rows_enhanced)}) than baseline ({len(rows_baseline)}). "
                        f"Stats: {stats}"
                    )
                    print(
                        f"  [{backend_name}] {query_name} lazy: "
                        f"{len(rows_enhanced)}/{len(rows_baseline)} rows "
                        f"(hits={stats['cache_hits']})"
                    )
                else:
                    print(f"  [{backend_name}] {query_name} lazy: not enhanced (stats={stats})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    # ---- Cross-backend tests ---- #

    def test_spatial_filter_wkb_validity_bbox(self, ci_db_connection):
        """Verify BBox get_spatial_filter() returns valid WKB with correct SRID."""
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()
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

                    # Verify WKB is valid geometry in PostGIS
                    with ci_db_connection.cursor() as cur:
                        cur.execute(
                            "SELECT ST_IsValid(ST_GeomFromWKB(%s, %s)), ST_SRID(ST_GeomFromWKB(%s, %s))",
                            (wkb, srid, wkb, srid),
                        )
                        row = cur.fetchone()
                        assert row is not None
                        is_valid, returned_srid = row
                        if is_valid is not None:
                            assert is_valid, f"WKB geometry not valid for {backend_name}/{query_name}"
                        assert returned_srid == srid, (
                            f"SRID mismatch: returned {returned_srid}, expected {srid}"
                        )
                    print(f"  [{backend_name}] {query_name}: valid WKB ({len(wkb)} bytes, SRID={srid})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    def test_spatial_filter_wkb_validity_h3(self, ci_db_connection, h3_available):
        """Verify H3 get_spatial_filter() returns valid WKB with correct SRID."""
        if not h3_available:
            pytest.skip("h3 extension not available")

        backend_name = "postgis_h3"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()
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

                    with ci_db_connection.cursor() as cur:
                        cur.execute(
                            "SELECT ST_IsValid(ST_GeomFromWKB(%s, %s)), ST_SRID(ST_GeomFromWKB(%s, %s))",
                            (wkb, srid, wkb, srid),
                        )
                        row = cur.fetchone()
                        assert row is not None
                        is_valid, returned_srid = row
                        if is_valid is not None:
                            assert is_valid, f"WKB geometry not valid for {backend_name}/{query_name}"
                        assert returned_srid == srid, (
                            f"SRID mismatch: returned {returned_srid}, expected {srid}"
                        )
                    print(f"  [{backend_name}] {query_name}: valid WKB ({len(wkb)} bytes, SRID={srid})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    def test_lazy_nonlazy_consistency_bbox(self, ci_db_connection):
        """Verify lazy and non-lazy BBox paths return consistent result counts."""
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            for query_name, sql_query in SPATIAL_QUERIES:
                sql_query = sql_query.strip()

                enhanced_nonlazy, stats_nonlazy = partitioncache.apply_cache(
                    query=sql_query,
                    cache_handler=handler,
                    partition_key=config["partition_key"],
                    geometry_column=config["geometry_column"],
                    buffer_distance=config["buffer_distance"],
                    min_component_size=1,
                )

                enhanced_lazy, stats_lazy = partitioncache.apply_cache_lazy(
                    query=sql_query,
                    cache_handler=handler,
                    partition_key=config["partition_key"],
                    geometry_column=config["geometry_column"],
                    buffer_distance=config["buffer_distance"],
                    method="TMP_TABLE_IN",
                    min_component_size=1,
                )

                assert stats_nonlazy["enhanced"] == stats_lazy["enhanced"], (
                    f"[{backend_name}] {query_name}: enhancement status differs "
                    f"(nonlazy={stats_nonlazy['enhanced']}, lazy={stats_lazy['enhanced']})"
                )

                if stats_nonlazy["enhanced"] and stats_lazy["enhanced"]:
                    rows_nonlazy = _run_multi_statement(ci_db_connection, enhanced_nonlazy)
                    rows_lazy = _run_multi_statement(ci_db_connection, enhanced_lazy)
                    assert len(rows_nonlazy) == len(rows_lazy), (
                        f"[{backend_name}] {query_name}: result count differs "
                        f"(nonlazy={len(rows_nonlazy)}, lazy={len(rows_lazy)})"
                    )
                    print(
                        f"  [{backend_name}] {query_name} consistency: "
                        f"nonlazy={len(rows_nonlazy)}, lazy={len(rows_lazy)} (match)"
                    )

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()


@pytest.mark.skipif(not PSYCOPG_AVAILABLE, reason="psycopg not installed")
class TestSpatialBBoxBufferRegression:
    """Regression tests for the BBox envelope buffering fix.

    The BBox handler must buffer each envelope by ``buffer_distance + cell_size``
    before intersecting them.  Without buffering, fragments whose envelopes do
    not physically overlap produce an empty spatial filter geometry, silently
    dropping all query results.

    The H3 handler already applied this fix (buffering by one H3 edge length);
    these tests ensure BBox has parity.
    """

    def test_bbox_nonoverlapping_fragments_within_buffer(self, ci_db_connection):
        """Key regression: non-overlapping envelopes within buffer_distance must produce a non-empty filter.

        The pois table has ice cream shops around y=5802000-5802050 and ALDI
        supermarkets around y=5802070-5802130.  Their BBox envelopes do NOT
        overlap (~20 m gap).  With buffer_distance=500 m the spatial filter
        must still be non-empty because 20 m < 500 m.
        """
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            from partitioncache.query_processor import generate_all_hashes

            # Use q1 which joins ice cream + pharmacy + ALDI
            sql_query = SPATIAL_QUERIES[0][1].strip()
            hashes = generate_all_hashes(
                sql_query,
                partition_key=config["partition_key"],
                min_component_size=1,
                skip_partition_key_joins=True,
                geometry_column=config["geometry_column"],
            )

            assert hashes, "Expected query to produce hashes"

            result = handler.get_spatial_filter(
                keys=set(hashes),
                partition_key=config["partition_key"],
                buffer_distance=config["buffer_distance"],  # 500 m
            )

            assert result is not None, (
                "Spatial filter must not be None — fragments are within buffer_distance. "
                "If this fails, the BBox handler is intersecting raw envelopes without buffering."
            )

            wkb, srid = result
            # Verify geometry is non-empty
            with ci_db_connection.cursor() as cur:
                cur.execute(
                    "SELECT ST_IsEmpty(ST_GeomFromWKB(%s, %s))",
                    (wkb, srid),
                )
                row = cur.fetchone()
                assert row is not None
                assert not row[0], (
                    "Spatial filter geometry must not be empty — fragments are within buffer_distance. "
                    "This regression means envelopes were intersected without buffering."
                )

            print(f"  PASS: non-overlapping fragments produced non-empty filter ({len(wkb)} bytes, SRID={srid})")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    def test_bbox_filter_lazy_matches_nonlazy_with_spread_data(self, ci_db_connection):
        """Lazy and non-lazy spatial filters must agree even with spread-out data."""
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            from partitioncache.query_processor import generate_all_hashes

            sql_query = SPATIAL_QUERIES[0][1].strip()
            hashes = generate_all_hashes(
                sql_query,
                partition_key=config["partition_key"],
                min_component_size=1,
                skip_partition_key_joins=True,
                geometry_column=config["geometry_column"],
            )

            # Non-lazy filter
            result_nonlazy = handler.get_spatial_filter(
                keys=set(hashes),
                partition_key=config["partition_key"],
                buffer_distance=config["buffer_distance"],
            )

            # Lazy filter
            lazy_sql = handler.get_spatial_filter_lazy(
                keys=set(hashes),
                partition_key=config["partition_key"],
                buffer_distance=config["buffer_distance"],
            )

            assert (result_nonlazy is None) == (lazy_sql is None), (
                "Lazy and non-lazy should agree on whether a filter exists"
            )

            if result_nonlazy is not None and lazy_sql is not None:
                wkb_nonlazy, srid = result_nonlazy
                # Execute lazy SQL to get WKB for comparison
                with ci_db_connection.cursor() as cur:
                    cur.execute(f"SELECT ST_AsBinary(({lazy_sql})::geometry)")  # noqa: S608
                    row = cur.fetchone()
                    assert row is not None and row[0] is not None
                    wkb_lazy = bytes(row[0])

                # Both should be non-empty
                with ci_db_connection.cursor() as cur:
                    cur.execute(
                        "SELECT ST_IsEmpty(ST_GeomFromWKB(%s, %s)), ST_IsEmpty(ST_GeomFromWKB(%s, %s))",
                        (wkb_nonlazy, srid, wkb_lazy, srid),
                    )
                    row = cur.fetchone()
                    assert row is not None
                    assert not row[0], "Non-lazy filter is empty"
                    assert not row[1], "Lazy filter is empty"

                print("  PASS: lazy and non-lazy filters both non-empty and consistent")

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

    def test_bbox_enhanced_query_returns_results_with_spread_data(self, ci_db_connection):
        """Enhanced query must return >= baseline rows even with spread-out POI data.

        This is the key end-to-end regression test. With the original bug (no
        buffering), the enhanced query returned 0 rows while baseline returned 18+.
        """
        backend_name = "postgis_bbox"
        config = BACKEND_CONFIGS[backend_name]

        handler = partitioncache.get_cache_handler(backend_name, singleton=False)

        try:
            _populate_cache(handler, config["partition_key"], config["geometry_column"])

            sql_query = SPATIAL_QUERIES[0][1].strip()
            rows_baseline = _run_query(ci_db_connection, sql_query)
            assert len(rows_baseline) > 0, "Baseline query should return results with spread-out POIs"

            enhanced_query, stats = partitioncache.apply_cache(
                query=sql_query,
                cache_handler=handler,
                partition_key=config["partition_key"],
                geometry_column=config["geometry_column"],
                buffer_distance=config["buffer_distance"],
                min_component_size=1,
            )

            assert stats["enhanced"], "Query should be enhanced (cache was populated)"

            rows_enhanced = _run_multi_statement(ci_db_connection, enhanced_query)
            assert len(rows_enhanced) >= len(rows_baseline), (
                f"Enhanced query returned fewer rows ({len(rows_enhanced)}) than baseline "
                f"({len(rows_baseline)}). This regression means the BBox spatial filter "
                f"is too restrictive — envelopes were likely intersected without buffering."
            )
            print(
                f"  PASS: enhanced={len(rows_enhanced)} >= baseline={len(rows_baseline)} "
                f"(hits={stats['cache_hits']})"
            )

        finally:
            _cleanup_cache_tables(handler, config["partition_key"])
            handler.close()

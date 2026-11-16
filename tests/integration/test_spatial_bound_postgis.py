"""PostGIS tests of the spatial buffer bound: fragments over tables not bounded in distance to p0 must not restrict it.

The derived buffer bounds only tables linked to p0 by top-level distance conjuncts. A fragment containing another
table (joined to p0 only on an attribute, or through ``NOT ST_DWithin``) can lie arbitrarily far from p0's rows; the
data below puts such matches 10 km away, so restricting p0 with them would lose every row. The lookup must then use
only the fragments over bounded tables (fewer than are cached) or run without the cache.

Each case populates the cache through the library's spatial population path (the query's variants from
``generate_all_query_hash_pairs``, stored with ``set_cache_lazy``, as ``pcache-add --direct`` does) and checks that the
rewritten query returns exactly the rows of the original query.

Requires PostgreSQL with PostGIS and the standard DB_* / PG_BBOX_* environment variables. The H3 handler
(``rocksdict_h3_grouped``) is not covered: it needs the Python ``h3`` library, which the CI environment does not install.
"""

import os

import pytest

import partitioncache
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs

try:
    import psycopg

    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False

TABLE = "spatial_bound_points"
PARTITION_KEY = "spatial_bound_bbox"
GEOMETRY_COLUMN = "geom"

# SRID 25832 (metres). Cafes and bakeries within 100 m of each other near (549000, 5802000), a bank within 150 m of
# the cafes, and museums named like the cafes 10 km to the east: matches of a museum alias are far from every cafe.
SAMPLE_ROWS_SQL = f"""
    INSERT INTO {TABLE} (name, subtype, geom) VALUES
        ('Aurora', 'cafe', ST_SetSRID(ST_MakePoint(549000, 5802000), 25832)),
        ('Borealis', 'cafe', ST_SetSRID(ST_MakePoint(549040, 5802030), 25832)),
        ('Corona', 'cafe', ST_SetSRID(ST_MakePoint(549500, 5802500), 25832)),
        ('Kornfeld', 'bakery', ST_SetSRID(ST_MakePoint(549050, 5802010), 25832)),
        ('Sauerteig', 'bakery', ST_SetSRID(ST_MakePoint(549020, 5802060), 25832)),
        ('Kasse', 'bank', ST_SetSRID(ST_MakePoint(549100, 5802050), 25832)),
        ('Aurora', 'museum', ST_SetSRID(ST_MakePoint(559000, 5802000), 25832)),
        ('Borealis', 'museum', ST_SetSRID(ST_MakePoint(559040, 5802030), 25832)),
        ('Corona', 'museum', ST_SetSRID(ST_MakePoint(549520, 5802510), 25832))
"""

# p3 is tied to p1 only by the name: the derived buffer (100) does not bound it
ATTRIBUTE_TIED = f"""
    SELECT p1.name AS cafe, p2.name AS bakery, p3.name AS museum
    FROM {TABLE} AS p1, {TABLE} AS p2, {TABLE} AS p3
    WHERE p1.subtype = 'cafe' AND p2.subtype = 'bakery' AND p3.subtype = 'museum'
      AND ST_DWithin(p1.geom, p2.geom, 100)
      AND p3.name = p1.name
"""

# A negated distance bounds nothing: s can be anywhere
NEGATED = f"""
    SELECT p1.name AS cafe, s.name AS museum
    FROM {TABLE} AS p1, {TABLE} AS s
    WHERE p1.subtype = 'cafe' AND s.subtype = 'museum'
      AND NOT ST_DWithin(p1.geom, s.geom, 100)
"""

# Control: every table is linked to p1 by a distance
FULLY_LINKED = f"""
    SELECT p1.name AS cafe, p2.name AS bakery, p3.name AS bank
    FROM {TABLE} AS p1, {TABLE} AS p2, {TABLE} AS p3
    WHERE p1.subtype = 'cafe' AND p2.subtype = 'bakery' AND p3.subtype = 'bank'
      AND ST_DWithin(p1.geom, p2.geom, 100)
      AND ST_DWithin(p1.geom, p3.geom, 150)
"""


def _conn_str() -> str:
    dbname = os.getenv("DB_NAME", "")
    if not dbname:
        pytest.skip("DB_NAME not set")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "integration_user")
    password = os.getenv("DB_PASSWORD", "integration_password")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


@pytest.fixture(scope="module")
def spatial_db():
    """A psycopg 3 connection with a seeded point table of this module (dropped at the end)."""
    if not PSYCOPG_AVAILABLE:
        pytest.skip("psycopg not installed")
    try:
        conn = psycopg.connect(_conn_str(), autocommit=True)
    except Exception as e:
        pytest.skip(f"Cannot connect to database: {e}")
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        if cur.fetchone() is None:
            conn.close()
            pytest.skip("PostGIS extension not available")
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(f"CREATE TABLE {TABLE} (id SERIAL PRIMARY KEY, name TEXT NOT NULL, subtype TEXT NOT NULL, geom geometry(Point, 25832) NOT NULL)")
        cur.execute(f"CREATE INDEX ON {TABLE} USING GIST (geom)")
        cur.execute(SAMPLE_ROWS_SQL)
    yield conn
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.close()


@pytest.fixture
def bbox_handler(spatial_db):
    handler = partitioncache.get_cache_handler("postgis_bbox", singleton=False)
    handler.delete_partition(PARTITION_KEY)  # a partition left over by an aborted run
    handler.register_partition_key(PARTITION_KEY, "geometry")
    yield handler
    handler.delete_partition(PARTITION_KEY)
    handler.close()


def _populate(handler, query: str, min_component_size: int) -> None:
    """Store every variant of the query as ``pcache-add --direct`` does for a spatial key."""
    pairs = generate_all_query_hash_pairs(
        query.strip(), PARTITION_KEY, min_component_size=min_component_size, skip_partition_key_joins=True, geometry_column=GEOMETRY_COLUMN
    )
    assert pairs
    for variant, variant_hash in pairs:
        assert handler.set_cache_lazy(variant_hash, variant, PARTITION_KEY), variant
        handler.set_query(variant_hash, variant, PARTITION_KEY)


def _cached_lookup_variants(handler, query: str, min_component_size: int) -> int:
    """How many of the variants the spatial lookup probes are cached (arguments as in apply_cache's spatial path)."""
    hashes = generate_all_hashes(
        query.strip(), PARTITION_KEY, min_component_size=min_component_size, fix_attributes=False, auto_detect_partition_join=False,
        skip_partition_key_joins=True, geometry_column=GEOMETRY_COLUMN,
    )
    return len(handler.filter_existing_keys(set(hashes), PARTITION_KEY))


def _rows(conn, sql_text: str) -> list:
    """Sorted rows of the last result set of a (multi-statement) script, run in one execute (one transaction)."""
    from partitioncache.db_handler.postgres import fetch_final_result_set

    with conn.cursor() as cur:
        cur.execute(sql_text)
        return sorted(fetch_final_result_set(cur))


def _apply(handler, query: str, lazy: bool, min_component_size: int) -> tuple[str, dict]:
    apply = partitioncache.apply_cache_lazy if lazy else partitioncache.apply_cache
    return apply(
        query=query.strip(), cache_handler=handler, partition_key=PARTITION_KEY, geometry_column=GEOMETRY_COLUMN, p0_alias="p1",
        min_component_size=min_component_size,
    )


@pytest.mark.skipif(not PSYCOPG_AVAILABLE, reason="psycopg not installed")
@pytest.mark.parametrize("lazy", [False, True])
class TestSpatialBoundPostGISBBox:
    def test_attribute_tied_table_uses_only_bounded_fragments(self, spatial_db, bbox_handler, lazy):
        """The museum fragments (10 km away) are cached but not used; the cafe/bakery fragments restrict p1."""
        _populate(bbox_handler, ATTRIBUTE_TIED, min_component_size=1)
        baseline = _rows(spatial_db, ATTRIBUTE_TIED)
        assert baseline, "the query should return rows"
        cached = _cached_lookup_variants(bbox_handler, ATTRIBUTE_TIED, 1)

        enhanced, stats = _apply(bbox_handler, ATTRIBUTE_TIED, lazy, min_component_size=1)

        assert "uncached_reason" not in stats, stats
        assert stats["enhanced"] == 1, stats
        assert 0 < stats["cache_hits"] < cached, (stats, cached)  # the subset path, not the fully linked one
        assert _rows(spatial_db, enhanced) == baseline

    def test_negated_distance_runs_uncached(self, spatial_db, bbox_handler, lazy):
        """With fragments of two tables only, none is bounded to p1: the query runs without the cache."""
        _populate(bbox_handler, NEGATED, min_component_size=2)
        baseline = _rows(spatial_db, NEGATED)
        assert baseline, "the query should return rows"
        assert _cached_lookup_variants(bbox_handler, NEGATED, 2) > 0

        enhanced, stats = _apply(bbox_handler, NEGATED, lazy, min_component_size=2)

        assert stats["uncached_reason"] == "spatial_unbounded", stats
        assert stats["enhanced"] == 0 and stats["cache_hits"] > 0, stats
        assert enhanced.strip() == NEGATED.strip()
        assert _rows(spatial_db, enhanced) == baseline

    def test_negated_distance_uses_the_fragment_of_p0_alone(self, spatial_db, bbox_handler, lazy):
        """With single-table fragments, the one over p1 restricts it without a buffer; the museum fragments are not used."""
        _populate(bbox_handler, NEGATED, min_component_size=1)
        baseline = _rows(spatial_db, NEGATED)
        cached = _cached_lookup_variants(bbox_handler, NEGATED, 1)

        enhanced, stats = _apply(bbox_handler, NEGATED, lazy, min_component_size=1)

        assert "uncached_reason" not in stats, stats
        assert stats["enhanced"] == 1, stats
        assert 0 < stats["cache_hits"] < cached, (stats, cached)
        assert _rows(spatial_db, enhanced) == baseline

    def test_fully_linked_query_uses_every_cached_fragment(self, spatial_db, bbox_handler, lazy):
        """Control: all tables are bounded to p1, every cached fragment restricts it, and no row is lost."""
        _populate(bbox_handler, FULLY_LINKED, min_component_size=1)
        baseline = _rows(spatial_db, FULLY_LINKED)
        assert baseline, "the query should return rows"
        cached = _cached_lookup_variants(bbox_handler, FULLY_LINKED, 1)

        enhanced, stats = _apply(bbox_handler, FULLY_LINKED, lazy, min_component_size=1)

        assert "uncached_reason" not in stats, stats
        assert stats["enhanced"] == 1 and stats["cache_hits"] == cached > 0, (stats, cached)
        assert _rows(spatial_db, enhanced) == baseline

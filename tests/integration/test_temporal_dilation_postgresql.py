"""Temporal dilation against a real PostgreSQL: with the cache, a correlation query returns exactly the rows it returns without.

The partition key ``w`` is the hour bucket of ``ts``. Two of the matching pairs straddle a bucket boundary, so a plain
intersection of the cached bucket sets loses them; dilation must bring them back. The test ids contain the backend
name, so the CI job of each backend selects its cases (-k postgresql_array / -k postgresql_bit).
"""

import os

import psycopg
import pytest

from partitioncache.apply_cache import apply_cache, apply_cache_lazy
from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.query_processor import generate_all_query_hash_pairs

CONN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "integration_user"),
    "password": os.getenv("PG_PASSWORD", "test_password"),
    "dbname": os.getenv("PG_DBNAME", "partitioncache_integration"),
}
EVENTS = "pc_td_events"
WIDTH = 3600
BITSIZE = 20
# (id, ts, kind); w = ts / 3600. Pairs (x, y) with y.ts - x.ts in [0, 1200]: 1-2 and 7-8 straddle a boundary, 3-4 do not
ROWS = [(1, 3500, "x"), (2, 3700, "y"), (3, 7300, "x"), (4, 7400, "y"), (5, 20000, "x"), (6, 30000, "y"), (7, 68000, "x"), (8, 68500, "y")]
QUERY = f"SELECT a.id, b.id FROM {EVENTS} AS a, {EVENTS} AS b WHERE a.kind = 'x' AND b.kind = 'y' AND b.ts BETWEEN a.ts AND a.ts + 1200"
TEMPORAL = {"temporal_bucket_width": WIDTH, "temporal_column": "ts"}


@pytest.fixture
def conn():
    try:
        connection = psycopg.connect(**CONN, autocommit=True)
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    connection.execute(f"DROP TABLE IF EXISTS {EVENTS}")
    connection.execute(f"CREATE TABLE {EVENTS} (id INTEGER, ts INTEGER, w INTEGER, kind TEXT)")
    connection.cursor().executemany(f"INSERT INTO {EVENTS} VALUES (%s, %s, %s / {WIDTH}, %s)", [(i, ts, ts, kind) for i, ts, kind in ROWS])
    yield connection
    connection.execute(f"DROP TABLE IF EXISTS {EVENTS}")
    connection.close()


def _drop_tables(connection, prefix):
    for (name,) in connection.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE %s", (f"{prefix}%",)).fetchall():
        connection.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')


@pytest.fixture(params=["postgresql_array", "postgresql_bit"])
def handler(request, conn):
    prefix = f"pc_td_{request.param}"
    _drop_tables(conn, prefix)
    args = (CONN["dbname"], CONN["host"], CONN["user"], CONN["password"], CONN["port"], prefix)
    h = PostgreSQLArrayCacheHandler(*args) if request.param == "postgresql_array" else PostgreSQLBitCacheHandler(*args, bitsize=BITSIZE)
    h.register_partition_key("w", "integer")
    yield h
    h.db.rollback()
    h.close()
    _drop_tables(conn, prefix)


def _populate(handler, query):
    for variant, key in generate_all_query_hash_pairs(query, "w", min_component_size=1, skip_partition_key_joins=True, warn_no_partition_key=False):
        assert handler.set_cache_lazy(key, variant, "w"), variant


def _rows(conn, query):
    return set(conn.execute(query).fetchall())


def test_cached_query_returns_the_rows_of_the_uncached_query(conn, handler):
    expected = _rows(conn, QUERY)
    assert expected == {(1, 2), (3, 4), (7, 8)}
    _populate(handler, QUERY)

    lazy_query, lazy_stats = apply_cache_lazy(QUERY, handler, "w", min_component_size=1, **TEMPORAL)
    assert lazy_stats["enhanced"] == 1
    assert _rows(conn, lazy_query) == expected

    plain_query, plain_stats = apply_cache(QUERY, handler, "w", min_component_size=1, **TEMPORAL)
    assert plain_stats["enhanced"] == 1
    assert _rows(conn, plain_query) == expected


def test_without_dilation_the_cache_loses_the_straddling_pairs(conn, handler):
    """Guards the test itself: the data must make the plain intersection unsound."""
    _populate(handler, QUERY)
    cached_query, _ = apply_cache_lazy(QUERY, handler, "w", min_component_size=1, dilation_buckets=0)
    assert _rows(conn, cached_query) == {(3, 4)}


def test_query_without_a_two_sided_window_runs_without_the_cache(conn, handler):
    query = f"SELECT a.id, b.id FROM {EVENTS} AS a, {EVENTS} AS b WHERE a.kind = 'x' AND b.kind = 'y' AND b.ts <= a.ts + 1200"
    _populate(handler, query)
    cached_query, stats = apply_cache_lazy(query, handler, "w", min_component_size=1, **TEMPORAL)
    assert stats["enhanced"] == 0
    assert _rows(conn, cached_query) == _rows(conn, query)


def test_dilation_reaches_past_the_last_bucket_of_the_bit_array(handler):
    """A bucket one past the bitsize must survive the shift (bit strings keep their length when shifted)."""
    handler.set_cache("top", {BITSIZE - 1}, "w")
    keys, _ = handler.get_intersected_dilated({"top"}, "w", 1)
    assert keys == {BITSIZE - 2, BITSIZE - 1, BITSIZE}

"""
A lazy intersection reads cache entries when the query runs, not when it is built. An entry evicted in between
must not empty the result: the restriction then falls away and the query returns what it returns without the cache.

Runs against PostgreSQL with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB (the roaring bitmap handler also
needs the roaringbitmap extension).
"""

import os
import uuid

import pytest

from partitioncache.apply_cache import extend_query_with_partition_keys_lazy

CITIES = 100
ROWS_PER_CITY = 10
METHODS = ["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]
HANDLERS = ["array_aggregate", "array_join", "bit", "roaringbit"]


def _connection_params():
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "db_name": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
        "db_host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "db_user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "db_password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "db_port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
    }


def _make_handler(kind, params, prefix):
    if kind.startswith("array"):
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        handler = PostgreSQLArrayCacheHandler(**params, db_tableprefix=prefix)
        if kind == "array_join":
            handler.array_intersect_agg_available = False  # the per-key join instead of array_intersect_agg
        handler.register_partition_key("city_id", "integer")
    elif kind == "bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

        handler = PostgreSQLBitCacheHandler(**params, db_tableprefix=prefix, bitsize=CITIES + 2)
        handler.register_partition_key("city_id", "integer", bitsize=CITIES + 2)
    else:
        from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

        try:
            handler = PostgreSQLRoaringBitCacheHandler(**params, db_tableprefix=prefix)
            handler.register_partition_key("city_id", "integer")
        except Exception as e:
            pytest.skip(f"roaringbitmap not available: {e}")
    return handler


@pytest.fixture(params=["integer", "bigint"])
def points(request):
    """A table with CITIES cities of ROWS_PER_CITY rows each, partition key city_id."""
    import psycopg

    params = _connection_params()
    table = f"lazy_points_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(
        dbname=params["db_name"], host=params["db_host"], user=params["db_user"], password=params["db_password"], port=params["db_port"]
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id serial PRIMARY KEY, city_id {request.param} NOT NULL, val int NOT NULL)")
        cur.execute(f"INSERT INTO {table} (city_id, val) SELECT c, r FROM generate_series(1, {CITIES}) c, generate_series(1, {ROWS_PER_CITY}) r")
        cur.execute(f"CREATE INDEX ON {table} (city_id)")
    conn.commit()
    try:
        yield conn, table
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        conn.close()


@pytest.fixture(params=HANDLERS)
def handler(request):
    params = _connection_params()
    handler = _make_handler(request.param, params, f"lazyevict_{uuid.uuid4().hex[:8]}")
    try:
        yield handler
    finally:
        for key in ("h1", "h2", "h3", "h4"):
            handler.delete(key, "city_id")
        handler.close()


def _run(conn, script):
    """Run a possibly multi-statement query in one transaction and return the rows of its last statement."""
    rows = None
    with conn.cursor() as cur:
        cur.execute(script)
        while True:
            if cur.description is not None:
                rows = cur.fetchall()
            if not cur.nextset():
                break
    conn.rollback()  # drops the ON COMMIT DROP temporary tables
    return rows


def _enhanced(handler, keys, method, query):
    lazy_subquery, used = handler.get_intersected_lazy(keys, "city_id")
    assert lazy_subquery is not None and used == len(keys)
    return extend_query_with_partition_keys_lazy(query, lazy_subquery, "city_id", method=method, p0_alias="p")


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("evicted", [("h1",), ("h1", "h2")], ids=["one_of_two", "all"])
def test_entries_evicted_after_the_lookup_do_not_empty_the_result(points, handler, method, evicted):
    conn, table = points
    query = f"SELECT count(*) FROM {table} AS p WHERE p.city_id BETWEEN 10 AND 20"
    # Valid cache entries: supersets of the cities the query can return
    handler.set_cache("h1", set(range(1, 31)), "city_id")
    handler.set_cache("h2", set(range(5, 26)), "city_id")
    enhanced = _enhanced(handler, {"h1", "h2"}, method, query)
    uncached = _run(conn, query)
    assert _run(conn, enhanced) == uncached

    for key in evicted:
        handler.delete(key, "city_id")

    assert _run(conn, enhanced) == uncached


@pytest.mark.parametrize("method", METHODS)
def test_present_entries_still_restrict_the_query(points, handler, method):
    """Entries that are no supersets of the query's cities show whether the restriction is applied."""
    conn, table = points
    query = f"SELECT count(*) FROM {table} AS p"
    handler.set_cache("h1", set(range(1, 6)), "city_id")
    handler.set_cache("h2", set(range(3, 9)), "city_id")

    assert _run(conn, _enhanced(handler, {"h1", "h2"}, method, query)) == [(3 * ROWS_PER_CITY,)]


@pytest.mark.parametrize("method", METHODS)
def test_an_empty_intersection_still_returns_no_rows(points, handler, method):
    """An empty intersection of present entries proves that no partition matches; it must not fall back to all keys."""
    conn, table = points
    query = f"SELECT count(*) FROM {table} AS p"
    handler.set_cache("h3", set(range(1, 6)), "city_id")
    handler.set_cache("h4", set(range(6, 11)), "city_id")

    assert _run(conn, _enhanced(handler, {"h3", "h4"}, method, query)) == [(0,)]


def test_join_without_alias_falls_back_to_the_keys_of_every_joined_table(points, handler):
    """Without an alias TMP_TABLE_JOIN joins the keys to every table, so the fallback must cover the keys of all of them."""
    conn, table = points
    small = f"{table}_small"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {small} AS SELECT id, city_id FROM {table} WHERE city_id <= 50")
    conn.commit()
    try:
        # The FROM table has cities the joined table lacks
        query = f"SELECT count(*) FROM {table} AS p JOIN {small} AS q ON q.id = 1 WHERE p.city_id BETWEEN 60 AND 70"
        handler.set_cache("h1", set(range(1, 101)), "city_id")
        lazy_subquery, _ = handler.get_intersected_lazy({"h1"}, "city_id")
        enhanced = extend_query_with_partition_keys_lazy(query, lazy_subquery, "city_id", method="TMP_TABLE_JOIN")  # type: ignore[arg-type]
        uncached = _run(conn, query)
        assert uncached == [(11 * ROWS_PER_CITY,)]

        handler.delete("h1", "city_id")

        assert _run(conn, enhanced) == uncached
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {small}")
        conn.commit()


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("evicted", [("h1",), ("h1", "h2")], ids=["one_of_two", "all"])
def test_evicted_entries_of_a_dilated_intersection_do_not_empty_the_result(points, handler, method, evicted):
    if not hasattr(handler, "get_intersected_dilated_lazy"):
        pytest.skip(f"{type(handler).__name__} has no lazy dilated intersection")
    conn, table = points
    query = f"SELECT count(*) FROM {table} AS p WHERE p.city_id BETWEEN 10 AND 20"
    handler.set_cache("h1", set(range(1, 31)), "city_id")
    handler.set_cache("h2", set(range(5, 26)), "city_id")
    lazy_subquery, used = handler.get_intersected_dilated_lazy({"h1", "h2"}, "city_id", 1)
    assert lazy_subquery is not None and used == 2
    enhanced = extend_query_with_partition_keys_lazy(query, lazy_subquery, "city_id", method=method, p0_alias="p")
    uncached = _run(conn, query)
    assert _run(conn, enhanced) == uncached

    for key in evicted:
        handler.delete(key, "city_id")

    assert _run(conn, enhanced) == uncached


@pytest.mark.parametrize("method", METHODS)
def test_a_cte_target_keeps_the_restriction(points, handler, method):
    """A CTE has no rows a separate statement could read, so the placeholder stays a NULL and the query is restricted as before."""
    conn, table = points
    query = f"WITH c AS (SELECT * FROM {table}) SELECT count(*) FROM c AS p"
    handler.set_cache("h1", set(range(1, 6)), "city_id")
    lazy_subquery, _ = handler.get_intersected_lazy({"h1"}, "city_id")
    enhanced = extend_query_with_partition_keys_lazy(query, lazy_subquery, "city_id", method=method, p0_alias="p")  # type: ignore[arg-type]

    assert _run(conn, enhanced) == [(5 * ROWS_PER_CITY,)]


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize(
    ("datatype", "column_type", "values"),
    [
        ("text", "varchar(20)", ["berlin", "bremen", "hamburg", "koeln"]),
        ("timestamp", "timestamptz", ["2026-01-01 00:00:00", "2026-01-02 00:00:00", "2026-01-03 00:00:00", "2026-01-04 00:00:00"]),
        ("float", "numeric", [1.5, 2.5, 3.5, 4.5]),
    ],
)
def test_array_keys_of_other_types_are_restricted_and_fall_back(method, datatype, column_type, values):
    """COALESCE of the cached array and the column's keys must resolve to a common type for every datatype."""
    import datetime

    import psycopg

    from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

    params = _connection_params()
    table = f"lazy_typed_{uuid.uuid4().hex[:8]}"
    keys = [datetime.datetime.fromisoformat(v) if datatype == "timestamp" else v for v in values]
    conn = psycopg.connect(
        dbname=params["db_name"], host=params["db_host"], user=params["db_user"], password=params["db_password"], port=params["db_port"]
    )
    handler = PostgreSQLArrayCacheHandler(**params, db_tableprefix=f"lazytyped_{uuid.uuid4().hex[:8]}")
    try:
        handler.register_partition_key("k", datatype)
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id serial PRIMARY KEY, k {column_type} NOT NULL)")
            for key in values:
                cur.execute(f"INSERT INTO {table} (k) SELECT %s FROM generate_series(1, {ROWS_PER_CITY})", (key,))
        conn.commit()
        handler.set_cache("h1", set(keys[:2]), "k")
        lazy_subquery, _ = handler.get_intersected_lazy({"h1"}, "k")
        enhanced = extend_query_with_partition_keys_lazy(f"SELECT count(*) FROM {table} AS p", lazy_subquery, "k", method=method, p0_alias="p")  # type: ignore[arg-type]

        assert _run(conn, enhanced) == [(2 * ROWS_PER_CITY,)]
        handler.delete("h1", "k")
        assert _run(conn, enhanced) == [(len(values) * ROWS_PER_CITY,)]
    finally:
        handler.close()
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        conn.close()

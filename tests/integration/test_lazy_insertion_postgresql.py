"""
Lazy insertion and the pg_cron processor job against a real PostgreSQL, for each PostgreSQL cache handler.

Test ids carry the backend name, so each backend's CI job selects its own cases (-k <backend>).
The tests open their own connections from PG_* / DB_* and do not need pg_cron.
"""

import datetime
import os
import uuid
from decimal import Decimal

import psycopg
import pytest

CONN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "integration_user"),
    "password": os.getenv("PG_PASSWORD", "test_password"),
    "dbname": os.getenv("PG_DBNAME", os.getenv("DB_NAME", "partitioncache_integration")),
}
BACKENDS = ["postgresql_array", "postgresql_bit", "postgresql_roaringbit"]
PROCESSOR_BACKEND = {"postgresql_array": "array", "postgresql_bit": "bit", "postgresql_roaringbit": "roaringbit"}


def _make_handler(backend: str):
    args = {
        "db_name": CONN["dbname"],
        "db_host": CONN["host"],
        "db_user": CONN["user"],
        "db_password": CONN["password"],
        "db_port": CONN["port"],
        "db_tableprefix": f"lazy_{uuid.uuid4().hex[:8]}",
    }
    if backend == "postgresql_array":
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler as handler_cls
    elif backend == "postgresql_bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler as handler_cls

        args["bitsize"] = 1000
    else:
        from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler as handler_cls
    try:
        handler = handler_cls(**args)
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    if backend == "postgresql_roaringbit":
        handler.db.rollback()
        handler.cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'roaringbitmap'")
        if handler.cursor.fetchone() is None:
            handler.close()
            pytest.skip("roaringbitmap extension not installed")
    return handler


@pytest.fixture(params=BACKENDS)
def backend_handler(request):
    handler = _make_handler(request.param)
    handler.register_partition_key("pk", "integer")
    yield request.param, handler
    handler.close()


def test_zero_rows_store_an_empty_set(backend_handler):
    _, handler = backend_handler
    assert handler.set_cache_lazy("empty", "SELECT 1 AS pk WHERE false", "pk") is True
    stored = handler.get("empty", "pk")
    assert stored is not None and len(stored) == 0
    assert handler.is_null("empty", "pk") is False


def test_quotes_percent_dollar_and_backslash_in_the_query(backend_handler):
    """The query travels as a literal inside a DO block; none of these may break it."""
    _, handler = backend_handler
    query = "SELECT k AS pk FROM (VALUES (1), (2), (NULL::int)) AS v(k) WHERE 'a%b' <> $$x$pc$y$$ AND E'back\\\\slash' <> 'it''s'"
    assert handler.set_cache_lazy("special", query, "pk") is True
    assert set(handler.get("special", "pk")) == {1, 2}


def test_repopulating_a_key_replaces_its_value(backend_handler):
    _, handler = backend_handler
    handler.set_query("again", "SELECT original", "pk")  # a queries row exists before the first insert
    assert handler.set_cache_lazy("again", "SELECT 1 AS pk UNION ALL SELECT 2", "pk") is True
    assert handler.set_cache_lazy("again", "SELECT 3 AS pk", "pk") is True
    assert set(handler.get("again", "pk")) == {3}


@pytest.mark.parametrize(
    "datatype, query, expected",
    [
        ("text", "SELECT 'a%b' AS pk UNION ALL SELECT 'it''s' UNION ALL SELECT E'back\\\\slash'", {"a%b", "it's", "back\\slash"}),
        ("float", "SELECT 1.5::float8 AS pk UNION ALL SELECT 2.25", {Decimal("1.5"), Decimal("2.25")}),
        ("timestamp", "SELECT TIMESTAMP '2024-01-01 10:00:00' AS pk", {datetime.datetime(2024, 1, 1, 10, 0)}),
    ],
    ids=["postgresql_array-text", "postgresql_array-float", "postgresql_array-timestamp"],
)
def test_array_lazy_insertion_for_each_datatype(datatype, query, expected):
    handler = _make_handler("postgresql_array")
    try:
        handler.register_partition_key("pk", datatype)
        assert handler.set_cache_lazy("typed", query, "pk") is True
        assert set(handler.get("typed", "pk")) == expected
        assert handler.set_cache_lazy("typed_empty", "SELECT NULL AS pk WHERE false", "pk") is True
        assert len(handler.get("typed_empty", "pk")) == 0
    finally:
        handler.close()


def test_processor_job_stores_results_and_zero_rows(backend_handler):
    """_partitioncache_execute_job stores the keys, and an empty set (not the NULL marker) for zero rows."""
    from partitioncache.cli.postgresql_queue_processor import get_queue_table_prefix_from_env, setup_database_objects

    backend, handler = backend_handler
    conn = psycopg.connect(**CONN, autocommit=True)
    try:
        try:
            setup_database_objects(conn, cron_conn=None)
        except Exception as e:
            pytest.skip(f"processor functions could not be set up: {e}")
        queue_prefix = get_queue_table_prefix_from_env()
        conn.execute("SELECT partitioncache_initialize_cache_processor_tables(%s)", [queue_prefix])
        for item_id, (key, query) in enumerate([("job_some", "SELECT 5 AS pk UNION ALL SELECT 7 UNION ALL SELECT NULL"), ("job_empty", "SELECT 5 AS pk WHERE false")]):
            ok = conn.execute(
                "SELECT _partitioncache_execute_job(%s, %s, %s, 'pk', 'integer', %s, %s, %s, 'manual')",
                (900_000 + item_id, query, key, queue_prefix, handler.tableprefix, PROCESSOR_BACKEND[backend]),
            ).fetchone()[0]
            assert ok is True
        assert set(handler.get("job_some", "pk")) == {5, 7}
        stored = handler.get("job_empty", "pk")
        assert stored is not None and len(stored) == 0
        assert handler.is_null("job_empty", "pk") is False
    finally:
        conn.close()

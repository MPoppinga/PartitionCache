"""
The queue processor's result limit: results with at least result_limit partition keys are not stored, and the
query is recorded with the status 'limit' so it is not computed again.

The PostgreSQL cases run with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB; the key-value handlers run where
available.
"""

import os
import uuid

import pytest

QUEUE_PREFIX = "resultlimit_queue"


def _pg_connect():
    import psycopg

    return psycopg.connect(
        dbname=os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
        host=os.getenv("PARTITIONCACHE_TEST_PG_HOST"),
        user=os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        password=os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        port=int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        autocommit=True,
    )


@pytest.fixture
def postgresql_array():
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

    prefix = f"resultlimit_{uuid.uuid4().hex[:8]}"
    handler = PostgreSQLArrayCacheHandler(
        db_name=os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
        db_host=os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        db_user=os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        db_password=os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        db_port=int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        db_tableprefix=prefix,
    )
    handler.register_partition_key("pk", "integer")
    try:
        yield handler
    finally:
        handler.close()


class TestPostgreSQLProcessorResultLimit:
    @pytest.fixture
    def conn(self, postgresql_array):
        from partitioncache.cli.postgresql_queue_processor import SQL_CACHE_FILE, SQL_HANDLERS_FILE

        conn = _pg_connect()
        conn.execute(SQL_HANDLERS_FILE.read_text())
        conn.execute(SQL_CACHE_FILE.read_text())
        conn.execute("SELECT partitioncache_initialize_cache_processor_tables(%s)", (QUEUE_PREFIX,))
        try:
            yield conn
        finally:
            conn.close()

    def _run(self, conn, handler, query_hash, query, result_limit):
        ok = conn.execute(
            "SELECT _partitioncache_execute_job(1, %s, %s, 'pk', 'integer', %s, %s, 'array', 'manual', 60, NULL, %s)",
            (query, query_hash, QUEUE_PREFIX, handler.tableprefix, result_limit),
        ).fetchone()[0]
        return ok, handler.get_query_status(query_hash, "pk")

    def test_result_at_the_limit_is_not_stored_and_marked(self, conn, postgresql_array):
        three_keys = "SELECT 1 AS pk UNION ALL SELECT 2 UNION ALL SELECT 3"
        assert self._run(conn, postgresql_array, "large", three_keys, 3) == (True, "limit")
        assert postgresql_array.get("large", "pk") is None
        # Known, so the processor does not compute it again
        assert postgresql_array.exists("large", "pk", check_query=True)

    def test_result_below_the_limit_is_stored(self, conn, postgresql_array):
        assert self._run(conn, postgresql_array, "small", "SELECT 1 AS pk UNION ALL SELECT 2", 3) == (True, "ok")
        assert postgresql_array.get("small", "pk") == {1, 2}

    def test_no_limit_stores_everything(self, conn, postgresql_array):
        assert self._run(conn, postgresql_array, "any", "SELECT generate_series(1, 50) AS pk", None) == (True, "ok")
        assert len(postgresql_array.get("any", "pk")) == 50


def test_queries_table_from_before_the_limit_status_is_migrated(postgresql_array):
    """A queries table whose CHECK lacks 'limit' (created by older versions) accepts it after handler setup."""
    from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

    table = postgresql_array.tableprefix + "_queries"
    conn = _pg_connect()
    try:
        name = conn.execute("SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'c'", (table,)).fetchone()[0]
        conn.execute(f"ALTER TABLE {table} DROP CONSTRAINT {name}, ADD CONSTRAINT {name} CHECK (status IN ('ok', 'timeout', 'failed'))")

        handler = PostgreSQLArrayCacheHandler(
            db_name=os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
            db_host=os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
            db_user=os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
            db_password=os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
            db_port=int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
            db_tableprefix=postgresql_array.tableprefix,
        )
        try:
            handler.set_query("q", "SELECT 1", "pk")
            assert handler.set_query_status("q", "pk", "limit") is True
            assert handler.get_query_status("q", "pk") == "limit"
        finally:
            handler.close()
    finally:
        conn.close()


@pytest.mark.parametrize("backend", ["rocksdict", "rocksdb", "redis_set"])
def test_key_value_handlers_keep_limit_and_failed_apart(backend, tmp_path):
    if backend == "redis_set":
        from tests.pytest.test_empty_set_contract import REDIS_HOST, REDIS_PORT, _redis_available

        if not _redis_available():
            pytest.skip(f"no Redis server at {REDIS_HOST}:{REDIS_PORT}")
        from partitioncache.cache_handler.redis_set import RedisCacheHandler

        handler = RedisCacheHandler(db_name=15, db_host=REDIS_HOST, db_password=None, db_port=REDIS_PORT)
    elif backend == "rocksdict":
        pytest.importorskip("rocksdict")
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        handler = RocksDictCacheHandler(str(tmp_path / "rocksdict"))
    else:
        pytest.importorskip("rocksdb")
        from partitioncache.cache_handler.rocks_db_set import RocksDBCacheHandler

        handler = RocksDBCacheHandler(str(tmp_path / "rocksdb"))
    partition_key = f"pk_{uuid.uuid4().hex[:8]}"
    try:
        handler.register_partition_key(partition_key, "integer")
        for key, status in [("a", "limit"), ("b", "failed"), ("c", "timeout")]:
            handler.set_query(key, f"SELECT {key}", partition_key)
            assert handler.set_query_status(key, partition_key, status) is True
            assert handler.get_query_status(key, partition_key) == status
        assert handler.set_query_status("a", partition_key, "ok") is True
        assert handler.get_query_status("a", partition_key) == "ok"
    finally:
        for key in ("a", "b", "c"):
            handler.delete(key, partition_key)
        handler.close()

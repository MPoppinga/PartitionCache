"""
Keys a bit array cannot hold must never be stored as a different set.

A key beyond the bitsize grows the PostgreSQL bit arrays (to max key + 1 + bitsize_buffer), also in the
pg_cron processor, and fails loudly in the other bit handlers; a negative key always fails (bitarray[-1]
silently set the last bit). The bitsize never shrinks, which would cut the stored bit arrays.
Backends run where available: DuckDB and RocksDB locally, Redis with a server
(PARTITIONCACHE_TEST_REDIS_HOST/PORT), PostgreSQL with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import logging
import os
import uuid

import pytest

from tests.pytest.test_empty_set_contract import REDIS_HOST, REDIS_PORT, _redis_available

# The bitsize trigger function as it was before USING and the shrink check: the ALTER fails once a row exists
OLD_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION {prefix}_bitsize_trigger_func() RETURNS TRIGGER AS $t$
DECLARE v_cache_table TEXT;
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.bitsize != NEW.bitsize THEN
        v_cache_table := '{prefix}_cache_' || NEW.partition_key;
        EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS partition_keys_count', v_cache_table);
        EXECUTE format('ALTER TABLE %I ALTER COLUMN partition_keys TYPE BIT(%s)', v_cache_table, NEW.bitsize);
        EXECUTE format('ALTER TABLE %I ADD COLUMN partition_keys_count INTEGER GENERATED ALWAYS AS '
                       '(length(replace(partition_keys::text, ''0'', ''''))) STORED', v_cache_table);
    END IF;
    RETURN NEW;
END;
$t$ LANGUAGE plpgsql
"""


def _postgresql_bit(bitsize, bitsize_buffer=0):
    host = os.getenv("PARTITIONCACHE_TEST_PG_HOST")
    if not host:
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

    return PostgreSQLBitCacheHandler(
        db_name=os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
        db_host=host,
        db_user=os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        db_password=os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        db_port=int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        db_tableprefix=f"bitrange_{uuid.uuid4().hex[:8]}",
        bitsize=bitsize,
        bitsize_buffer=bitsize_buffer,
    )


def _failed_or_raised(store) -> bool:
    try:
        return store() is False
    except ValueError:
        return True


class TestPostgreSQLBitGrowth:
    def test_eager_key_beyond_bitsize_grows_with_buffer_and_keeps_entries(self, caplog):
        handler = _postgresql_bit(bitsize=16, bitsize_buffer=8)
        try:
            handler.register_partition_key("pk", "integer")
            assert handler.set_cache("small", {1, 2}, "pk") is True
            with caplog.at_level(logging.WARNING):
                assert handler.set_cache("large", {3, 20}, "pk") is True
            assert handler.get("large", "pk") == {3, 20}
            assert handler.get("small", "pk") == {1, 2}
            assert handler._get_partition_bitsize("pk") == 29  # 20 + 1 + 8
            assert "grown to 29" in caplog.text
        finally:
            handler.close()

    def test_lazy_key_beyond_bitsize_grows(self):
        handler = _postgresql_bit(bitsize=16, bitsize_buffer=8)
        try:
            handler.register_partition_key("pk", "integer")
            handler.set_cache("small", {1, 2}, "pk")
            assert handler.set_cache_lazy("lazy", "SELECT 5 AS pk UNION ALL SELECT 40", "pk") is True
            assert handler.get("lazy", "pk") == {5, 40}
            assert handler.get("small", "pk") == {1, 2}
            assert handler._get_partition_bitsize("pk") == 49
        finally:
            handler.close()

    def test_negative_keys_are_rejected(self):
        handler = _postgresql_bit(bitsize=16)
        try:
            handler.register_partition_key("pk", "integer")
            assert handler.set_cache("eager", {-1, 4}, "pk") is False
            assert handler.set_cache_lazy("lazy", "SELECT -1 AS pk UNION ALL SELECT 4", "pk") is False
            assert handler.get("eager", "pk") is None
            assert handler.get("lazy", "pk") is None
        finally:
            handler.close()

    def test_bitsize_never_shrinks(self):
        """A smaller bitsize would cut the stored bit arrays: the setter keeps the larger one, the trigger refuses."""
        import psycopg

        handler = _postgresql_bit(bitsize=16)
        try:
            handler.register_partition_key("pk", "integer")
            assert handler.set_cache("large", {3, 20}, "pk") is True
            handler._set_partition_bitsize("pk", 16)
            assert handler._get_partition_bitsize("pk") == 21
            with pytest.raises(psycopg.errors.RaiseException, match="cannot shrink"):
                handler.cursor.execute(f"UPDATE {handler.tableprefix}_partition_metadata SET bitsize = 16 WHERE partition_key = 'pk'")
            handler.db.rollback()
            assert handler.get("large", "pk") == {3, 20}
        finally:
            handler.close()

    def test_old_trigger_function_is_replaced_on_growth(self):
        handler = _postgresql_bit(bitsize=16)
        try:
            handler.register_partition_key("pk", "integer")
            assert handler.set_cache("small", {1, 2}, "pk") is True
            handler.cursor.execute(OLD_TRIGGER_FUNCTION.format(prefix=handler.tableprefix))
            handler.db.commit()
            assert handler.set_cache("large", {3, 20}, "pk") is True
            assert handler.get("large", "pk") == {3, 20}
            assert handler.get("small", "pk") == {1, 2}
        finally:
            handler.close()


class TestPostgreSQLBitProcessor:
    """The pg_cron processor job (_partitioncache_execute_job), called directly without pg_cron."""

    QUEUE_PREFIX = "bitrange_queue"

    @pytest.fixture
    def setup(self):
        handler = _postgresql_bit(bitsize=16)
        import psycopg

        from partitioncache.cli.postgresql_queue_processor import SQL_CACHE_FILE, SQL_HANDLERS_FILE

        conn = psycopg.connect(
            dbname=os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
            host=os.getenv("PARTITIONCACHE_TEST_PG_HOST"),
            user=os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
            password=os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
            port=int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
            autocommit=True,
        )
        conn.execute(SQL_HANDLERS_FILE.read_text())
        conn.execute(SQL_CACHE_FILE.read_text())
        conn.execute("SELECT partitioncache_initialize_cache_processor_tables(%s)", (self.QUEUE_PREFIX,))
        handler.register_partition_key("pk", "integer")
        handler.set_cache("small", {1, 2}, "pk")
        notices: list[str] = []
        conn.add_notice_handler(lambda d: notices.append(d.message_primary))
        try:
            yield handler, conn, notices
        finally:
            conn.close()
            handler.close()

    def _run(self, handler, conn, query_hash, query):
        ok = conn.execute(
            "SELECT _partitioncache_execute_job(1, %s, %s, 'pk', 'integer', %s, %s, 'bit', 'manual')",
            (query, query_hash, self.QUEUE_PREFIX, handler.tableprefix),
        ).fetchone()[0]
        status = conn.execute(f"SELECT status FROM {handler.tableprefix}_queries WHERE query_hash = %s", (query_hash,)).fetchone()
        return ok, status[0] if status else None

    def test_key_beyond_bitsize_grows_with_buffer(self, setup):
        handler, conn, notices = setup
        conn.execute("SET partitioncache.bitsize_buffer = '8'")
        assert self._run(handler, conn, "large", "SELECT 5 AS pk UNION ALL SELECT 40") == (True, "ok")
        assert handler.get("large", "pk") == {5, 40}
        assert handler.get("small", "pk") == {1, 2}
        assert handler._get_partition_bitsize("pk") == 49
        assert any("grown to 49" in n for n in notices)

    def test_negative_buffer_fails_instead_of_dropping_keys(self, setup):
        handler, conn, _ = setup
        conn.execute("SET partitioncache.bitsize_buffer = '-10'")
        assert self._run(handler, conn, "large", "SELECT 3 AS pk UNION ALL SELECT 20") == (False, "failed")
        assert handler.get("large", "pk") is None
        assert handler._get_partition_bitsize("pk") == 16

    def test_negative_key_fails(self, setup):
        handler, conn, _ = setup
        assert self._run(handler, conn, "negative", "SELECT -1 AS pk UNION ALL SELECT 4") == (False, "failed")
        assert handler.get("negative", "pk") is None

    def test_bitsize_grown_while_the_variant_runs(self, setup):
        """The bitsize read before the variant is stale when another job grows the bit arrays meanwhile."""
        handler, conn, _ = setup
        prefix = handler.tableprefix
        conn.execute(
            f"CREATE FUNCTION {prefix}_grow() RETURNS INTEGER AS $$ "
            f"SELECT 3 FROM (SELECT partitioncache_bootstrap_partition('{prefix}', 'pk', 'integer', 'bit', 41)) AS grown $$ LANGUAGE sql"
        )
        assert self._run(handler, conn, "stale", f"SELECT {prefix}_grow() AS pk") == (True, "ok")
        assert handler._get_partition_bitsize("pk") == 41
        assert handler.get("stale", "pk") == {3}
        assert handler.get("small", "pk") == {1, 2}


class TestDuckDBBitRange:
    @pytest.fixture
    def handler(self):
        from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

        handler = DuckDBBitCacheHandler(":memory:", table_prefix="bitrange", bitsize=16)
        handler.register_partition_key("pk", "integer")
        yield handler
        handler.close()

    def test_eager_out_of_range_fails(self, handler):
        assert handler.set_cache("large", {3, 20}, "pk") is False
        assert handler.set_cache("negative", {-1, 4}, "pk") is False
        assert handler.get("large", "pk") is None
        assert handler.get("negative", "pk") is None

    def test_lazy_out_of_range_fails_instead_of_dropping(self, handler):
        assert handler.set_cache_lazy("large", "SELECT 5 AS pk UNION ALL SELECT 40", "pk") is False
        assert handler.set_cache_lazy("negative", "SELECT -1 AS pk UNION ALL SELECT 4", "pk") is False
        assert handler.get("large", "pk") is None
        assert handler.get("negative", "pk") is None
        assert handler.set_cache_lazy("fits", "SELECT 5 AS pk UNION ALL SELECT 15", "pk") is True
        assert handler.get("fits", "pk") == {5, 15}
        assert handler.set_cache_lazy("no_rows", "SELECT 5 AS pk WHERE false", "pk") is True
        assert handler.get("no_rows", "pk") == set()


@pytest.mark.parametrize("backend", ["redis_bit", "rocksdb_bit"])
def test_redis_and_rocksdb_bit_reject_keys_they_cannot_hold(backend, tmp_path):
    if backend == "redis_bit":
        if not _redis_available():
            pytest.skip(f"no Redis server at {REDIS_HOST}:{REDIS_PORT}")
        from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler

        handler = RedisBitCacheHandler(db_name=15, db_host=REDIS_HOST, db_password=None, db_port=REDIS_PORT, bitsize=16)
    else:
        pytest.importorskip("rocksdb")
        from partitioncache.cache_handler.rocks_db_bit import RocksDBBitCacheHandler

        handler = RocksDBBitCacheHandler(str(tmp_path / "rocksdb_bit"), bitsize=16)
    partition_key = f"pk_{uuid.uuid4().hex[:8]}"
    try:
        handler.register_partition_key(partition_key, "integer")
        assert _failed_or_raised(lambda: handler.set_cache("large", {3, 20}, partition_key))
        assert _failed_or_raised(lambda: handler.set_cache("negative", {-1, 4}, partition_key))
        assert handler.get("large", partition_key) is None
        assert handler.get("negative", partition_key) is None
    finally:
        for key in ("large", "negative"):
            handler.delete(key, partition_key)
        handler.close()

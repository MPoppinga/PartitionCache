"""
Keys a bit array cannot hold must never be stored as a different set.

A key beyond the bitsize grows the PostgreSQL bit arrays (to max key + 1 + bitsize_buffer) and fails
loudly in the other bit handlers; a negative key always fails (bitarray[-1] silently set the last bit).
Backends run where available: DuckDB and RocksDB locally, Redis with a server
(PARTITIONCACHE_TEST_REDIS_HOST/PORT), PostgreSQL with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import logging
import os
import uuid

import pytest

from tests.pytest.test_empty_set_contract import REDIS_HOST, REDIS_PORT, _redis_available


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

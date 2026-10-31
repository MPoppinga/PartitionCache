"""
Empty-result contract for cache handlers, against real backends where they are available locally.

An empty set is a valid result: the query has no rows, and cache application turns it into
"restrict to nothing". It must be stored, read back as an empty set and take part in intersections,
and it must never be confused with "not cached" or the set_null marker (both read back as None).

Redis handlers need a server (PARTITIONCACHE_TEST_REDIS_HOST/PORT, default localhost:6379) and are
skipped without one; RocksDB handlers need python-rocksdb. PostgreSQL handlers are covered by the
integration tests.
"""

import functools
import os
import socket
import uuid

import pytest

REDIS_HOST = os.getenv("PARTITIONCACHE_TEST_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("PARTITIONCACHE_TEST_REDIS_PORT", "6379"))


@functools.cache
def _redis_available() -> bool:
    # A plain socket probe first: redis-py retries a failed connect for many seconds
    try:
        socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.5).close()
        import redis

        return bool(redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=1).ping())
    except Exception:
        return False


def _rocksdict(tmp_path):
    from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

    return RocksDictCacheHandler(str(tmp_path / "rocksdict"))


def _rocksdict_roaringbit(tmp_path):
    from partitioncache.cache_handler.rocksdict_roaringbit import RocksDictRoaringBitCacheHandler

    return RocksDictRoaringBitCacheHandler(str(tmp_path / "rocksdict_roaring"))


def _duckdb_bit(tmp_path):
    from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

    return DuckDBBitCacheHandler(":memory:", table_prefix="contract", bitsize=64)


def _redis(cls_name, module, **kwargs):
    def factory(tmp_path):
        if not _redis_available():
            pytest.skip(f"no Redis server at {REDIS_HOST}:{REDIS_PORT}")
        cls = getattr(__import__(f"partitioncache.cache_handler.{module}", fromlist=[cls_name]), cls_name)
        return cls(db_name=15, db_host=REDIS_HOST, db_password=None, db_port=REDIS_PORT, **kwargs)

    return factory


def _rocksdb(cls_name, module, **kwargs):
    def factory(tmp_path):
        pytest.importorskip("rocksdb")
        cls = getattr(__import__(f"partitioncache.cache_handler.{module}", fromlist=[cls_name]), cls_name)
        return cls(str(tmp_path / module), **kwargs)

    return factory


# name -> (factory, needs a registered partition key to store an empty set)
HANDLERS = {
    "rocksdict": (_rocksdict, True),
    "rocksdict_roaringbit": (_rocksdict_roaringbit, False),
    "duckdb_bit": (_duckdb_bit, False),
    "redis_set": (_redis("RedisCacheHandler", "redis_set"), True),
    "redis_bit": (_redis("RedisBitCacheHandler", "redis_bit", bitsize=64), False),
    "redis_roaringbit": (_redis("RedisRoaringBitCacheHandler", "redis_roaringbit"), False),
    "rocksdb_set": (_rocksdb("RocksDBCacheHandler", "rocks_db_set"), True),
    "rocksdb_bit": (_rocksdb("RocksDBBitCacheHandler", "rocks_db_bit", bitsize=8), False),
}


@pytest.fixture(params=list(HANDLERS))
def handler_case(request, tmp_path):
    factory, needs_registration = HANDLERS[request.param]
    handler = factory(tmp_path)
    # A fresh partition key per test keeps shared backends (Redis) independent of earlier runs
    partition_key = f"pk_{uuid.uuid4().hex[:8]}"
    handler.register_partition_key(partition_key, "integer")
    yield handler, partition_key, needs_registration
    for key in ("empty", "full", "null", "switch"):
        try:
            handler.delete(key, partition_key)
        except Exception:
            pass
    handler.close()


def _is_empty(value) -> bool:
    return value is not None and len(value) == 0


def test_empty_set_is_stored_and_read_back_as_empty(handler_case):
    handler, pk, _ = handler_case
    assert handler.set_cache("empty", set(), pk) is True
    assert handler.exists("empty", pk) is True
    assert _is_empty(handler.get("empty", pk))
    assert handler.is_null("empty", pk) is False


def test_empty_set_empties_the_intersection(handler_case):
    handler, pk, _ = handler_case
    handler.set_cache("empty", set(), pk)
    handler.set_cache("full", {1, 2}, pk)

    keys, hits = handler.get_intersected({"empty"}, pk)
    assert _is_empty(keys) and hits == 1
    keys, hits = handler.get_intersected({"empty", "full"}, pk)
    assert _is_empty(keys) and hits == 2


def test_null_marker_and_missing_key_are_not_empty(handler_case):
    handler, pk, _ = handler_case
    handler.set_cache("full", {1, 2}, pk)
    handler.set_null("null", pk)

    assert handler.get("null", pk) is None
    assert handler.is_null("null", pk) is True
    assert handler.get("missing", pk) is None
    keys, hits = handler.get_intersected({"null", "missing"}, pk)
    assert keys is None and hits == 0
    keys, hits = handler.get_intersected({"null", "full"}, pk)
    assert set(keys) == {1, 2} and hits == 1


def test_empty_and_non_empty_replace_each_other(handler_case):
    handler, pk, _ = handler_case
    handler.set_cache("switch", set(), pk)
    assert handler.set_cache("switch", {3, 4}, pk) is True
    assert set(handler.get("switch", pk)) == {3, 4}
    assert handler.set_cache("switch", {5}, pk) is True
    assert set(handler.get("switch", pk)) == {5}
    assert handler.set_cache("switch", set(), pk) is True
    assert _is_empty(handler.get("switch", pk))


def test_empty_set_for_unregistered_partition_key(handler_case):
    """Handlers that infer the datatype from the values refuse instead of raising StopIteration."""
    handler, _, needs_registration = handler_case
    result = handler.set_cache("empty", set(), f"unregistered_{uuid.uuid4().hex[:8]}")
    if needs_registration:
        assert result is False
    else:
        assert result is True


def test_rocksdb_set_text_partition_keys(tmp_path):
    """Comma-joined text sets: the empty set reads back empty, keys the format cannot represent are rejected."""
    pytest.importorskip("rocksdb")
    from partitioncache.cache_handler.rocks_db_set import RocksDBCacheHandler

    handler = RocksDBCacheHandler(str(tmp_path / "rocksdb_text"))
    handler.register_partition_key("city", "text")
    try:
        assert handler.set_cache("empty", set(), "city") is True
        assert handler.get("empty", "city") == set()
        assert handler.set_cache("full", {"Berlin", "Hamburg"}, "city") is True
        assert handler.get("full", "city") == {"Berlin", "Hamburg"}
        for bad in ({"a,b"}, {""}, {"\x00"}):
            with pytest.raises(ValueError):
                handler.set_cache("bad", bad, "city")
    finally:
        handler.close()

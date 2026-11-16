"""Partition keys whose cache table name PostgreSQL would truncate are refused.

PostgreSQL truncates identifiers longer than 63 bytes without an error. Two partition keys with a long common beginning
then map to one cache table, and a hash cached under one key is read for the other.
"""

from unittest.mock import Mock

import pytest

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

PREFIX = "pc_sensor_readings"  # "pc_sensor_readings_cache_" is 25 bytes
FITS = "k" * 38  # 63 bytes
TOO_LONG = "k" * 39  # 64 bytes
TOO_LONG_MULTIBYTE = "ü" * 20  # 20 characters, 40 bytes: 65 bytes


def _handler(cls):
    h = cls.__new__(cls)
    h.db = Mock()
    h.cursor = Mock()
    h.cursor.fetchone.return_value = None
    h.tableprefix = PREFIX
    h._cached_datatype = {}
    h.default_bitsize = 100
    h.bitsize_buffer = 0
    h.array_intersect_agg_available = True
    return h


def _executed(handler) -> str:
    return " ".join(str(c.args[0]) for c in handler.cursor.execute.call_args_list)


@pytest.mark.parametrize("cls", [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler])
@pytest.mark.parametrize("partition_key", [TOO_LONG, TOO_LONG_MULTIBYTE], ids=["ascii", "multibyte"])
def test_registering_a_partition_key_with_a_too_long_table_name_is_refused(cls, partition_key):
    handler = _handler(cls)
    with pytest.raises(ValueError, match="63 bytes"):
        handler.register_partition_key(partition_key, "integer")
    assert "bootstrap" not in _executed(handler) and "CREATE TABLE" not in _executed(handler)


@pytest.mark.parametrize("cls", [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler])
def test_using_a_partition_key_with_a_too_long_table_name_is_refused(cls):
    handler = _handler(cls)
    with pytest.raises(ValueError, match="63 bytes"):
        handler.get("h1", TOO_LONG)
    assert handler.exists("h1", TOO_LONG) is False
    assert handler.filter_existing_keys({"h1"}, TOO_LONG) == set()
    assert handler.cursor.execute.call_count == 0


@pytest.mark.parametrize("cls", [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler])
def test_storing_under_a_partition_key_with_a_too_long_table_name_is_refused(cls):
    handler = _handler(cls)
    try:
        stored = handler.set_cache("h1", {1, 2}, TOO_LONG)
    except ValueError as e:
        assert "63 bytes" in str(e)
        stored = False
    assert stored is False
    assert handler.set_cache_lazy("h1", "SELECT 1 AS k", TOO_LONG) is False
    assert "INSERT" not in _executed(handler)


def test_a_table_name_of_63_bytes_is_used():
    handler = _handler(PostgreSQLArrayCacheHandler)
    handler.cursor.fetchone.return_value = ("integer",)
    assert len(f"{PREFIX}_cache_{FITS}".encode()) == 63
    assert handler.get_datatype(FITS) == "integer"

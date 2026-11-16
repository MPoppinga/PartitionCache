"""A new partition key whose cache table PostgreSQL would truncate to another key's table name is refused.

The rule runs on the server (partitioncache_bootstrap_partition, SQLSTATE PCT01; tests/integration/
test_cache_table_name_collision.py). Here: the handlers turn that refusal into a ValueError and do not write on, and
a long name alone is not refused.
"""

from unittest.mock import Mock

import psycopg
import pytest

from partitioncache.cache_handler.postgresql_abstract import SHARED_CACHE_TABLE_SQLSTATE
from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

LONG = "sensor_readings_" + "x" * 60
MESSAGE = f"Partition key '{LONG}_b' would share the cache table 'p_cache_sensor...' with partition key '{LONG}_a'"


class _SharedCacheTable(psycopg.Error):
    sqlstate = SHARED_CACHE_TABLE_SQLSTATE


def _handler(cls, refuse_bootstrap: bool):
    h = cls.__new__(cls)
    h.db = Mock()
    h.cursor = Mock()
    h.cursor.fetchone.return_value = (True,)  # the advisory lock of the bit handler
    h.tableprefix = "p"
    h._cached_datatype = {}
    h.default_bitsize = 100
    h.bitsize_buffer = 0
    h.array_intersect_agg_available = True
    h._get_partition_datatype = Mock(return_value=None)
    bootstrapped = []
    h._get_partition_bitsize = Mock(side_effect=lambda partition_key: 100 if bootstrapped else None)  # bit
    h._load_sql_functions = Mock()

    def execute(statement, params=None):
        if "partitioncache_bootstrap_partition" in str(statement):
            if refuse_bootstrap:
                raise _SharedCacheTable(MESSAGE)
            bootstrapped.append(statement)

    h.cursor.execute.side_effect = execute
    return h


def _executed(handler) -> list[str]:
    return [str(c.args[0]) for c in handler.cursor.execute.call_args_list]


@pytest.mark.parametrize("cls", [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler])
def test_the_bootstraps_refusal_is_a_value_error(cls):
    handler = _handler(cls, refuse_bootstrap=True)
    with pytest.raises(ValueError, match="would share the cache table"):
        handler.register_partition_key(LONG + "_b", "integer")
    # The array handler's fallback (a second bootstrap) is not tried for this error
    assert sum("partitioncache_bootstrap_partition" in s for s in _executed(handler)) == 1
    handler.db.rollback.assert_called()


@pytest.mark.parametrize("cls", [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler])
def test_storing_under_a_refused_key_writes_nothing(cls):
    handler = _handler(cls, refuse_bootstrap=True)
    try:
        stored = handler.set_cache("h1", {1, 2}, LONG + "_b")
    except ValueError:
        stored = False
    assert stored is False
    assert not any("INSERT" in s for s in _executed(handler))


@pytest.mark.parametrize("cls", [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler])
def test_a_long_partition_key_alone_is_not_refused(cls):
    handler = _handler(cls, refuse_bootstrap=False)
    handler.register_partition_key(LONG, "integer")
    assert any("partitioncache_bootstrap_partition" in s for s in _executed(handler))

    del handler._get_partition_datatype  # the handler's own lookup: it reads the metadata, whatever the name's length
    handler.cursor.fetchone.return_value = ("integer",)
    assert handler.get_datatype(LONG) == "integer"


def test_refuse_shared_cache_table_names_both_keys_and_the_table():
    handler = _handler(PostgreSQLArrayCacheHandler, refuse_bootstrap=False)
    handler.cursor.fetchone.return_value = (LONG + "_a", "p_cache_" + LONG[:55])
    with pytest.raises(ValueError) as refused:
        handler._refuse_shared_cache_table(LONG + "_b")
    assert all(part in str(refused.value) for part in (LONG + "_a", LONG + "_b", "p_cache_" + LONG[:55]))

    handler.cursor.fetchone.return_value = None
    handler._refuse_shared_cache_table(LONG + "_c")

"""
A cache with a long table prefix must keep working where tables are looked up by name.

PostgreSQL stores identifiers in 63 bytes and compares a catalog name with a TEXT value as text. PL/pgSQL code
that looks a table up by its untruncated name therefore never finds it once the name is longer. With a prefix of
55 characters the metadata table (`<prefix>_partition_metadata`) and the cache table (`<prefix>_cache_pk`) are both
stored under truncated names:

- the eviction job logged "No cache metadata table found" and evicted nothing,
- the status report returned no row for the cache,
- the bitsize trigger did not find the cache table and never widened its bit column.

Runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import uuid
from pathlib import Path

import pytest

import partitioncache
from tests.pytest.test_shared_bitsize_trigger import _drop_cache, _functions_of, _handler, _query

PACKAGE = Path(partitioncache.__file__).parent
EVICTION_SQL = PACKAGE / "cache_handler" / "postgresql_cache_eviction_cache.sql"
STATUS_SQL = PACKAGE / "queue_handler" / "postgresql_queue_processor_cache_info.sql"

SHORT = "pcache_prefix"
# 46 + 9 = 55 characters: also `<prefix>_cache_pk` (64 bytes) is stored truncated
LONG = "pcache_prefix_longer_than_forty_seven_characte"


@pytest.fixture(params=[SHORT, LONG], ids=["short", "long"])
def prefix(request):
    name = f"{request.param}_{uuid.uuid4().hex[:8]}"
    assert (len(name) == 55) == (request.param == LONG)
    yield name
    _drop_cache(name)
    _query(f'DROP TABLE IF EXISTS "{name}_eviction_log" CASCADE')
    for function in _functions_of(name):
        _query(f'DROP FUNCTION IF EXISTS "{function}"() CASCADE')


def test_bit_arrays_grow(prefix):
    """The trigger has to find the cache table to widen its bit column, also under a truncated name."""
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
        assert handler.set_cache("small", {1, 2}, "pk") is True
        assert handler.set_cache("large", {3, 20}, "pk") is True
        assert handler.get("large", "pk") == {3, 20}
        assert handler.get("small", "pk") == {1, 2}
    finally:
        handler.close()


def test_largest_eviction_removes_entries(prefix):
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
        for key, keys in (("one", {1}), ("two", {1, 2}), ("three", {1, 2, 3})):
            assert handler.set_cache(key, keys, "pk") is True
            assert handler.set_query(key, "SELECT 1", "pk") is True

        handler.cursor.execute(EVICTION_SQL.read_text())
        handler.cursor.execute("SELECT partitioncache_initialize_eviction_cache_log_table(%s)", (prefix,))
        handler.cursor.execute("SELECT partitioncache_run_eviction_job_with_params('job', %s, 'largest', 1)", (prefix,))
        handler.db.commit()

        # Threshold 1: the two entries with the most keys go, the smallest stays
        assert handler.get_all_keys("pk") == ["one"]
        assert handler.get("one", "pk") == {1}
    finally:
        handler.close()


def test_status_report_lists_the_cache(prefix):
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
        assert handler.set_cache("one", {1, 2}, "pk") is True
        handler.db.commit()

        handler.cursor.execute(STATUS_SQL.read_text())
        # No queue with this prefix exists: the report then describes the cache alone
        handler.cursor.execute(
            "SELECT partition_key, cache_architecture, cache_exists FROM partitioncache_get_queue_and_cache_info(%s, %s)",
            (prefix, prefix + "_q"),
        )
        assert handler.cursor.fetchall() == [("pk", "bit", True)]
        handler.db.rollback()
    finally:
        handler.close()

"""Partition keys whose cache table names PostgreSQL truncates to the same name.

PostgreSQL truncates identifiers to 63 bytes without splitting a character, and without an error. A cache table name
<prefix>_cache_<partition_key> that is longer works: it is created, read and written under the truncated name. Two
partition keys whose names share their first 63 bytes would share one table, and a hash cached under one key would be
read for the other. The registration of a new key that would share a registered key's table is refused, by the Python
handlers and by partitioncache_bootstrap_partition (which the PL/pgSQL queue processor calls); everything else works
as before.
"""

import os
import uuid

import psycopg
import pytest
from psycopg import sql

from partitioncache.cache_handler.postgresql_abstract import SHARED_CACHE_TABLE_SQLSTATE

# "<prefix>_cache_" is 24 bytes for the 17-byte prefixes below; the keys' names exceed 63 bytes
LONG_A = "sensor_readings_" + "x" * 23 + "_a"  # 24 + 41 = 65 bytes; truncated: 24 + the first 39 key bytes
LONG_B = "sensor_readings_" + "x" * 23 + "_b"
FITS_63 = "sensor_readings_" + "y" * 23  # 24 + 39 = 63 bytes, not truncated
# 23 ASCII bytes after the prefix, then "ü" (2 bytes) at bytes 63-64: PostgreSQL drops the whole character (62 bytes)
MULTIBYTE_A = "sensor_readings_" + "z" * 22 + "üa"
MULTIBYTE_B = "sensor_readings_" + "z" * 22 + "üb"


def _params() -> dict:
    return {
        "db_name": os.getenv("PG_DBNAME", "partitioncache_integration"),
        "db_host": os.getenv("PG_HOST", "localhost"),
        "db_user": os.getenv("PG_USER", "integration_user"),
        "db_password": os.getenv("PG_PASSWORD", "integration_password"),
        "db_port": os.getenv("PG_PORT", "5432"),
    }


def _make(backend: str, prefix: str):
    if backend == "array":
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        return PostgreSQLArrayCacheHandler(**_params(), db_tableprefix=prefix)
    if backend == "bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

        return PostgreSQLBitCacheHandler(**_params(), db_tableprefix=prefix, bitsize=64)
    from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

    return PostgreSQLRoaringBitCacheHandler(**_params(), db_tableprefix=prefix)


def _drop(db_session, prefix: str) -> None:
    with db_session.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND starts_with(tablename::text, %s)", (prefix,))
        for (table,) in cur.fetchall():
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))


@pytest.fixture(params=["array", "bit", "roaringbit"])
def backend(request, db_session):
    if request.param == "roaringbit":
        with db_session.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'roaringbitmap'")
            if cur.fetchone() is None:
                pytest.skip("roaringbitmap extension not available")
    return request.param


@pytest.fixture
def prefix(db_session):
    name = f"name_len_{uuid.uuid4().hex[:8]}"  # 17 bytes
    yield name
    _drop(db_session, name)


@pytest.fixture
def handler(backend, prefix):
    h = _make(backend, prefix)
    yield h
    h.close()


def _keys(value) -> set | None:
    return None if value is None else set(value)


def _metadata_keys(db_session, prefix: str) -> set[str]:
    with db_session.cursor() as cur:
        cur.execute(sql.SQL("SELECT partition_key FROM {}").format(sql.Identifier(f"{prefix}_partition_metadata")))
        return {row[0] for row in cur.fetchall()}


@pytest.mark.parametrize(("first", "second"), [(LONG_A, LONG_B), (MULTIBYTE_A, MULTIBYTE_B)], ids=["ascii", "multibyte"])
def test_a_new_key_sharing_a_registered_keys_table_is_refused(handler, db_session, prefix, first, second):
    handler.register_partition_key(first, "integer")
    assert handler.set_cache("h1", {1, 2}, first)

    with pytest.raises(ValueError) as refused:
        handler.register_partition_key(second, "integer")
    message = str(refused.value)
    assert first in message and second in message
    with db_session.cursor() as cur:
        cur.execute("SELECT (%s || '_cache_' || %s)::name::text", (prefix, first))
        shared = cur.fetchone()[0]
    assert shared in message

    # Storing under the refused key registers it first, and is refused the same way
    try:
        stored = handler.set_cache("h2", {3}, second)
    except ValueError:
        stored = False
    assert stored is False
    assert handler.set_cache_lazy("h3", f'SELECT 4 AS "{second}"', second) is False

    assert _metadata_keys(db_session, prefix) == {first}
    assert _keys(handler.get("h1", first)) == {1, 2}
    assert handler.get("h2", first) is None and handler.get("h3", first) is None
    assert handler.get("h1", second) is None, "the first key's entry must not be read under the second key"
    assert handler.get_intersected({"h1"}, second) == (None, 0)


def test_a_single_long_key_registers_stores_and_reads(handler, db_session, prefix):
    handler.register_partition_key(LONG_A, "integer")
    handler.register_partition_key(LONG_A, "integer")  # again: it is registered, nothing is refused
    assert handler.set_cache("h1", {1, 2, 3}, LONG_A)
    assert handler.set_cache("h2", {2, 3, 9}, LONG_A)
    assert handler.set_cache_lazy("h3", f'SELECT 3 AS "{LONG_A}" UNION ALL SELECT 2', LONG_A)
    assert _keys(handler.get("h1", LONG_A)) == {1, 2, 3}
    result, count = handler.get_intersected({"h1", "h2", "h3"}, LONG_A)
    assert (_keys(result), count) == ({2, 3}, 3)
    assert handler.exists("h1", LONG_A)

    handler.register_partition_key(FITS_63, "integer")  # a name of exactly 63 bytes collides with nothing
    assert _metadata_keys(db_session, prefix) == {LONG_A, FITS_63}


def test_keys_registered_before_keep_working_also_when_they_share_a_table(backend, db_session, prefix):
    """A long key, and two keys that already share a table, registered as before the check: both work as before."""
    first = _make(backend, prefix)
    try:
        first.register_partition_key(LONG_A, "integer")
        assert first.set_cache("h1", {1, 2}, LONG_A)
    finally:
        first.close()
    # The second key's metadata row, as the bootstrap wrote it before the check (the table was already there)
    with db_session.cursor() as cur:
        columns, values = ("partition_key, datatype, bitsize", (LONG_B, "integer", 64)) if backend == "bit" else ("partition_key, datatype", (LONG_B, "integer"))
        cur.execute(
            sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(f"{prefix}_partition_metadata"), sql.SQL(columns), sql.SQL(", ").join(sql.Placeholder() * len(values))
            ),
            values,
        )

    handler = _make(backend, prefix)
    try:
        handler.register_partition_key(LONG_A, "integer")
        handler.register_partition_key(LONG_B, "integer")
        assert _keys(handler.get("h1", LONG_A)) == {1, 2}
        assert handler.set_cache("h4", {5}, LONG_A)
        assert _keys(handler.get("h4", LONG_A)) == {5}
        assert _keys(handler.get("h1", LONG_B)) == {1, 2}  # one table, as before: the defect the check prevents for new keys
    finally:
        handler.close()


def test_the_sql_bootstrap_refuses_only_a_new_key_sharing_a_table(handler, db_session, prefix):
    handler.register_partition_key(LONG_A, "integer")  # loads the SQL functions and creates the metadata table
    backend = {"postgresql_array": "array", "postgresql_bit": "bit", "postgresql_roaringbit": "roaringbit"}[repr(handler)]
    bitsize = 64 if backend == "bit" else None
    bootstrap = "SELECT partitioncache_bootstrap_partition(%s, %s, 'integer', %s, %s)"

    with db_session.cursor() as cur:
        with pytest.raises(psycopg.Error) as refused:
            cur.execute(bootstrap, (prefix, LONG_B, backend, bitsize))
        assert refused.value.sqlstate == SHARED_CACHE_TABLE_SQLSTATE
        assert LONG_A in str(refused.value) and LONG_B in str(refused.value)

        cur.execute(bootstrap, (prefix, LONG_A, backend, bitsize))  # registered: as before
        assert cur.fetchone()[0] is True
        cur.execute(bootstrap, (prefix, "sensor_" + "q" * 60, backend, bitsize))  # long, but shares nothing
        assert cur.fetchone()[0] is True
    assert _metadata_keys(db_session, prefix) == {LONG_A, "sensor_" + "q" * 60}


def test_the_bbox_handler_refuses_a_new_key_sharing_a_table(db_session, prefix):
    with db_session.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        if cur.fetchone() is None:
            pytest.skip("PostGIS not installed")
    from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

    handler = PostGISBBoxCacheHandler(**_params(), db_tableprefix=prefix)
    try:
        handler.register_partition_key(LONG_A, "geometry")
        handler.register_partition_key(LONG_A, "geometry")
        with pytest.raises(ValueError, match=LONG_B):
            handler.register_partition_key(LONG_B, "geometry")
        assert _metadata_keys(db_session, prefix) == {LONG_A}
    finally:
        handler.close()

"""Cache table names longer than PostgreSQL's 63-byte identifier limit are refused, in Python and in the SQL bootstrap.

PostgreSQL truncates longer identifiers without an error, so two partition keys with a long common beginning shared
one cache table. A name of exactly 63 bytes keeps working, also where names derived from it (the array handler's index
name) are longer and truncated.
"""

import os
import uuid

import psycopg
import pytest
from psycopg import sql

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler


def _params() -> dict:
    return {
        "db_name": os.getenv("PG_DBNAME", "partitioncache_integration"),
        "db_host": os.getenv("PG_HOST", "localhost"),
        "db_user": os.getenv("PG_USER", "integration_user"),
        "db_password": os.getenv("PG_PASSWORD", "integration_password"),
        "db_port": os.getenv("PG_PORT", "5432"),
    }


@pytest.fixture(params=["array", "bit"])
def handler(request, db_session):
    prefix = f"name_len_{uuid.uuid4().hex[:8]}"  # 17 bytes; "<prefix>_cache_" is 24 bytes
    if request.param == "array":
        h = PostgreSQLArrayCacheHandler(**_params(), db_tableprefix=prefix)
    else:
        h = PostgreSQLBitCacheHandler(**_params(), db_tableprefix=prefix, bitsize=64)
    yield h
    h.close()
    with db_session.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE %s", (prefix + "%",))
        for (table,) in cur.fetchall():
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))


def test_a_63_byte_table_name_works_and_a_64_byte_one_is_refused(handler):
    fits = "sensor_" + "x" * 32  # 24 + 39 = 63 bytes
    shared_beginning = fits + "_b"
    assert len(f"{handler.tableprefix}_cache_{fits}".encode()) == 63

    handler.register_partition_key(fits, "integer")
    assert handler.set_cache("h1", {1, 2}, fits)
    assert set(handler.get("h1", fits)) == {1, 2}

    with pytest.raises(ValueError, match="63 bytes"):
        handler.register_partition_key(shared_beginning, "integer")
    # Truncated, its table would be the one of `fits`, which holds h1
    with pytest.raises(ValueError, match="63 bytes"):
        handler.get("h1", shared_beginning)
    assert handler.set_cache("h2", {3}, shared_beginning) is False
    assert handler.get("h2", fits) is None


def test_the_sql_bootstrap_refuses_a_too_long_table_name(handler, db_session):
    handler.register_partition_key("zipcode", "integer")  # loads the SQL functions
    too_long = "z" * 40  # 24 + 40 = 64 bytes
    with pytest.raises(psycopg.errors.RaiseException, match="63 bytes"):
        with db_session.cursor() as cur:
            cur.execute("SELECT partitioncache_bootstrap_partition(%s, %s, 'integer', 'array')", (handler.tableprefix, too_long))
    with db_session.cursor() as cur:
        cur.execute("SELECT to_regclass(quote_ident(%s))", (f"{handler.tableprefix}_cache_{too_long}"[:63],))
        assert cur.fetchone()[0] is None

"""PostgreSQL handlers after a failed read, and materialised intersections whose entries vanish during the call.

A read that fails (here: a lock timeout while another session holds the table) aborts the handler's transaction.
Before the fix every later statement on the connection failed until a write rolled back, and the reads returned
"unknown": the cache was off for the rest of a read-only run.
"""

import os
import uuid

import psycopg
import pytest
from psycopg import sql

HANDLERS = ["postgresql_array", "postgresql_bit", "postgresql_roaringbit"]


def _params() -> dict:
    return {
        "db_name": os.getenv("PG_DBNAME", "partitioncache_integration"),
        "db_host": os.getenv("PG_HOST", "localhost"),
        "db_user": os.getenv("PG_USER", "integration_user"),
        "db_password": os.getenv("PG_PASSWORD", "integration_password"),
        "db_port": os.getenv("PG_PORT", "5432"),
    }


def _make_handler(backend: str, prefix: str):
    if backend == "postgresql_array":
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        return PostgreSQLArrayCacheHandler(**_params(), db_tableprefix=prefix)
    if backend == "postgresql_bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

        return PostgreSQLBitCacheHandler(**_params(), db_tableprefix=prefix, bitsize=64)
    from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

    return PostgreSQLRoaringBitCacheHandler(**_params(), db_tableprefix=prefix)


@pytest.fixture(params=HANDLERS)
def handler(request, db_session):
    if request.param == "postgresql_roaringbit":
        with db_session.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'roaringbitmap'")
            if cur.fetchone() is None:
                pytest.skip("roaringbitmap extension not available")
    prefix = f"failed_reads_{uuid.uuid4().hex[:8]}"
    h = _make_handler(request.param, prefix)
    h.register_partition_key("zipcode", "integer")
    assert h.set_cache("h1", {1, 2, 3}, "zipcode")
    assert h.set_cache("h2", {2, 3, 9}, "zipcode")
    yield h
    h.close()
    with db_session.cursor() as cur:
        for suffix in ("cache_zipcode", "partition_metadata", "queries"):
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(f"{prefix}_{suffix}")))


@pytest.fixture
def locker(db_session):
    """A second session that takes a table lock in its own transaction; the handler waits at most 200 ms for it."""
    conn = psycopg.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        user=os.getenv("PG_USER", "integration_user"),
        password=os.getenv("PG_PASSWORD", "integration_password"),
        dbname=os.getenv("PG_DBNAME", "partitioncache_integration"),
    )

    def lock(table: str) -> None:
        conn.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier(table)))

    yield lock, conn.rollback
    conn.close()


def _set_lock_timeout(handler) -> None:
    handler.db.execute("SET lock_timeout = '200ms'")
    handler.db.commit()


def _as_set(value):
    return None if value is None else set(value)


def test_reads_work_again_after_a_read_failed(handler, locker):
    lock, release = locker
    _set_lock_timeout(handler)
    prefix = handler.tableprefix

    lock(f"{prefix}_cache_zipcode")
    assert handler.get("h1", "zipcode") is None
    assert handler.get_intersected({"h1", "h2"}, "zipcode") == (None, 0)
    assert handler.filter_existing_keys({"h1", "h2"}, "zipcode") == set()
    release()

    assert _as_set(handler.get("h1", "zipcode")) == {1, 2, 3}
    result, count = handler.get_intersected({"h1", "h2"}, "zipcode")
    assert (_as_set(result), count) == ({2, 3}, 2)


def test_a_read_fails_inside_a_transaction_an_earlier_read_left_open(handler, locker):
    """get leaves its read transaction open; the shared read methods ended only a transaction they opened."""
    lock, release = locker
    _set_lock_timeout(handler)
    prefix = handler.tableprefix

    assert _as_set(handler.get("h1", "zipcode")) == {1, 2, 3}
    lock(f"{prefix}_queries")
    assert handler.get_query_status("h1", "zipcode") is None
    release()
    assert handler.get_query_status("h1", "zipcode") == "ok"
    assert handler.get_query("h1", "zipcode") == ""

    assert _as_set(handler.get("h1", "zipcode")) == {1, 2, 3}
    lock(f"{prefix}_partition_metadata")
    assert handler.get_partition_keys() == []
    release()
    assert [key for key, _ in handler.get_partition_keys()] == ["zipcode"]


def _deleting_before(handler, db_session, method: str, deleted: list[str]):
    """Replace handler.<method> (called between the existence check and the intersection) with one that first deletes
    the given entries in another session, as eviction or another process's delete would."""
    original = getattr(handler, method)
    table = sql.Identifier(f"{handler.tableprefix}_cache_zipcode")

    def deleting(*args, **kwargs):
        with db_session.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {} WHERE query_hash = ANY(%s)").format(table), (deleted,))
        return original(*args, **kwargs)

    setattr(handler, method, deleting)


def test_an_intersection_whose_entries_were_deleted_during_the_call_is_unknown(handler, db_session):
    _deleting_before(handler, db_session, "get_intersected_sql", ["h1", "h2"])
    assert handler.get_intersected({"h1", "h2"}, "zipcode") == (None, 0)


@pytest.mark.parametrize("deleted", [["h2"], ["h1", "h2"]], ids=["one_entry", "every_entry"])
def test_a_dilated_intersection_with_entries_deleted_during_the_call_is_not_empty(handler, db_session, deleted):
    method = {"postgresql_array": "get_intersected_dilated_sql", "postgresql_bit": "get_intersected_dilated_sql_wk"}.get(repr(handler))
    if method is None:
        pytest.skip(f"{handler!r} has no dilated intersection")
    result, count = handler.get_intersected_dilated({"h1", "h2"}, "zipcode", dilation=1)
    assert (set(result), count) == ({1, 2, 3, 4}, 2)

    _deleting_before(handler, db_session, method, deleted)
    result, count = handler.get_intersected_dilated({"h1", "h2"}, "zipcode", dilation=1)
    # Unknown, or the intersection over the entries that are left (h1 dilated is {0, ..., 4}); never a lost key
    assert result is None or {1, 2, 3, 4} <= set(result), result

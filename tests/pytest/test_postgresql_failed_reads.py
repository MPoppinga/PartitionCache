"""A failed read must not leave the handler's connection in an aborted transaction.

After one failed statement (statement timeout, cancel, lock timeout) every later statement on the connection raised
"current transaction is aborted" until a write happened to roll back. Reads swallowed that and returned "unknown", so
in a read-only run the cache was silently off. The fake connection here keeps the transaction status as PostgreSQL
would: a failed statement aborts the transaction, and every later statement fails until a rollback.
"""

import re
from types import SimpleNamespace

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

ENTRIES = {"h1": [1, 2, 3], "h2": [2, 3]}


class _Connection:
    """Transaction status like a psycopg connection; ``fail`` makes the next statement containing it fail."""

    def __init__(self):
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.closed = False
        self.fail: str | None = None

    def cursor(self):
        return _Cursor(self)

    def rollback(self):
        self.info.transaction_status = TransactionStatus.IDLE

    def commit(self):
        self.info.transaction_status = TransactionStatus.IDLE


class _Cursor:
    # An adaptation context for Composable.as_string(cursor), without a server connection
    adapters = psycopg.adapters
    connection = None

    def __init__(self, conn: _Connection):
        self.conn = conn
        self.rows: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else query.as_string(None)
        if self.conn.info.transaction_status == TransactionStatus.INERROR:
            raise psycopg.errors.InFailedSqlTransaction("current transaction is aborted, commands ignored until end of transaction block")
        self.conn.info.transaction_status = TransactionStatus.INTRANS
        if self.conn.fail and self.conn.fail in text:
            self.conn.fail = None
            self.conn.info.transaction_status = TransactionStatus.INERROR
            raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
        self.rows = _respond(text, params)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


def _respond(text: str, params) -> list:
    named = [k for k in ENTRIES if re.search(rf"\b{k}\b", text)]
    if "txid_current_if_assigned" in text:
        return [(False,)]
    if "SELECT partition_keys FROM" in text:
        return [(ENTRIES[params[0]],)] if params[0] in ENTRIES else []
    if "SELECT query_hash FROM" in text and "ANY(%s)" in text:
        return [(k,) for k in params[0] if k in ENTRIES]
    if "array_intersect_agg" in text:
        return [(sorted(set.intersection(*(set(ENTRIES[k]) for k in named))),)]
    if "generate_series" in text:  # dilated intersection, dilation 1, over entries that are all present
        dilated = [{v + d for v in ENTRIES[k] for d in (-1, 0, 1)} for k in named]
        keys = sorted(set.intersection(*dilated))
        return [(True, keys)] if text.lstrip().upper().startswith("SELECT (") else [(v,) for v in keys]
    if "SELECT status FROM" in text:
        return [("ok",)] if params[0] in ENTRIES else []
    if "SELECT query FROM" in text:
        return [("SELECT 1",)] if params[0] in ENTRIES else []
    if "SELECT partition_key, datatype FROM" in text:
        return [("k", "integer")]
    if "SELECT query_hash, query FROM" in text:
        return [(k, "SELECT 1") for k in ENTRIES]
    if "SELECT query_hash FROM" in text:
        return [(k,) for k in ENTRIES]
    if "SELECT count(*) FROM" in text:
        return [(2,)]
    raise AssertionError(f"unexpected statement: {text}")


@pytest.fixture
def handler():
    h = PostgreSQLArrayCacheHandler.__new__(PostgreSQLArrayCacheHandler)
    h.db = _Connection()
    h.cursor = h.db.cursor()
    h.tableprefix = "t"
    h._cached_datatype = {"k": "integer"}
    h.array_intersect_agg_available = True
    return h


READS = [
    ("get", lambda h: h.get("h1", "k"), "SELECT partition_keys FROM", None, {1, 2, 3}),
    ("get_intersected", lambda h: h.get_intersected({"h1", "h2"}, "k"), "array_intersect_agg", (None, 0), ({2, 3}, 2)),
    ("get_intersected_lookup", lambda h: h.get_intersected({"h1", "h2"}, "k"), "ANY(%s)", (None, 0), ({2, 3}, 2)),
    ("get_intersected_dilated", lambda h: h.get_intersected_dilated({"h1", "h2"}, "k", 1), "generate_series", (None, 0), ({1, 2, 3, 4}, 2)),
    ("filter_existing_keys", lambda h: h.filter_existing_keys({"h1", "h3"}, "k"), "ANY(%s)", set(), {"h1"}),
    ("get_query_status", lambda h: h.get_query_status("h1", "k"), "SELECT status FROM", None, "ok"),
    ("get_query", lambda h: h.get_query("h1", "k"), "SELECT query FROM", None, "SELECT 1"),
    ("get_partition_keys", lambda h: h.get_partition_keys(), "SELECT partition_key, datatype", [], [("k", "integer")]),
    ("get_all_queries", lambda h: h.get_all_queries("k"), "SELECT query_hash, query", [], [("h1", "SELECT 1"), ("h2", "SELECT 1")]),
]


@pytest.mark.parametrize("after_open_read", [False, True], ids=["idle", "after_a_read_left_open"])
@pytest.mark.parametrize(("read", "failing", "unknown", "expected"), [r[1:] for r in READS], ids=[r[0] for r in READS])
def test_a_failed_read_returns_unknown_and_later_reads_work(handler, after_open_read, read, failing, unknown, expected):
    if after_open_read:
        # get leaves its read transaction open; a read that fails next is inside a transaction it did not open
        assert handler.get("h2", "k") == {2, 3}
        assert handler.db.info.transaction_status == TransactionStatus.INTRANS

    handler.db.fail = failing
    assert read(handler) == unknown
    assert handler.db.fail is None, "the statement meant to fail did not run"
    assert handler.db.info.transaction_status != TransactionStatus.INERROR

    assert read(handler) == expected
    assert handler.get("h1", "k") == {1, 2, 3}


@pytest.mark.parametrize(
    ("read", "expected"),
    [(lambda h: h.get_all_keys("k"), ["h1", "h2"]), (lambda h: h.count_lazy_keys("SELECT 1 AS k"), 2)],
    ids=["get_all_keys", "count_lazy_keys"],
)
def test_a_read_ends_the_transaction_it_opened(handler, read, expected):
    assert read(handler) == expected
    assert handler.db.info.transaction_status == TransactionStatus.IDLE

"""Unit tests for the PostgreSQL db_handler result-set walking helper.

These cover ``fetch_final_result_set``, which makes the multi-statement temp-table
integration scripts (``CREATE TEMPORARY TABLE ... ON COMMIT DROP; ...; SELECT``) return
the rows of the FINAL statement rather than the first. The tests use a fake cursor so
they require no database connection.
"""

from partitioncache.db_handler.postgres import fetch_final_result_set


class FakeCursor:
    """Minimal stand-in for a psycopg cursor that exposes a sequence of result sets.

    Each result set is a tuple ``(description, rows)``. ``description`` is ``None`` for
    statements that produce no rows (CREATE TABLE, INSERT, ANALYZE) and a truthy value
    for SELECTs. ``nextset()`` advances to the next set and returns ``True``; at the end
    it returns ``None`` (matching psycopg semantics).
    """

    def __init__(self, result_sets):
        self._sets = result_sets
        self._idx = 0

    @property
    def description(self):
        return self._sets[self._idx][0]

    def fetchall(self):
        return self._sets[self._idx][1]

    def nextset(self):
        if self._idx < len(self._sets) - 1:
            self._idx += 1
            return True
        return None


class TestFetchFinalResultSet:
    def test_multi_statement_returns_final_select(self):
        """CREATE/INSERT/ANALYZE produce no rows; the trailing SELECT's rows are returned."""
        cur = FakeCursor(
            [
                (None, []),  # CREATE TEMPORARY TABLE ... ON COMMIT DROP
                (None, []),  # INSERT
                (None, []),  # ANALYZE
                (["partition_key"], [(10,), (20,), (30,)]),  # SELECT
            ]
        )
        assert fetch_final_result_set(cur) == [(10,), (20,), (30,)]

    def test_single_statement(self):
        """A plain single SELECT still returns its rows."""
        cur = FakeCursor([(["x"], [(42,)])])
        assert fetch_final_result_set(cur) == [(42,)]

    def test_no_result_producing_statement(self):
        """A script that ends without a SELECT yields an empty list, never raises."""
        cur = FakeCursor([(None, []), (None, [])])
        assert fetch_final_result_set(cur) == []

    def test_last_select_wins_over_earlier_select(self):
        """If multiple statements produce rows, only the final result set is returned."""
        cur = FakeCursor(
            [
                (["a"], [(1,)]),  # an earlier SELECT (should be skipped)
                (None, []),  # INSERT
                (["b"], [(99,)]),  # final SELECT
            ]
        )
        assert fetch_final_result_set(cur) == [(99,)]


class TestExecuteKeepsFalsyKeys:
    """execute drops only NULL: key 0 is a partition like any other."""

    def test_key_zero_and_empty_string_kept(self):
        from unittest.mock import MagicMock

        from partitioncache.db_handler.postgres import PostgresDBHandler

        handler = PostgresDBHandler.__new__(PostgresDBHandler)
        handler.conn = MagicMock()
        handler.cur = MagicMock()
        handler.cur.execute = MagicMock()
        fake = FakeCursor([(["pk"], [(0,), (5,), (None,), ("",)])])
        handler.cur.description = fake.description
        handler.cur.fetchall = fake.fetchall
        handler.cur.nextset = fake.nextset

        assert handler.execute("SELECT pk FROM t") == [0, 5, ""]

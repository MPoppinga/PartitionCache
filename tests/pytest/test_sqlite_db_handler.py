"""SQLiteDBHandler.execute: errors raise and falsy keys survive, so callers never cache a failure as "no rows"."""

import sqlite3

import pytest

from partitioncache.db_handler.sqlite import SQLiteDBHandler


@pytest.fixture
def handler(tmp_path):
    path = str(tmp_path / "t.db")
    conn = sqlite3.connect(path)
    conn.executescript("CREATE TABLE t (pk INTEGER); INSERT INTO t VALUES (0), (5), (NULL);")
    conn.close()
    h = SQLiteDBHandler(path)
    yield h
    h.close()


def test_key_zero_kept_and_null_dropped(handler):
    assert sorted(handler.execute("SELECT pk FROM t")) == [0, 5]


def test_zero_rows_is_an_empty_list(handler):
    assert handler.execute("SELECT pk FROM t WHERE pk > 100") == []


def test_error_raises_instead_of_returning_empty(handler):
    with pytest.raises(sqlite3.Error):
        handler.execute("SELECT pk FROM missing_table")

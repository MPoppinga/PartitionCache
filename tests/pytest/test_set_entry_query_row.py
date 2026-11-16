"""set_entry writes the query row only after the cache row was stored.

A query row reads as done (status 'ok'): the PL/pgSQL queue processor skips any hash that has one. Written after a
failed store, it left the hash without a cache row for good. set_entry_lazy has the same guarantee
(test_lazy_insertion_key_table_scripts.py).
"""

import pytest

from partitioncache.cache_handler.abstract import AbstractCacheHandler


class _FailingStore(AbstractCacheHandler):
    """A handler whose set_cache fails (returns False or raises); set_query records the calls."""

    def __init__(self, failure):
        self.failure = failure
        self.queries = []

    def set_cache(self, key, partition_key_identifiers, partition_key="partition_key"):
        if isinstance(self.failure, Exception):
            raise self.failure
        return self.failure

    def set_query(self, key, querytext, partition_key="partition_key"):
        self.queries.append(key)
        return True

    def exists(self, key, partition_key="partition_key", check_query=False):
        return False

    def __getattr__(self, name):  # the abstract interface's other methods are not called here
        raise AttributeError(name)


_FailingStore.__abstractmethods__ = frozenset()


@pytest.mark.parametrize("failure", [False, RuntimeError("statement timeout")], ids=["returns_false", "raises"])
def test_set_entry_writes_no_query_row_without_its_cache_row(failure):
    handler = _FailingStore(failure)
    assert handler.set_entry("h", {1, 2}, "SELECT 1", "k") is False
    assert handler.queries == []


def test_set_entry_writes_the_query_row_after_a_stored_cache_row():
    handler = _FailingStore(True)
    assert handler.set_entry("h", {1, 2}, "SELECT 1", "k") is True
    assert handler.queries == ["h"]

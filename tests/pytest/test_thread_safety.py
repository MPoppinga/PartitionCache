"""
ThreadSafeHandler: one lock per handler instance around every public method (cache_handler/thread_safety.py).

Threads that shared a PostgreSQL handler lost writes: ``_read_txn``'s rollback in one thread discarded another
thread's pending INSERT, whose set_cache still returned True. These tests check the mechanism without a database;
tests/integration/test_shared_handler_threads.py checks the stored rows of every backend.
"""

import inspect
import threading
import time
import types

import pytest

from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler
from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler
from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler
from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler
from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler
from partitioncache.cache_handler.thread_safety import ThreadSafeHandler

HANDLERS = [
    PostgreSQLAbstractCacheHandler,
    PostgreSQLArrayCacheHandler,
    PostgreSQLBitCacheHandler,
    PostgreSQLRoaringBitCacheHandler,
    PostGISSpatialAbstractCacheHandler,
    PostGISBBoxCacheHandler,
    DuckDBBitCacheHandler,
]


@pytest.mark.parametrize("cls", HANDLERS, ids=lambda cls: cls.__name__)
def test_every_public_method_is_locked(cls):
    public = [name for name in dir(cls) if not name.startswith("_")]
    functions = {name: inspect.getattr_static(cls, name) for name in public}
    functions = {name: value for name, value in functions.items() if isinstance(value, types.FunctionType)}
    assert functions, cls
    unlocked = sorted(name for name, value in functions.items() if not getattr(value, "__pcache_locked__", False))
    assert unlocked == [], f"{cls.__name__}: {unlocked}"
    # classmethods (get_instance) and staticmethods are not instance calls and stay unwrapped
    assert not getattr(inspect.getattr_static(cls, "get_instance").__func__, "__pcache_locked__", False)


class _Recorder(ThreadSafeHandler):
    """A handler without a database: records how many threads are inside its public methods at once."""

    def __init__(self):
        self.inside = 0
        self.most_inside = 0
        self.pending = False

    def work(self, seconds: float = 0.01) -> None:
        self.inside += 1
        self.most_inside = max(self.most_inside, self.inside)
        time.sleep(seconds)
        self.inside -= 1

    def outer(self) -> str:
        self.work(0)  # a public method calling another one: the lock is re-entrant
        return "done"

    def leave_pending(self) -> None:
        self.pending = True

    def nested_pending(self) -> None:
        self.leave_pending()  # the inner call returns with the write pending; only the outermost exit is checked
        self.pending = False

    def _has_pending_write(self) -> bool:
        return self.pending


def test_a_new_subclass_is_locked_without_a_decorator():
    class Child(_Recorder):
        def extra(self) -> int:
            return 1

    assert Child.extra.__pcache_locked__
    assert Child().extra() == 1


def test_public_methods_of_one_instance_run_one_at_a_time():
    handler = _Recorder()
    threads = [threading.Thread(target=handler.work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert handler.most_inside == 1


def test_two_instances_do_not_wait_for_each_other():
    first, second = _Recorder(), _Recorder()
    assert first._pcache_lock is not second._pcache_lock


def test_the_lock_is_reentrant():
    assert _Recorder().outer() == "done"


def test_a_pending_write_on_return_raises_when_checked(monkeypatch):
    monkeypatch.setattr(ThreadSafeHandler, "check_pending_writes", True)
    handler = _Recorder()
    with pytest.raises(AssertionError, match="leave_pending returned with a pending write"):
        handler.leave_pending()
    handler.pending = False
    handler.nested_pending()  # cleared before the outermost call returns


def test_unchecked_a_pending_write_does_not_raise(monkeypatch):
    monkeypatch.setattr(ThreadSafeHandler, "check_pending_writes", False)
    _Recorder().leave_pending()


def test_postgresql_pending_write_check_treats_a_mock_connection_as_idle():
    from unittest.mock import MagicMock

    handler = object.__new__(PostgreSQLArrayCacheHandler)
    handler.db = MagicMock()
    assert handler._has_pending_write() is False


def test_duckdb_shared_by_threads_stores_every_write(tmp_path):
    """Before the lock, threads sharing one DuckDB handler crashed the process (segmentation fault)."""
    pytest.importorskip("duckdb")
    from concurrent.futures import ThreadPoolExecutor

    handler = DuckDBBitCacheHandler(database=str(tmp_path / "shared.duckdb"), table_prefix="thr", bitsize=10000)
    handler.register_partition_key("zipcode", "integer", bitsize=10000)

    def worker(thread: int) -> list[str]:
        return [f"w_{thread}_{i}" for i in range(25) if handler.set_cache(f"w_{thread}_{i}", {thread * 100 + i}, "zipcode")]

    with ThreadPoolExecutor(8) as pool:
        written = [key for keys in pool.map(worker, range(8)) for key in keys]
    assert len(written) == 200
    assert all(handler.get(key, "zipcode") == {int(key.split("_")[1]) * 100 + int(key.split("_")[2])} for key in written)
    with ThreadPoolExecutor(8) as pool:
        reads = list(pool.map(lambda key: handler.get(key, "zipcode"), written))
    assert all(reads)
    handler.close()

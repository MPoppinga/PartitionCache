"""
Threads that share one cache handler instance: every write that returns True is stored, with exactly its keys.

Before the instance lock (cache_handler/thread_safety.py) a postgresql_bit handler shared by 4 threads lost about 40 %
of its set_cache writes although they returned True: one thread's read rollback (``_read_txn``) discarded another
thread's pending INSERT, whose queries row was then committed alone. The old test here only required a success rate
above 95 % and could not see it. These tests count the rows instead, for every backend of the integration run, and
for the pattern of pcache-monitor's worker threads (a handler from get_cache_handler per work item).
"""

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.cache_handler.helper import PartitionCacheHelper

from .utils import normalize_cache_result

WRITES = 200


def _partition_key() -> str:
    """A partition key of its own per test, so that the counts below see only this test's entries."""
    return f"thr_{uuid.uuid4().hex[:8]}"


def _register(handler: AbstractCacheHandler, partition_key: str) -> None:
    kwargs = {"bitsize": 200000} if "bit" in type(handler).__name__.lower() else {}
    handler.register_partition_key(partition_key, "integer", **kwargs)


def _assert_stored(handler: AbstractCacheHandler, partition_key: str, written: dict[str, set[int]]) -> None:
    """Every written entry is stored with exactly its keys, and nothing else is stored under the partition key."""
    wrong = {key: normalize_cache_result(handler.get(key, partition_key)) for key, keys in written.items()}
    wrong = {key: got for key, got in wrong.items() if got != written[key]}
    assert wrong == {}, f"{len(wrong)} of {len(written)} writes returned True but are not stored as written"
    assert sorted(handler.get_all_keys(partition_key)) == sorted(written)
    if hasattr(handler, "get_all_queries") and "postgresql" in type(handler).__name__.lower():
        assert sorted(query_hash for query_hash, _ in handler.get_all_queries(partition_key)) == sorted(written)


def _write_concurrently(write, threads: int) -> dict[str, set[int]]:
    """Run WRITES writes on ``threads`` threads; every write must return True."""
    per_thread = WRITES // threads

    def worker(thread: int) -> tuple[dict[str, set[int]], list[str]]:
        done, failed = {}, []
        for i in range(per_thread):
            key, keys = f"w_{thread}_{i}", {thread * 1000 + i, 199999 - thread * 1000 - i}
            (done.__setitem__(key, keys) if write(key, keys) else failed.append(key))
        return done, failed

    with ThreadPoolExecutor(threads) as pool:
        results = list(pool.map(worker, range(threads)))
    failed = [key for _, f in results for key in f]
    assert failed == [], f"{len(failed)} writes returned False"
    return {key: keys for done, _ in results for key, keys in done.items()}


@pytest.mark.parametrize("threads", [4, 10])
def test_set_cache_on_a_shared_instance(cache_client: AbstractCacheHandler, threads: int):
    partition_key = _partition_key()
    _register(cache_client, partition_key)
    written = _write_concurrently(lambda key, keys: cache_client.set_cache(key, keys, partition_key), threads)
    _assert_stored(cache_client, partition_key, written)


@pytest.mark.parametrize("threads", [4, 10])
def test_set_cache_lazy_on_a_shared_instance(cache_client: AbstractCacheHandler, threads: int):
    """The lazy storage path (the monitor's lazy insertion) runs the key query in the database."""
    if not isinstance(cache_client, AbstractCacheHandler_Lazy) or "postgresql" not in type(cache_client).__name__.lower():
        pytest.skip("set_cache_lazy runs the key query in PostgreSQL")
    partition_key = _partition_key()
    _register(cache_client, partition_key)

    def write(key: str, keys: set[int]) -> bool:
        query = " UNION ALL ".join(f"SELECT {k} AS {partition_key}" for k in sorted(keys))
        return cache_client.set_cache_lazy(key, query, partition_key)

    _assert_stored(cache_client, partition_key, _write_concurrently(write, threads))


def test_set_entry_through_a_shared_helper(cache_client: AbstractCacheHandler):
    """set_entry writes the cache row and the queries row; under the instance lock the pair is one exclusive call."""
    partition_key = _partition_key()
    _register(cache_client, partition_key)
    helper = PartitionCacheHelper(cache_client, partition_key, "integer")
    written = _write_concurrently(lambda key, keys: helper.set_entry(key, keys, f"SELECT {min(keys)} AS {partition_key}"), 4)
    _assert_stored(cache_client, partition_key, written)
    assert all(cache_client.get_query(key, partition_key) for key in written)


def test_reads_on_a_shared_instance_return_the_stored_sets(cache_client: AbstractCacheHandler):
    partition_key = _partition_key()
    _register(cache_client, partition_key)
    stored = {f"r_{i}": {i, i + 500} for i in range(100)}
    for key, keys in stored.items():
        assert cache_client.set_cache(key, keys, partition_key)

    def reader(thread: int) -> list[str]:
        wrong = []
        for j in range(100):
            key = f"r_{(thread * 37 + j * 11) % 100}"
            if normalize_cache_result(cache_client.get(key, partition_key)) != stored[key]:
                wrong.append(key)
        return wrong

    with ThreadPoolExecutor(8) as pool:
        wrong = [key for result in pool.map(reader, range(8)) for key in result]
    assert wrong == []


def test_monitor_pattern_handler_per_work_item(cache_client: AbstractCacheHandler):
    """pcache-monitor's workers call get_cache_handler(..., singleton=True) per fragment: PostgreSQL gives each a new instance."""
    backend = os.environ.get("CACHE_BACKEND", "")
    if not backend.startswith("postgresql"):
        pytest.skip("the per-call instances of pcache-monitor's threads are a PostgreSQL behaviour")
    partition_key = _partition_key()
    _register(cache_client, partition_key)
    shared = []

    def write(key: str, keys: set[int]) -> bool:
        handler = get_cache_handler(backend, singleton=True)
        try:
            shared.append(handler is cache_client)
            return handler.set_cache(key, keys, partition_key)
        finally:
            handler.close()

    written = _write_concurrently(write, 4)
    _assert_stored(cache_client, partition_key, written)
    assert not any(shared)

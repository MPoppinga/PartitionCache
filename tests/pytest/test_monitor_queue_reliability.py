"""pcache-monitor: --close waits for both queues and the decomposer, a missing datatype is resolved, a failed push is not lost silently."""

import logging
import os
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import partitioncache.cli.monitor_cache_queue as mcq
from partitioncache.query_processor import hash_query

MODULE = "partitioncache.cli.monitor_cache_queue"
QUERY = "SELECT * FROM orders AS o WHERE o.total > 10"
PAIRS = [("SELECT DISTINCT o.pk FROM orders AS o WHERE o.total > 10", "h1")]


@pytest.fixture
def args():
    args = Mock()
    args.close = False
    args.disable_optimized_polling = False
    args.max_processes = 2
    args.status_log_interval = 10
    args.cache_backend = "rocksdict"
    args.db_backend = "sqlite"
    args.limit = None
    args.long_running_query_timeout = "0"
    args.force_recalculate = False
    args.disable_lazy_insertion = False
    args.enable_cache_optimization = False
    args.disable_cache_optimization = True
    args.enable_duckdb_acceleration = False
    args.bitsize = None
    args.log_query_times = None
    args.size_priority = None
    args.skip_partition_key_joins = []
    args.grid_cell_size = None
    original = getattr(mcq, "args", None)
    mcq.args = args
    yield args
    if original is not None:
        mcq.args = original
    else:
        delattr(mcq, "args")


@pytest.fixture
def exit_event():
    event = threading.Event()
    with patch(f"{MODULE}.exit_event", event):
        yield event


@pytest.fixture(autouse=True)
def no_requeue_memory():
    mcq._requeued_original_queries.clear()
    yield
    mcq._requeued_original_queries.clear()


def _lengths(original: int, fragments: int) -> dict:
    return {"original_query_queue": original, "query_fragment_queue": fragments}


class TestCloseCondition:
    @pytest.mark.parametrize(
        "active,lengths,closes",
        [(0, _lengths(0, 0), True), (1, _lengths(0, 0), False), (0, _lengths(1, 0), False), (0, _lengths(0, 1), False)],
    )
    def test_needs_no_worker_and_both_queues_empty(self, exit_event, active, lengths, closes):
        with patch(f"{MODULE}.get_queue_lengths", return_value=lengths):
            assert mcq.close_when_done(active) is closes
        assert exit_event.is_set() is closes

    def test_not_while_a_decomposition_is_in_flight(self, exit_event):
        """The decomposer holds the lock from its pop to its push: both queues can be empty in between."""
        with patch(f"{MODULE}.get_queue_lengths", return_value=_lengths(0, 0)) as lengths:
            with mcq.decomposition_lock:
                held_elsewhere = threading.Thread(target=lambda: setattr(self, "closed", mcq.close_when_done(0)))
                held_elsewhere.start()
                held_elsewhere.join(5)
        assert self.closed is False
        assert not exit_event.is_set()
        lengths.assert_not_called()

    def test_not_when_the_queue_count_fails(self, exit_event):
        with patch(f"{MODULE}.get_queue_lengths", side_effect=RuntimeError("connection lost")):
            assert mcq.close_when_done(0) is False
        assert not exit_event.is_set()

    @pytest.mark.parametrize("lengths,closes", [(_lengths(2, 0), False), (_lengths(0, 0), True)])
    def test_fragment_executor_waits_for_the_original_queue(self, args, lengths, closes):
        args.close = True
        rounds = [0]

        def stop_after_a_few_rounds():
            rounds[0] += 1
            return rounds[0] > 3

        with (
            patch(f"{MODULE}.exit_event") as event,
            patch(f"{MODULE}.pop_from_query_fragment_queue", return_value=None),
            patch(f"{MODULE}.pop_from_query_fragment_queue_blocking", return_value=None),
            patch(f"{MODULE}.get_cache_handler"),
            patch(f"{MODULE}.get_queue_lengths", return_value=lengths),
        ):
            event.is_set.side_effect = stop_after_a_few_rounds
            mcq.fragment_executor()
        assert event.set.called is closes

    def test_decomposer_does_not_pop_after_the_exit(self, args, exit_event):
        args.close = True
        exit_event.set()
        with patch(f"{MODULE}.pop_from_original_query_queue") as pop, patch(f"{MODULE}.pop_from_original_query_queue_blocking") as pop_blocking:
            mcq.query_fragment_processor(args, (None, None, None))
        pop.assert_not_called()
        pop_blocking.assert_not_called()

    def test_close_waits_until_the_popped_query_is_pushed(self, args, exit_event):
        """The original query is popped (both queues read empty) and decomposed; --close exits only after the push."""
        args.close = True
        pushing = threading.Event()
        release = threading.Event()

        def push(*_args, **_kwargs):
            pushing.set()
            release.wait(10)
            return True

        with (
            patch(f"{MODULE}.pop_from_original_query_queue", side_effect=[(QUERY, "pk", "integer"), None, None, None, None, None]),
            patch(f"{MODULE}.generate_all_query_hash_pairs", return_value=PAIRS),
            patch(f"{MODULE}.push_to_query_fragment_queue", side_effect=push) as pushed,
            patch(f"{MODULE}.get_queue_lengths", return_value=_lengths(0, 0)),
        ):
            decomposer = threading.Thread(target=mcq.query_fragment_processor, args=(args, (None, None, None)), daemon=True)
            decomposer.start()
            assert pushing.wait(5)
            assert mcq.close_when_done(0) is False
            release.set()
            for _ in range(100):
                if mcq.close_when_done(0):
                    break
                threading.Event().wait(0.05)
            decomposer.join(5)
        assert exit_event.is_set()
        assert not decomposer.is_alive()
        pushed.assert_called_once()


class TestMissingDatatype:
    def _run(self, cache, datatype, db_rows=(1, 2, 3)):
        db = Mock()
        db.execute.return_value = list(db_rows)
        with patch(f"{MODULE}.get_cache_handler", return_value=cache), patch(f"{MODULE}.get_db_handler", return_value=db):
            return mcq.run_and_store_query("SELECT DISTINCT o.pk FROM orders AS o", "h1", "pk", datatype)

    @pytest.mark.parametrize("datatype", [None, ""])
    def test_unregistered_key_is_inferred_from_the_result(self, args, datatype):
        cache = Mock()
        cache.get_datatype.return_value = None
        cache.set_cache.return_value = True
        cache.set_query.return_value = True

        assert self._run(cache, datatype) is True

        cache.register_partition_key.assert_not_called()
        cache.set_entry_lazy.assert_not_called()  # lazy insertion needs a registered datatype
        cache.set_cache.assert_called_once_with("h1", {1, 2, 3}, "pk")

    @pytest.mark.parametrize("datatype", [None, ""])
    def test_registered_key_uses_its_datatype(self, args, datatype):
        cache = Mock()
        cache.get_datatype.return_value = "integer"
        cache.set_entry_lazy.return_value = True
        # Lazy insertion runs the fragment on the cache handler's connection, which must be the data database
        args.db_backend = "postgresql"
        args.db_name = "sales"
        cache.db = SimpleNamespace(info=SimpleNamespace(host="localhost", port=5432, dbname="sales"))

        with patch.dict(os.environ, {"DB_HOST": "localhost", "DB_PORT": "5432"}):
            os.environ.pop("PG_DB_HOST", None)
            os.environ.pop("PG_DB_PORT", None)
            assert self._run(cache, datatype) is True

        cache.register_partition_key.assert_called_once()
        assert cache.register_partition_key.call_args.args == ("pk", "integer")
        cache.set_entry_lazy.assert_called_once()

    def test_empty_result_of_an_unregistered_key_gets_the_status_failed(self, args, tmp_path):
        """The datatype cannot be inferred from an empty result; the fragment is marked, not dropped silently."""
        pytest.importorskip("rocksdict")
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        cache = RocksDictCacheHandler(str(tmp_path / "cache"))
        try:
            assert self._run(cache, None, db_rows=()) is True
            assert cache.get_query_status("h1", "pk") == "failed"
        finally:
            cache.close()

    def test_real_handler_with_empty_string_datatype(self, args, tmp_path):
        """A queue row with '' as datatype (stored by an older client) registers nothing and stores the inferred keys."""
        pytest.importorskip("rocksdict")
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        cache = RocksDictCacheHandler(str(tmp_path / "cache"))
        try:
            assert self._run(cache, "") is True
            assert cache.get("h1", "pk") == {1, 2, 3}
            assert cache.get_datatype("pk") == "integer"
        finally:
            cache.close()


class TestFailedFragmentPush:
    def _decompose_once(self, args, push_back=True):
        with (
            patch(f"{MODULE}.exit_event") as event,
            patch(f"{MODULE}.pop_from_original_query_queue_blocking", return_value=(QUERY, "pk", "integer")),
            patch(f"{MODULE}.generate_all_query_hash_pairs", return_value=PAIRS),
            patch(f"{MODULE}.push_to_query_fragment_queue", return_value=False),
            patch(f"{MODULE}.push_to_original_query_queue", return_value=push_back) as requeue,
        ):
            event.is_set.side_effect = [False, True]
            mcq.query_fragment_processor(args, (None, None, None))
        return requeue

    def test_query_is_put_back_once_then_dropped(self, args, caplog):
        with caplog.at_level(logging.ERROR, logger="PartitionCache"):
            first = self._decompose_once(args)
            second = self._decompose_once(args)

        first.assert_called_once_with(QUERY, "pk", "integer")
        second.assert_not_called()
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 2
        assert all(hash_query(QUERY) in message for message in errors)
        assert "put back" in errors[0] and "dropped" in errors[1]

    def test_failed_put_back_is_logged_as_lost(self, args, caplog):
        with caplog.at_level(logging.ERROR, logger="PartitionCache"):
            self._decompose_once(args, push_back=False)
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and hash_query(QUERY) in errors[0] and "lost" in errors[0]

    def test_successful_push_forgets_an_earlier_put_back(self, args):
        self._decompose_once(args)
        with (
            patch(f"{MODULE}.exit_event") as event,
            patch(f"{MODULE}.pop_from_original_query_queue_blocking", return_value=(QUERY, "pk", "integer")),
            patch(f"{MODULE}.generate_all_query_hash_pairs", return_value=PAIRS),
            patch(f"{MODULE}.push_to_query_fragment_queue", return_value=True),
        ):
            event.is_set.side_effect = [False, True]
            mcq.query_fragment_processor(args, (None, None, None))
        assert not mcq._requeued_original_queries

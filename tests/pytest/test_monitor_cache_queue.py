"""
Unit tests for the monitor cache queue module.
"""

import logging
import os
from unittest.mock import Mock, patch

import pytest

from partitioncache.cli.monitor_cache_queue import (
    apply_cache_optimization,
    fragment_executor,
    print_status,
    process_completed_future,
    query_fragment_processor,
    run_and_store_query,
)


@pytest.fixture
def mock_env():
    """Mock environment variables for testing."""
    env_vars = {
        "QUERY_QUEUE_PROVIDER": "redis",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "QUERY_QUEUE_REDIS_DB": "1",
        "QUERY_QUEUE_REDIS_QUEUE_KEY": "test_queue",
    }
    with patch.dict(os.environ, env_vars):
        yield


@pytest.fixture
def mock_args():
    """Mock command line arguments."""
    args = Mock()
    args.partition_key = "partition_key"
    args.cache_backend = "rocksdb_set"
    args.db_backend = "sqlite"
    args.db_dir = "test.db"
    args.max_processes = 2
    args.limit = None
    args.long_running_query_timeout = "0"
    args.close = False
    # Add common variant generation args
    args.min_component_size = 1
    args.follow_graph = True
    args.no_auto_detect_star_join = False
    args.max_component_size = None
    args.star_join_table = None
    args.no_warn_partition_key = False
    args.bucket_steps = 1.0
    args.add_constraints = None
    args.remove_constraints_all = None
    args.remove_constraints_add = None
    # Add cache optimization args - disable by default for tests
    args.enable_cache_optimization = False
    args.disable_cache_optimization = True
    args.min_cache_hits = 1
    args.cache_optimization_method = "IN"
    args.prefer_lazy_optimization = True
    # Add DuckDB acceleration args
    args.enable_duckdb_acceleration = False
    # Add bitsize arg
    args.bitsize = None
    # Add other processing args
    args.max_pending_jobs = 5
    args.status_log_interval = 10
    args.disable_optimized_polling = False
    return args


@pytest.fixture
def mock_redis():
    """Mock Redis connection."""
    with patch("partitioncache.cli.monitor_cache_queue.redis.Redis") as mock_redis_class:
        redis_instance = Mock()
        mock_redis_class.return_value = redis_instance
        yield redis_instance


class TestQueryFragmentProcessor:
    """Test query fragment processor functionality."""

    @patch("partitioncache.cli.monitor_cache_queue.exit_event")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue")
    @patch("partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs")
    @patch("partitioncache.cli.monitor_cache_queue.push_to_query_fragment_queue")
    def test_query_fragment_processor_success(self, mock_push_fragments, mock_generate, mock_pop, mock_pop_blocking, mock_exit_event, mock_args, mock_env):
        """Test successful query fragment processing."""
        # Setup mocks
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop_blocking.return_value = ("SELECT * FROM test_table", "test_partition_key", "integer")
        mock_generate.return_value = [("SELECT DISTINCT t1.partition_key FROM test_table t1", "hash1")]

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            # Create constraint args tuple
            constraint_args = (None, None, None)  # (add_constraints, remove_constraints_all, remove_constraints_add)
            query_fragment_processor(mock_args, constraint_args)
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        # Verify calls
        mock_pop_blocking.assert_called_once()
        mock_generate.assert_called_once_with(
            "SELECT * FROM test_table",
            "test_partition_key",
            min_component_size=mock_args.min_component_size,
            follow_graph=mock_args.follow_graph,
            keep_all_attributes=True,
            auto_detect_star_join=not mock_args.no_auto_detect_star_join,
            max_component_size=mock_args.max_component_size,
            star_join_table=mock_args.star_join_table,
            warn_no_partition_key=not mock_args.no_warn_partition_key,
            bucket_steps=mock_args.bucket_steps,
            add_constraints=None,
            remove_constraints_all=None,
            remove_constraints_add=None,
        )
        mock_push_fragments.assert_called_once_with([("SELECT DISTINCT t1.partition_key FROM test_table t1", "hash1")], "test_partition_key", "integer")

    @patch("partitioncache.cli.monitor_cache_queue.exit_event")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue")
    def test_query_fragment_processor_empty_queue(self, mock_pop, mock_pop_blocking, mock_exit_event, mock_args, mock_env):
        """Test query fragment processor with empty queue."""
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop_blocking.return_value = None  # Empty queue

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            # Create constraint args tuple
            constraint_args = (None, None, None)  # (add_constraints, remove_constraints_all, remove_constraints_add)
            query_fragment_processor(mock_args, constraint_args)
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        mock_pop_blocking.assert_called_once()

    @patch("partitioncache.cli.monitor_cache_queue.exit_event")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue")
    def test_query_fragment_processor_error_handling(self, mock_pop, mock_pop_blocking, mock_exit_event, mock_args, mock_env):
        """Test query fragment processor error handling."""
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop_blocking.side_effect = Exception("Test error")

        with patch("time.sleep") as mock_sleep:
            # Monkey patch args into the module
            import partitioncache.cli.monitor_cache_queue as mcq_module

            original_args = getattr(mcq_module, "args", None)
            mcq_module.args = mock_args

            try:
                # Create constraint args tuple
                constraint_args = (None, None, None)  # (add_constraints, remove_constraints_all, remove_constraints_add)
                query_fragment_processor(mock_args, constraint_args)
            finally:
                # Restore original args
                if original_args is not None:
                    mcq_module.args = original_args
                else:
                    delattr(mcq_module, "args")

        mock_sleep.assert_called_once_with(1)

    @patch("partitioncache.cli.monitor_cache_queue.exit_event")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue")
    @patch("partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs")
    @patch("partitioncache.cli.monitor_cache_queue.push_to_query_fragment_queue")
    def test_query_fragment_processor_with_constraints(self, mock_push_fragments, mock_generate, mock_pop, mock_pop_blocking, mock_exit_event, mock_args, mock_env):
        """Test query fragment processing with constraint arguments."""
        # Setup mocks
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop_blocking.return_value = ("SELECT * FROM test_table", "test_partition_key", "integer")
        mock_generate.return_value = [("SELECT DISTINCT t1.partition_key FROM test_table t1", "hash1")]

        # Test specific constraint args
        add_constraints = {"test_table": "status = 'active'"}
        remove_constraints_all = ["size", "color"]
        remove_constraints_add = ["category"]
        constraint_args = (add_constraints, remove_constraints_all, remove_constraints_add)

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            query_fragment_processor(mock_args, constraint_args)
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        # Verify generate_all_query_hash_pairs was called with correct constraint args
        mock_generate.assert_called_once()
        call_kwargs = mock_generate.call_args[1]
        assert call_kwargs["add_constraints"] == add_constraints
        assert call_kwargs["remove_constraints_all"] == remove_constraints_all
        assert call_kwargs["remove_constraints_add"] == remove_constraints_add


class TestRunAndStoreQuery:
    """Test run and store query functionality."""

    @patch("partitioncache.cli.monitor_cache_queue.get_cache_handler")
    @patch("partitioncache.cli.monitor_cache_queue.get_db_handler")
    def test_run_and_store_query_success(self, mock_get_db, mock_get_cache, mock_args):
        """Test successful query execution and storage."""
        # Setup mocks
        mock_cache = Mock()
        # Make sure the mock doesn't have lazy insertion methods
        # so it uses the traditional path
        del mock_cache.set_cache_lazy
        del mock_cache.set_entry_lazy
        mock_get_cache.return_value = mock_cache

        mock_db = Mock()
        mock_db.execute.return_value = [1, 2, 3]
        mock_get_db.return_value = mock_db

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            result = run_and_store_query("SELECT * FROM test", "test_hash", "test_partition_key", "integer")
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        assert result is True
        mock_cache.set_cache.assert_called_once_with("test_hash", {1, 2, 3}, "test_partition_key")
        mock_db.close.assert_called_once()

    @patch("partitioncache.cli.monitor_cache_queue.get_cache_handler")
    @patch("partitioncache.cli.monitor_cache_queue.get_db_handler")
    def test_run_and_store_query_limit_reached(self, mock_get_db, mock_get_cache, mock_args):
        """Test query execution with limit reached."""
        mock_args.limit = 2

        mock_cache = Mock()
        # Make sure the mock doesn't have lazy insertion methods
        del mock_cache.set_cache_lazy
        del mock_cache.set_entry_lazy
        mock_get_cache.return_value = mock_cache

        mock_db = Mock()
        mock_db.execute.return_value = [1, 2, 3]  # 3 results, limit is 2
        mock_get_db.return_value = mock_db

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            result = run_and_store_query("SELECT * FROM test", "test_hash", "test_partition_key", "integer")
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        assert result is True
        mock_cache.set_query_status.assert_called_once_with("test_hash", "test_partition_key", "failed")
        mock_cache.set_cache.assert_not_called()

    @patch("partitioncache.cli.monitor_cache_queue.get_cache_handler")
    @patch("partitioncache.cli.monitor_cache_queue.get_db_handler")
    def test_run_and_store_query_timeout(self, mock_get_db, mock_get_cache, mock_args):
        """Test query execution with timeout."""
        import psycopg

        mock_cache = Mock()
        # Make sure the mock doesn't have lazy insertion methods
        del mock_cache.set_cache_lazy
        del mock_cache.set_entry_lazy
        mock_get_cache.return_value = mock_cache

        mock_db = Mock()
        mock_db.execute.side_effect = psycopg.OperationalError("statement timeout")
        mock_get_db.return_value = mock_db

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            result = run_and_store_query("SELECT * FROM test", "test_hash", "test_partition_key", "integer")
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        assert result is True
        mock_cache.set_query_status.assert_called_once_with("test_hash", "test_partition_key", "timeout")

    @patch("partitioncache.cli.monitor_cache_queue.get_cache_handler")
    @patch("partitioncache.cli.monitor_cache_queue.get_db_handler")
    def test_run_and_store_query_error(self, mock_get_db, mock_get_cache, mock_args):
        """Test query execution with general error."""
        mock_get_cache.side_effect = Exception("Cache error")

        with patch("builtins.open", create=True):
            with patch("os.makedirs", create=True):
                # Monkey patch args into the module
                import partitioncache.cli.monitor_cache_queue as mcq_module

                original_args = getattr(mcq_module, "args", None)
                mcq_module.args = mock_args

                try:
                    result = run_and_store_query("SELECT * FROM test", "test_hash", "test_partition_key", "integer")
                finally:
                    # Restore original args
                    if original_args is not None:
                        mcq_module.args = original_args
                    else:
                        delattr(mcq_module, "args")

        assert result is False


class TestPrintStatus:
    """Test print status functionality."""

    def test_print_status_basic(self, caplog):
        """Test basic print status."""
        with caplog.at_level(logging.INFO):
            print_status(5, 3)
        assert "Active processes: 5, Pending jobs: 3, Original query queue: 0, Query fragment queue: 0" in caplog.text

    def test_print_status_with_queues(self, caplog):
        """Test print status with queue lengths."""
        with caplog.at_level(logging.INFO):
            print_status(2, 1, 10, 5)
        assert "Active processes: 2, Pending jobs: 1, Original query queue: 10, Query fragment queue: 5" in caplog.text


class TestProcessCompletedFuture:
    """Test process completed future functionality."""

    def test_process_completed_future_success(self, mock_args):
        """Test successful future completion processing."""
        with patch("partitioncache.cli.monitor_cache_queue.status_lock"):
            with patch("partitioncache.cli.monitor_cache_queue.active_futures", ["hash1", "hash2"]):
                with patch("partitioncache.cli.monitor_cache_queue.pending_jobs", []):
                    with patch("partitioncache.cli.monitor_cache_queue.pool"):
                        with patch(
                            "partitioncache.cli.monitor_cache_queue.get_queue_lengths", return_value={"original_query_queue": 0, "query_fragment_queue": 0}
                        ):
                            # Monkey patch args into the module
                            import partitioncache.cli.monitor_cache_queue as mcq_module

                            original_args = getattr(mcq_module, "args", None)
                            mcq_module.args = mock_args

                            try:
                                mock_future = Mock()
                                process_completed_future(mock_future, "hash1")
                            finally:
                                # Restore original args
                                if original_args is not None:
                                    mcq_module.args = original_args
                                else:
                                    delattr(mcq_module, "args")

    def test_process_completed_future_with_pending(self, mock_args):
        """Test future completion with pending jobs."""
        with patch("partitioncache.cli.monitor_cache_queue.status_lock"):
            with patch("partitioncache.cli.monitor_cache_queue.active_futures", ["hash1"]):
                with patch("partitioncache.cli.monitor_cache_queue.pending_jobs", [("query2", "hash2", "test_partition_key", "integer")]):
                    with patch("partitioncache.cli.monitor_cache_queue.pool") as mock_pool:
                        with patch(
                            "partitioncache.cli.monitor_cache_queue.get_queue_lengths", return_value={"original_query_queue": 0, "query_fragment_queue": 0}
                        ):
                            mock_pool.submit.return_value = Mock()

                            # Monkey patch args into the module
                            import partitioncache.cli.monitor_cache_queue as mcq_module

                            original_args = getattr(mcq_module, "args", None)
                            mcq_module.args = mock_args

                            try:
                                mock_future = Mock()
                                process_completed_future(mock_future, "hash1")
                            finally:
                                # Restore original args
                                if original_args is not None:
                                    mcq_module.args = original_args
                                else:
                                    delattr(mcq_module, "args")

                            # Should submit the pending job
                            mock_pool.submit.assert_called_once()


class TestFragmentExecutorComponents:
    """Test components of fragment executor."""

    def test_fragment_executor_empty_queue(self, mock_args, mock_env):
        """Test fragment executor with empty query fragment queue."""
        # Set up mock args with required attributes
        mock_args.status_log_interval = 10
        mock_args.disable_optimized_polling = False
        mock_args.max_pending_jobs = 5
        mock_args.cache_backend = "redis_set"

        with patch("partitioncache.cli.monitor_cache_queue.exit_event") as mock_exit_event:
            with patch("partitioncache.cli.monitor_cache_queue.pop_from_query_fragment_queue") as mock_pop:
                with patch("partitioncache.cli.monitor_cache_queue.pop_from_query_fragment_queue_blocking") as mock_pop_blocking:
                    with patch("partitioncache.cli.monitor_cache_queue.get_cache_handler") as mock_get_cache:
                        with patch("partitioncache.cli.monitor_cache_queue.get_queue_lengths") as mock_get_lengths:
                            with patch("partitioncache.cli.monitor_cache_queue.time.time") as mock_time:
                                mock_exit_event.is_set.side_effect = [False, False, True]  # Run a bit then exit
                                mock_pop.return_value = None  # Empty queue
                                mock_pop_blocking.return_value = None
                                mock_get_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}
                                mock_time.return_value = 1000.0

                                mock_cache = Mock()
                                mock_get_cache.return_value = mock_cache

                                # Monkey patch args
                                import partitioncache.cli.monitor_cache_queue as mcq_module

                                original_args = getattr(mcq_module, "args", None)
                                mcq_module.args = mock_args

                                try:
                                    # This would normally run forever, but our mock will exit after one iteration
                                    fragment_executor()
                                finally:
                                    # Restore original values
                                    if original_args is not None:
                                        mcq_module.args = original_args
                                    else:
                                        delattr(mcq_module, "args")

                                # Either regular or blocking pop should be called
                                assert mock_pop.called or mock_pop_blocking.called

    def test_fragment_executor_cached_query(self, mock_args, mock_env):
        """Test fragment executor with already cached query."""
        # Set up mock args with required attributes
        mock_args.status_log_interval = 10
        mock_args.disable_optimized_polling = False
        mock_args.max_pending_jobs = 5
        mock_args.cache_backend = "redis_set"

        with patch("partitioncache.cli.monitor_cache_queue.exit_event") as mock_exit_event:
            with patch("partitioncache.cli.monitor_cache_queue.pop_from_query_fragment_queue") as mock_pop:
                with patch("partitioncache.cli.monitor_cache_queue.pop_from_query_fragment_queue_blocking") as mock_pop_blocking:
                    with patch("partitioncache.cli.monitor_cache_queue.get_cache_handler") as mock_get_cache:
                        with patch("partitioncache.cli.monitor_cache_queue.get_queue_lengths") as mock_get_lengths:
                            with patch("partitioncache.cli.monitor_cache_queue.time.time") as mock_time:
                                mock_exit_event.is_set.side_effect = [False, False, True]  # Run a bit then exit
                                mock_pop.return_value = ("SELECT * FROM test", "cached_hash", "test_partition_key", "integer")
                                mock_pop_blocking.return_value = ("SELECT * FROM test", "cached_hash", "test_partition_key", "integer")
                                mock_get_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}
                                mock_time.return_value = 1000.0

                                mock_cache = Mock()
                                mock_cache.exists.return_value = True  # Already in cache
                                mock_get_cache.return_value = mock_cache

                                # Monkey patch args
                                import partitioncache.cli.monitor_cache_queue as mcq_module

                                original_args = getattr(mcq_module, "args", None)
                                mcq_module.args = mock_args

                                try:
                                    fragment_executor()
                                finally:
                                    # Restore original values
                                    if original_args is not None:
                                        mcq_module.args = original_args
                                    else:
                                        delattr(mcq_module, "args")

                                mock_cache.exists.assert_called_with("cached_hash", "test_partition_key", check_query=False)


class TestIntegration:
    """Integration tests for monitor cache queue."""

    @patch("partitioncache.cli.monitor_cache_queue.exit_event")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
    @patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue")
    @patch("partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs")
    @patch("partitioncache.cli.monitor_cache_queue.push_to_query_fragment_queue")
    def test_end_to_end_fragment_processing(self, mock_push_to_outgoing, mock_generate, mock_pop, mock_pop_blocking, mock_exit_event, mock_args, mock_env):
        """Test end-to-end fragment processing workflow."""
        # Setup mocks for one iteration
        mock_exit_event.is_set.side_effect = [False, True]
        mock_pop_blocking.return_value = ("SELECT * FROM test_table WHERE id = 1", "test_partition_key", "integer")
        mock_generate.return_value = [("SELECT DISTINCT t1.partition_key FROM test_table t1 WHERE t1.id = 1", "hash1")]
        mock_push_to_outgoing.return_value = True

        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module

        original_args = getattr(mcq_module, "args", None)
        mcq_module.args = mock_args

        try:
            # Create constraint args tuple
            constraint_args = (None, None, None)  # (add_constraints, remove_constraints_all, remove_constraints_add)
            query_fragment_processor(mock_args, constraint_args)
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, "args")

        # Verify the workflow
        mock_pop_blocking.assert_called_once()
        mock_generate.assert_called_once_with(
            "SELECT * FROM test_table WHERE id = 1",
            "test_partition_key",
            min_component_size=mock_args.min_component_size,
            follow_graph=mock_args.follow_graph,
            keep_all_attributes=True,
            auto_detect_star_join=not mock_args.no_auto_detect_star_join,
            max_component_size=mock_args.max_component_size,
            star_join_table=mock_args.star_join_table,
            warn_no_partition_key=not mock_args.no_warn_partition_key,
            bucket_steps=mock_args.bucket_steps,
            add_constraints=None,
            remove_constraints_all=None,
            remove_constraints_add=None,
        )
        mock_push_to_outgoing.assert_called_once_with(
            [("SELECT DISTINCT t1.partition_key FROM test_table t1 WHERE t1.id = 1", "hash1")], "test_partition_key", "integer"
        )


class TestCacheOptimization:
    """Test cache optimization functionality."""

    def test_apply_cache_optimization_disabled(self, mock_args):
        """Test that optimization is not applied when disabled."""
        mock_args.enable_cache_optimization = False

        mock_cache_handler = Mock()

        query = "SELECT * FROM test WHERE id = 1"
        query_hash = "test_hash"
        partition_key = "test_partition"
        partition_datatype = "integer"

        optimized_query, was_optimized, stats = apply_cache_optimization(
            query, query_hash, partition_key, partition_datatype, mock_cache_handler, mock_args
        )

        assert optimized_query == query
        assert was_optimized is False
        assert stats["total_hashes"] == 0
        assert stats["cache_hits"] == 0
        assert stats["method_used"] is None

    @patch("partitioncache.cli.monitor_cache_queue.get_partition_keys_lazy")
    def test_apply_cache_optimization_lazy_with_hits(self, mock_get_lazy, mock_args):
        """Test lazy optimization when cache hits are found."""
        mock_args.enable_cache_optimization = True
        mock_args.disable_cache_optimization = False
        mock_args.prefer_lazy_optimization = True
        mock_args.cache_optimization_method = "IN_SUBQUERY"
        mock_args.min_component_size = 2
        mock_args.follow_graph = True

        # Create a proper mock that will pass isinstance check
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
        mock_cache_handler = Mock(spec=AbstractCacheHandler_Lazy)

        # Mock lazy intersection results
        lazy_subquery = "SELECT partition_key FROM cache_table WHERE hash IN ('h1', 'h2')"
        mock_get_lazy.return_value = (lazy_subquery, 5, 2)  # subquery, total_hashes, hits

        query = "SELECT * FROM test WHERE id = 1"
        query_hash = "test_hash"
        partition_key = "test_partition"
        partition_datatype = "integer"

        with patch("partitioncache.cli.monitor_cache_queue.extend_query_with_partition_keys_lazy") as mock_extend:
            mock_extend.return_value = "SELECT * FROM test WHERE id = 1 AND test_partition IN (SELECT ...)"

            optimized_query, was_optimized, stats = apply_cache_optimization(
                query, query_hash, partition_key, partition_datatype, mock_cache_handler, mock_args
            )

        assert was_optimized is True
        assert stats["total_hashes"] == 5
        assert stats["cache_hits"] == 2
        assert stats["method_used"] == "IN_SUBQUERY"

        mock_extend.assert_called_once_with(
            query=query,
            lazy_subquery=lazy_subquery,
            partition_key=partition_key,
            method="IN_SUBQUERY",
            analyze_tmp_table=True,
        )

    @patch("partitioncache.cli.monitor_cache_queue.get_partition_keys")
    def test_apply_cache_optimization_standard_with_hits(self, mock_get_keys, mock_args):
        """Test standard optimization when cache hits are found."""
        mock_args.enable_cache_optimization = True
        mock_args.disable_cache_optimization = False
        mock_args.prefer_lazy_optimization = False
        mock_args.cache_optimization_method = "IN"
        mock_args.min_cache_hits = 2
        mock_args.min_component_size = 2

        mock_cache_handler = Mock()

        # Mock standard intersection results
        partition_keys = {100, 200, 300}
        mock_get_keys.return_value = (partition_keys, 10, 3)  # keys, total_hashes, hits

        query = "SELECT * FROM test WHERE id = 1"
        query_hash = "test_hash"
        partition_key = "test_partition"
        partition_datatype = "integer"

        with patch("partitioncache.cli.monitor_cache_queue.extend_query_with_partition_keys") as mock_extend:
            mock_extend.return_value = "SELECT * FROM test WHERE id = 1 AND test_partition IN (100, 200, 300)"

            optimized_query, was_optimized, stats = apply_cache_optimization(
                query, query_hash, partition_key, partition_datatype, mock_cache_handler, mock_args
            )

        assert was_optimized is True
        assert stats["total_hashes"] == 10
        assert stats["cache_hits"] == 3
        assert stats["method_used"] == "IN"

        mock_extend.assert_called_once_with(
            query=query,
            partition_keys=partition_keys,
            partition_key=partition_key,
            method="IN",
            analyze_tmp_table=True,
        )

    @patch("partitioncache.cli.monitor_cache_queue.get_partition_keys")
    def test_apply_cache_optimization_below_threshold(self, mock_get_keys, mock_args):
        """Test that optimization is not applied when hits are below threshold."""
        mock_args.enable_cache_optimization = True
        mock_args.disable_cache_optimization = False
        mock_args.prefer_lazy_optimization = False
        mock_args.min_cache_hits = 5
        mock_args.min_component_size = 2

        mock_cache_handler = Mock()

        # Mock intersection results with hits below threshold
        partition_keys = {100, 200}
        mock_get_keys.return_value = (partition_keys, 10, 2)  # keys, total_hashes, hits

        query = "SELECT * FROM test WHERE id = 1"
        query_hash = "test_hash"
        partition_key = "test_partition"
        partition_datatype = "integer"

        optimized_query, was_optimized, stats = apply_cache_optimization(
            query, query_hash, partition_key, partition_datatype, mock_cache_handler, mock_args
        )

        assert optimized_query == query
        assert was_optimized is False
        assert stats["total_hashes"] == 10
        assert stats["cache_hits"] == 2
        assert stats["method_used"] is None

    @patch("partitioncache.cli.monitor_cache_queue.get_partition_keys_lazy")
    def test_apply_cache_optimization_exception_handling(self, mock_get_lazy, mock_args):
        """Test that exceptions are handled gracefully."""
        mock_args.enable_cache_optimization = True
        mock_args.disable_cache_optimization = False
        mock_args.prefer_lazy_optimization = True
        mock_args.min_component_size = 2
        mock_args.follow_graph = True

        # Create a proper mock that will pass isinstance check
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
        mock_cache_handler = Mock(spec=AbstractCacheHandler_Lazy)

        # Mock exception
        mock_get_lazy.side_effect = Exception("Test error")

        query = "SELECT * FROM test WHERE id = 1"
        query_hash = "test_hash"
        partition_key = "test_partition"
        partition_datatype = "integer"

        optimized_query, was_optimized, stats = apply_cache_optimization(
            query, query_hash, partition_key, partition_datatype, mock_cache_handler, mock_args
        )

        assert optimized_query == query
        assert was_optimized is False
        assert stats["total_hashes"] == 0
        assert stats["cache_hits"] == 0
        assert stats["method_used"] is None

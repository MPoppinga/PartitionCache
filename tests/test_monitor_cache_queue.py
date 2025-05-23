"""
Unit tests for the monitor cache queue module.
"""

import json
import os
import threading
import time
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from partitioncache.cli.monitor_cache_queue import (
    query_fragment_processor,
    push_fragments_to_outgoing_queue,
    run_and_store_query,
    fragment_executor,
    print_status,
    process_completed_future
)


@pytest.fixture
def mock_env():
    """Mock environment variables for testing."""
    env_vars = {
        'QUERY_QUEUE_PROVIDER': 'redis',
        'REDIS_HOST': 'localhost',
        'REDIS_PORT': '6379',
        'QUERY_QUEUE_REDIS_DB': '1',
        'QUERY_QUEUE_REDIS_QUEUE_KEY': 'test_queue'
    }
    with patch.dict(os.environ, env_vars):
        yield


@pytest.fixture
def mock_args():
    """Mock command line arguments."""
    args = Mock()
    args.partition_key = "partition_key"
    args.cache_backend = "rocksdb"
    args.db_backend = "sqlite"
    args.db_dir = "test.db"
    args.max_processes = 2
    args.limit = None
    args.long_running_query_timeout = "0"
    args.close = False
    return args


@pytest.fixture
def mock_redis():
    """Mock Redis connection."""
    with patch('partitioncache.cli.monitor_cache_queue.redis.Redis') as mock_redis_class:
        redis_instance = Mock()
        mock_redis_class.return_value = redis_instance
        yield redis_instance


class TestQueryFragmentProcessor:
    """Test query fragment processor functionality."""
    
    @patch('partitioncache.cli.monitor_cache_queue.exit_event')
    @patch('partitioncache.cli.monitor_cache_queue.pop_from_incoming_queue')
    @patch('partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs')
    @patch('partitioncache.cli.monitor_cache_queue.push_fragments_to_outgoing_queue')
    def test_query_fragment_processor_success(self, mock_push_fragments, mock_generate, 
                                            mock_pop, mock_exit_event, mock_args):
        """Test successful query fragment processing."""
        # Setup mocks
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop.return_value = "SELECT * FROM test_table"
        mock_generate.return_value = [("SELECT DISTINCT t1.partition_key FROM test_table t1", "hash1")]
        
        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module
        original_args = getattr(mcq_module, 'args', None)
        mcq_module.args = mock_args
        
        try:
            query_fragment_processor()
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, 'args')
        
        # Verify calls
        mock_pop.assert_called_once()
        mock_generate.assert_called_once_with("SELECT * FROM test_table", "partition_key", 1, True, True)
        mock_push_fragments.assert_called_once_with([("SELECT DISTINCT t1.partition_key FROM test_table t1", "hash1")])
    
    @patch('partitioncache.cli.monitor_cache_queue.exit_event')
    @patch('partitioncache.cli.monitor_cache_queue.pop_from_incoming_queue')
    def test_query_fragment_processor_empty_queue(self, mock_pop, mock_exit_event, mock_args):
        """Test query fragment processor with empty queue."""
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop.return_value = None  # Empty queue
        
        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module
        original_args = getattr(mcq_module, 'args', None)
        mcq_module.args = mock_args
        
        try:
            query_fragment_processor()
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, 'args')
        
        mock_pop.assert_called_once()
    
    @patch('partitioncache.cli.monitor_cache_queue.exit_event')
    @patch('partitioncache.cli.monitor_cache_queue.pop_from_incoming_queue')
    def test_query_fragment_processor_error_handling(self, mock_pop, mock_exit_event, mock_args):
        """Test query fragment processor error handling."""
        mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
        mock_pop.side_effect = Exception("Test error")
        
        with patch('time.sleep') as mock_sleep:
            # Monkey patch args into the module
            import partitioncache.cli.monitor_cache_queue as mcq_module
            original_args = getattr(mcq_module, 'args', None)
            mcq_module.args = mock_args
            
            try:
                query_fragment_processor()
            finally:
                # Restore original args
                if original_args is not None:
                    mcq_module.args = original_args
                else:
                    delattr(mcq_module, 'args')
        
        mock_sleep.assert_called_once_with(1)


class TestPushFragmentsToOutgoingQueue:
    """Test push fragments to outgoing queue functionality."""
    
    @patch('partitioncache.cli.monitor_cache_queue.push_to_outgoing_queue')
    def test_push_fragments_success(self, mock_push_to_outgoing):
        """Test successful fragment pushing."""
        mock_push_to_outgoing.return_value = True
        
        query_hash_pairs = [("query1", "hash1"), ("query2", "hash2")]
        
        result = push_fragments_to_outgoing_queue(query_hash_pairs)
        
        assert result is True
        mock_push_to_outgoing.assert_called_once_with(query_hash_pairs)
    
    @patch('partitioncache.cli.monitor_cache_queue.push_to_outgoing_queue')
    def test_push_fragments_error(self, mock_push_to_outgoing):
        """Test fragment pushing with error."""
        mock_push_to_outgoing.side_effect = Exception("Queue error")
        
        # Should not raise exception, should return False
        result = push_fragments_to_outgoing_queue([("query", "hash")])
        assert result is False


class TestRunAndStoreQuery:
    """Test run and store query functionality."""
    
    @patch('partitioncache.cli.monitor_cache_queue.get_cache_handler')
    @patch('partitioncache.cli.monitor_cache_queue.get_db_handler')
    def test_run_and_store_query_success(self, mock_get_db, mock_get_cache, mock_args):
        """Test successful query execution and storage."""
        # Setup mocks
        mock_cache = Mock()
        mock_get_cache.return_value = mock_cache
        
        mock_db = Mock()
        mock_db.execute.return_value = [1, 2, 3]
        mock_get_db.return_value = mock_db
        
        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module
        original_args = getattr(mcq_module, 'args', None)
        mcq_module.args = mock_args
        
        try:
            result = run_and_store_query("SELECT * FROM test", "test_hash")
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, 'args')
        
        assert result is True
        mock_cache.set_set.assert_called_once_with("test_hash", {1, 2, 3})
        mock_db.close.assert_called_once()
    
    @patch('partitioncache.cli.monitor_cache_queue.get_cache_handler')
    @patch('partitioncache.cli.monitor_cache_queue.get_db_handler')
    def test_run_and_store_query_limit_reached(self, mock_get_db, mock_get_cache, mock_args):
        """Test query execution with limit reached."""
        mock_args.limit = 2
        
        mock_cache = Mock()
        mock_get_cache.return_value = mock_cache
        
        mock_db = Mock()
        mock_db.execute.return_value = [1, 2, 3]  # 3 results, limit is 2
        mock_get_db.return_value = mock_db
        
        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module
        original_args = getattr(mcq_module, 'args', None)
        mcq_module.args = mock_args
        
        try:
            result = run_and_store_query("SELECT * FROM test", "test_hash")
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, 'args')
        
        assert result is True
        mock_cache.set_null.assert_called_once_with("_LIMIT_test_hash")
        mock_cache.set_set.assert_not_called()
    
    @patch('partitioncache.cli.monitor_cache_queue.get_cache_handler')
    @patch('partitioncache.cli.monitor_cache_queue.get_db_handler')
    def test_run_and_store_query_timeout(self, mock_get_db, mock_get_cache, mock_args):
        """Test query execution with timeout."""
        import psycopg
        
        mock_cache = Mock()
        mock_get_cache.return_value = mock_cache
        
        mock_db = Mock()
        mock_db.execute.side_effect = psycopg.OperationalError("statement timeout")
        mock_get_db.return_value = mock_db
        
        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module
        original_args = getattr(mcq_module, 'args', None)
        mcq_module.args = mock_args
        
        try:
            result = run_and_store_query("SELECT * FROM test", "test_hash")
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, 'args')
        
        assert result is True
        mock_cache.set_null.assert_called_once_with("_TIMEOUT_test_hash")
    
    @patch('partitioncache.cli.monitor_cache_queue.get_cache_handler')
    @patch('partitioncache.cli.monitor_cache_queue.get_db_handler')
    def test_run_and_store_query_error(self, mock_get_db, mock_get_cache, mock_args):
        """Test query execution with general error."""
        mock_get_cache.side_effect = Exception("Cache error")
        
        with patch('builtins.open', create=True):
            with patch('os.makedirs', create=True):
                # Monkey patch args into the module
                import partitioncache.cli.monitor_cache_queue as mcq_module
                original_args = getattr(mcq_module, 'args', None)
                mcq_module.args = mock_args
                
                try:
                    result = run_and_store_query("SELECT * FROM test", "test_hash")
                finally:
                    # Restore original args
                    if original_args is not None:
                        mcq_module.args = original_args
                    else:
                        delattr(mcq_module, 'args')
        
        assert result is False


class TestPrintStatus:
    """Test print status functionality."""
    
    def test_print_status_basic(self, capsys):
        """Test basic print status."""
        print_status(5, 3)
        captured = capsys.readouterr()
        assert "Active processes: 5, Pending jobs: 3, Original query queue: 0, Query fragment queue: 0" in captured.out
    
    def test_print_status_with_queues(self, capsys):
        """Test print status with queue lengths."""
        print_status(2, 1, 10, 5)
        captured = capsys.readouterr()
        assert "Active processes: 2, Pending jobs: 1, Original query queue: 10, Query fragment queue: 5" in captured.out


class TestProcessCompletedFuture:
    """Test process completed future functionality."""
    
    def test_process_completed_future_success(self, mock_args):
        """Test successful future completion processing."""
        with patch('partitioncache.cli.monitor_cache_queue.status_lock'):
            with patch('partitioncache.cli.monitor_cache_queue.active_futures', ['hash1', 'hash2']):
                with patch('partitioncache.cli.monitor_cache_queue.pending_jobs', []):
                    with patch('partitioncache.cli.monitor_cache_queue.pool') as mock_pool:
                        with patch('partitioncache.cli.monitor_cache_queue.get_queue_lengths', return_value={'incoming': 0, 'outgoing': 0}):
                            # Monkey patch args into the module
                            import partitioncache.cli.monitor_cache_queue as mcq_module
                            original_args = getattr(mcq_module, 'args', None)
                            mcq_module.args = mock_args
                            
                            try:
                                mock_future = Mock()
                                process_completed_future(mock_future, 'hash1')
                            finally:
                                # Restore original args
                                if original_args is not None:
                                    mcq_module.args = original_args
                                else:
                                    delattr(mcq_module, 'args')

    def test_process_completed_future_with_pending(self, mock_args):
        """Test future completion with pending jobs."""
        with patch('partitioncache.cli.monitor_cache_queue.status_lock'):
            with patch('partitioncache.cli.monitor_cache_queue.active_futures', ['hash1']) as mock_active:
                with patch('partitioncache.cli.monitor_cache_queue.pending_jobs', [('query2', 'hash2')]) as mock_pending:
                    with patch('partitioncache.cli.monitor_cache_queue.pool') as mock_pool:
                        with patch('partitioncache.cli.monitor_cache_queue.get_queue_lengths', return_value={'incoming': 0, 'outgoing': 0}):
                            mock_pool.submit.return_value = Mock()
                            
                            # Monkey patch args into the module
                            import partitioncache.cli.monitor_cache_queue as mcq_module
                            original_args = getattr(mcq_module, 'args', None)
                            mcq_module.args = mock_args
                            
                            try:
                                mock_future = Mock()
                                process_completed_future(mock_future, 'hash1')
                            finally:
                                # Restore original args
                                if original_args is not None:
                                    mcq_module.args = original_args
                                else:
                                    delattr(mcq_module, 'args')
                            
                            # Should submit the pending job
                            mock_pool.submit.assert_called_once()


class TestFragmentExecutorComponents:
    """Test components of fragment executor."""
    
    def test_fragment_executor_empty_queue(self, mock_args):
        """Test fragment executor with empty outgoing queue."""
        with patch('partitioncache.cli.monitor_cache_queue.exit_event') as mock_exit_event:
            with patch('partitioncache.cli.monitor_cache_queue.pop_from_outgoing_queue') as mock_pop:
                with patch('partitioncache.cli.monitor_cache_queue.get_cache_handler') as mock_get_cache:
                    mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
                    mock_pop.return_value = None  # Empty queue
                    
                    mock_cache = Mock()
                    mock_get_cache.return_value = mock_cache
                    
                    # Monkey patch args into the module
                    import partitioncache.cli.monitor_cache_queue as mcq_module
                    original_args = getattr(mcq_module, 'args', None)
                    mcq_module.args = mock_args
                    
                    try:
                        # This would normally run forever, but our mock will exit after one iteration
                        fragment_executor()
                    finally:
                        # Restore original args
                        if original_args is not None:
                            mcq_module.args = original_args
                        else:
                            delattr(mcq_module, 'args')
                    
                    mock_pop.assert_called()

    def test_fragment_executor_cached_query(self, mock_args):
        """Test fragment executor with already cached query."""
        with patch('partitioncache.cli.monitor_cache_queue.exit_event') as mock_exit_event:
            with patch('partitioncache.cli.monitor_cache_queue.pop_from_outgoing_queue') as mock_pop:
                with patch('partitioncache.cli.monitor_cache_queue.get_cache_handler') as mock_get_cache:
                    mock_exit_event.is_set.side_effect = [False, True]  # Run once then exit
                    mock_pop.return_value = ("SELECT * FROM test", "cached_hash")
                    
                    mock_cache = Mock()
                    mock_cache.exists.return_value = True  # Already in cache
                    mock_get_cache.return_value = mock_cache
                    
                    # Monkey patch args into the module
                    import partitioncache.cli.monitor_cache_queue as mcq_module
                    original_args = getattr(mcq_module, 'args', None)
                    mcq_module.args = mock_args
                    
                    try:
                        fragment_executor()
                    finally:
                        # Restore original args
                        if original_args is not None:
                            mcq_module.args = original_args
                        else:
                            delattr(mcq_module, 'args')
                    
                    mock_cache.exists.assert_called_with("cached_hash")


class TestIntegration:
    """Integration tests for monitor cache queue."""
    
    @patch('partitioncache.cli.monitor_cache_queue.exit_event')
    @patch('partitioncache.cli.monitor_cache_queue.pop_from_incoming_queue')
    @patch('partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs')
    @patch('partitioncache.cli.monitor_cache_queue.push_to_outgoing_queue')
    def test_end_to_end_fragment_processing(self, mock_push_to_outgoing, mock_generate, mock_pop, mock_exit_event, mock_args):
        """Test end-to-end fragment processing workflow."""
        # Setup mocks for one iteration
        mock_exit_event.is_set.side_effect = [False, True]
        mock_pop.return_value = "SELECT * FROM test_table WHERE id = 1"
        mock_generate.return_value = [
            ("SELECT DISTINCT t1.partition_key FROM test_table t1 WHERE t1.id = 1", "hash1")
        ]
        mock_push_to_outgoing.return_value = True
        
        # Monkey patch args into the module
        import partitioncache.cli.monitor_cache_queue as mcq_module
        original_args = getattr(mcq_module, 'args', None)
        mcq_module.args = mock_args
        
        try:
            query_fragment_processor()
        finally:
            # Restore original args
            if original_args is not None:
                mcq_module.args = original_args
            else:
                delattr(mcq_module, 'args')
        
        # Verify the workflow
        mock_pop.assert_called_once()
        mock_generate.assert_called_once_with(
            "SELECT * FROM test_table WHERE id = 1", 
            "partition_key", 
            1, 
            True, 
            True
        )
        mock_push_to_outgoing.assert_called_once_with([
            ("SELECT DISTINCT t1.partition_key FROM test_table t1 WHERE t1.id = 1", "hash1")
        ]) 
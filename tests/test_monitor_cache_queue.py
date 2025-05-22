import pytest
from unittest.mock import Mock, patch, MagicMock
import redis
import psycopg
from partitioncache.cli.monitor_cache_queue import run_and_store_query, process_completed_future

@pytest.fixture
def mock_cache_handler():
    mock = Mock()
    mock.exists.return_value = False
    mock.set_set = Mock()
    return mock

@pytest.fixture
def mock_db_handler():
    mock = Mock()
    mock.execute.return_value = {1, 2, 3}
    mock.close = Mock()
    return mock

@pytest.fixture
def mock_redis():
    mock = Mock(spec=redis.Redis)
    mock.blpop.return_value = [b'queue_key', b'SELECT * FROM table']
    return mock

@pytest.fixture
def mock_args():
    class Args:
        db_backend = "postgresql"
        cache_backend = "redis"
        db_dir = "data/test_db.sqlite"
        db_env_file = "test.env"
        partition_key = "partition_key"
        close = False
        max_processes = 12
        db_name = "test_db"
        long_running_query_timeout = "0"
        limit = None
    return Args()

@pytest.fixture(autouse=True)
def setup_args(mock_args):
    with patch('partitioncache.cli.monitor_cache_queue.args', mock_args):
        yield

def test_run_and_store_query_success(mock_cache_handler, mock_db_handler):
    query = "SELECT * FROM table"
    query_hash = "test_hash"
    
    with patch('partitioncache.cli.monitor_cache_queue.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.monitor_cache_queue.PostgresDBHandler', return_value=mock_db_handler):
        
        result = run_and_store_query(query, query_hash)
        
        assert result is True
        mock_db_handler.execute.assert_called_with(query)
        mock_cache_handler.set_set.assert_called_with(query_hash, {1, 2, 3})
        mock_db_handler.close.assert_called_once()

def test_run_and_store_query_with_limit(mock_cache_handler, mock_db_handler, mock_args):
    query = "SELECT * FROM table"
    query_hash = "test_hash"
    mock_db_handler.execute.return_value = set(range(1000))  # Return large result set
    mock_args.limit = 100
    
    with patch('partitioncache.cli.monitor_cache_queue.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.monitor_cache_queue.PostgresDBHandler', return_value=mock_db_handler):
        
        result = run_and_store_query(query, query_hash)
        
        assert result is True
        mock_cache_handler.set_null.assert_called_with(f"_LIMIT_{query_hash}")

def test_run_and_store_query_timeout(mock_cache_handler, mock_db_handler):
    query = "SELECT * FROM table"
    query_hash = "test_hash"
    mock_db_handler.execute.side_effect = psycopg.OperationalError("statement timeout")
    
    with patch('partitioncache.cli.monitor_cache_queue.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.monitor_cache_queue.PostgresDBHandler', return_value=mock_db_handler):
        
        result = run_and_store_query(query, query_hash)
        
        assert result is True
        mock_cache_handler.set_null.assert_called_with(f"_TIMEOUT_{query_hash}")

def test_run_and_store_query_error(mock_cache_handler, mock_db_handler):
    query = "SELECT * FROM table"
    query_hash = "test_hash"
    mock_db_handler.execute.side_effect = Exception("Test error")
    
    with patch('partitioncache.cli.monitor_cache_queue.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.monitor_cache_queue.PostgresDBHandler', return_value=mock_db_handler), \
         patch('builtins.open', MagicMock()):
        
        result = run_and_store_query(query, query_hash)
        
        assert result is False

def test_process_completed_future():
    mock_future = Mock()
    mock_future.result.return_value = True
    mock_pool = Mock()
    
    with patch('partitioncache.cli.monitor_cache_queue.pool', mock_pool), \
         patch('partitioncache.cli.monitor_cache_queue.active_futures', ["test_hash"]), \
         patch('partitioncache.cli.monitor_cache_queue.pending_jobs', [("query2", "hash2")]):
        
        process_completed_future(mock_future, "test_hash")
        
        # Verify that the completed hash was removed from active_futures
        from partitioncache.cli.monitor_cache_queue import active_futures
        assert "test_hash" not in active_futures
        
        # Verify that a new job was started from pending_jobs
        assert mock_pool.submit.called 
import pytest
from unittest.mock import Mock, patch
import os
from partitioncache.cli.add_to_cache import main

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
def mock_args():
    class Args:
        query = "SELECT * FROM table"
        queue = False
        no_recompose = False
        db_backend = "postgresql"
        db_name = "test_db"
        env_file = None
        cache_backend = "redis"
        partition_key = "partition_key"
        db_env_file = "test.env"
    return Args()

def test_main_direct_execution(mock_cache_handler, mock_db_handler, mock_args):
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.add_to_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.add_to_cache.PostgresDBHandler', return_value=mock_db_handler), \
         patch.dict(os.environ, {
             'PG_DB_HOST': 'localhost',
             'PG_DB_PORT': '5432',
             'PG_DB_USER': 'test_user',
             'PG_DB_PASSWORD': 'test_pass'
         }), \
         patch('partitioncache.cli.add_to_cache.generate_all_query_hash_pairs', return_value=[("SELECT * FROM table", "hash1")]):
        
        main()
        
        # Verify database and cache operations
        assert mock_db_handler.execute.called
        assert mock_cache_handler.set_set.called
        assert mock_db_handler.close.called
        assert mock_cache_handler.close.called

def test_main_queue_mode(mock_args):
    mock_args.queue = True
    
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.add_to_cache.push_to_queue') as mock_push:
        
        mock_push.return_value = True
        main()
        
        # Verify query was pushed to queue
        mock_push.assert_called_with(mock_args.query)

def test_main_no_recompose(mock_cache_handler, mock_db_handler, mock_args):
    mock_args.no_recompose = True
    
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.add_to_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.add_to_cache.PostgresDBHandler', return_value=mock_db_handler), \
         patch.dict(os.environ, {
             'PG_DB_HOST': 'localhost',
             'PG_DB_PORT': '5432',
             'PG_DB_USER': 'test_user',
             'PG_DB_PASSWORD': 'test_pass'
         }), \
         patch('partitioncache.cli.add_to_cache.clean_query', return_value="SELECT * FROM table"), \
         patch('partitioncache.cli.add_to_cache.hash_query', return_value="hash1"):
        
        main()
        
        # Verify single query execution without recomposition
        mock_db_handler.execute.assert_called_once()
        mock_cache_handler.set_set.assert_called_once()

def test_main_with_env_file(mock_cache_handler, mock_db_handler, mock_args, tmp_path):
    # Create temporary env file
    env_file = tmp_path / ".env"
    env_file.write_text("PG_DB_HOST=testhost\nPG_DB_PORT=5432\n")
    mock_args.env_file = str(env_file)
    
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.add_to_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.add_to_cache.PostgresDBHandler', return_value=mock_db_handler), \
         patch('partitioncache.cli.add_to_cache.generate_all_query_hash_pairs', return_value=[("SELECT * FROM table", "hash1")]):
        
        main()
        
        # Verify env file was loaded and handlers were initialized
        assert mock_db_handler.execute.called
        assert mock_cache_handler.set_set.called

def test_main_error_handling(mock_args):
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.add_to_cache.get_cache_handler', side_effect=Exception("Test error")), \
         pytest.raises(SystemExit) as exc_info:
        
        main()
        
        # Verify error handling
        assert exc_info.value.code == 1 
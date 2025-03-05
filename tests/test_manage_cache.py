import pytest
from unittest.mock import Mock, patch
import redis
import os
from partitioncache.cli.manage_cache import (
    copy_cache,
    export_cache,
    restore_cache,
    delete_cache,
    get_all_keys,
    count_cache,
    count_queue,
    clear_queue,
    delete_all_caches,
    count_all_caches,
    remove_termination_entries,
    remove_large_entries,
)

@pytest.fixture
def mock_cache_handler():
    mock = Mock()
    mock.get_all_keys.return_value = ["key1", "key2", "_LIMIT_key3", "_TIMEOUT_key4"]
    mock.get.side_effect = lambda key: {1, 2, 3} if key == "key1" else {4, 5, 6}
    mock.exists.return_value = False
    return mock

@pytest.fixture
def mock_redis():
    mock = Mock(spec=redis.Redis)
    return mock

def test_copy_cache(mock_cache_handler):
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        copy_cache("redis", "postgresql_bit")
        
        # Verify the cache handler was used correctly
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.get.called
        assert mock_cache_handler.set_set.called
        assert mock_cache_handler.close.called

def test_export_cache(mock_cache_handler, tmp_path):
    archive_file = tmp_path / "test_archive.pkl"
    
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        export_cache("redis", str(archive_file))
        
        # Verify file was created
        assert archive_file.exists()
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.get.called
        assert mock_cache_handler.close.called

def test_restore_cache(mock_cache_handler, tmp_path):
    archive_file = tmp_path / "test_archive.pkl"
    
    # Create a test archive file
    with open(archive_file, "wb") as f:
        import pickle
        pickle.dump({"key1": {1, 2, 3}}, f)
    
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        restore_cache("redis", str(archive_file))
        
        # Verify cache operations
        assert mock_cache_handler.exists.called
        assert mock_cache_handler.set_set.called
        assert mock_cache_handler.close.called

def test_delete_cache(mock_cache_handler):
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        delete_cache("redis")
        
        # Verify all keys were deleted
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.delete.called
        assert mock_cache_handler.close.called

def test_count_cache(mock_cache_handler):
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        count_cache("redis")
        
        # Verify keys were counted
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.close.called

def test_count_queue(mock_redis):
    with patch('redis.Redis', return_value=mock_redis):
        count_queue()
        
        # Verify queue length was checked
        assert mock_redis.llen.called
        assert mock_redis.close.called

def test_clear_queue(mock_redis):
    with patch('redis.Redis', return_value=mock_redis):
        clear_queue()
        
        # Verify queue was cleared
        assert mock_redis.delete.called
        assert mock_redis.close.called

def test_delete_all_caches(monkeypatch, mock_cache_handler):
    # Mock input to simulate user confirmation
    monkeypatch.setattr('builtins.input', lambda _: 'yes')
    
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        delete_all_caches()
        
        # Verify all caches were deleted
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.delete.called
        assert mock_cache_handler.close.called

def test_count_all_caches(mock_cache_handler):
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        count_all_caches()
        
        # Verify all caches were counted
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.close.called

def test_remove_termination_entries(mock_cache_handler):
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        remove_termination_entries("redis")
        
        # Verify termination entries were removed
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.delete.called
        assert mock_cache_handler.close.called

def test_remove_large_entries(mock_cache_handler):
    with patch('partitioncache.cli.manage_cache.get_cache_handler', return_value=mock_cache_handler):
        remove_large_entries("redis", 2)
        
        # Verify large entries were removed
        assert mock_cache_handler.get_all_keys.called
        assert mock_cache_handler.get.called
        assert mock_cache_handler.delete.called
        assert mock_cache_handler.close.called 
import pytest
from unittest.mock import Mock, patch
import sys
import os
import json
from io import StringIO
from partitioncache.cli.read_from_cache import main

@pytest.fixture
def mock_cache_handler():
    mock = Mock()
    mock.exists.return_value = True
    mock.get.return_value = {1, 2, 3}
    return mock

@pytest.fixture
def mock_args():
    class Args:
        def __init__(self):
            self.query = "SELECT * FROM test"
            self.cache_backend = "redis"
            self.partition_key = "partition_key"
            self.env_file = None
            self.output_format = "list"
    return Args()

@pytest.fixture
def capture_stdout():
    return StringIO()

def test_main_list_output(mock_cache_handler, mock_args, capture_stdout):
    partition_keys = {1, 2, 3}
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.read_from_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.read_from_cache.get_partition_keys', return_value=(partition_keys, 1, 1)):

        with pytest.raises(SystemExit) as exc_info:
            main(file=capture_stdout)
        
        assert exc_info.value.code == 0
        output = capture_stdout.getvalue().strip()
        expected = ",".join(str(x) for x in sorted(partition_keys))
        assert output == expected

def test_main_json_output(mock_cache_handler, mock_args, capture_stdout):
    mock_args.output_format = "json"
    partition_keys = {1, 2, 3}

    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.read_from_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.read_from_cache.get_partition_keys', return_value=(partition_keys, 1, 1)):

        with pytest.raises(SystemExit) as exc_info:
            main(file=capture_stdout)
        
        assert exc_info.value.code == 0
        output = capture_stdout.getvalue().strip()
        assert sorted(json.loads(output)) == sorted(list(partition_keys))

def test_main_lines_output(mock_cache_handler, mock_args, capture_stdout):
    mock_args.output_format = "lines"
    partition_keys = {1, 2, 3}

    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.read_from_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.read_from_cache.get_partition_keys', return_value=(partition_keys, 1, 1)):

        with pytest.raises(SystemExit) as exc_info:
            main(file=capture_stdout)
        
        assert exc_info.value.code == 0
        output = sorted(line.strip() for line in capture_stdout.getvalue().strip().split('\n'))
        assert output == [str(x) for x in sorted(partition_keys)]

def test_main_no_results(mock_cache_handler, mock_args):
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.read_from_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.read_from_cache.get_partition_keys', return_value=(None, 0, 0)):

        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 0

def test_main_with_env_file(mock_cache_handler, mock_args, tmp_path):
    # Create temporary env file
    env_file = tmp_path / ".env"
    env_file.write_text("CACHE_HOST=testhost\nCACHE_PORT=6379\n")
    mock_args.env_file = str(env_file)

    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.read_from_cache.get_cache_handler', return_value=mock_cache_handler), \
         patch('partitioncache.cli.read_from_cache.get_partition_keys', return_value=({1, 2, 3}, 1, 1)):

        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 0
        
        # Verify environment variables were loaded
        assert os.getenv("CACHE_HOST") == "testhost"
        assert os.getenv("CACHE_PORT") == "6379"
        
        mock_cache_handler.close.assert_called_once()

def test_main_error_handling(mock_cache_handler, mock_args):
    with patch('argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('partitioncache.cli.read_from_cache.get_cache_handler', side_effect=Exception("Test error")):

        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 1 
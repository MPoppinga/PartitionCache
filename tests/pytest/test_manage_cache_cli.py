"""Tests for the manage_cache CLI with subparsers."""

from unittest.mock import MagicMock, patch

import pytest

from partitioncache.cache_handler.redis_abstract import RedisAbstractCacheHandler
from partitioncache.cli.manage_cache import main, show_comprehensive_status


class TestManageCacheCLI:
    """Test the new subparser-based CLI structure."""

    def test_help_displays_commands(self):
        """Test that the main help shows available commands."""

        with patch("sys.argv", ["manage_cache.py", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    @pytest.mark.parametrize(
        "argv,mock_path,mock_args",
        [
            # Setup commands
            (["setup", "all"], "setup_all_tables", None),
            (["setup", "queue"], "setup_queue_tables", None),
            (["setup", "cache"], "setup_cache_metadata_tables", None),
            # Status commands
            (["status", "env"], "validate_environment", None),
            (["status", "tables"], "check_table_status", None),
            (["status"], "show_comprehensive_status", None),
            (["status", "all"], "show_comprehensive_status", None),
            # Cache commands
            (["cache", "count", "--type", "postgresql_array"], "count_cache", ("postgresql_array",)),
            (["cache", "copy", "--from", "redis_set", "--to", "postgresql_array"], "copy_cache", ("redis_set", "postgresql_array", None)),
            (["cache", "export", "--type", "postgresql_array", "--file", "backup.pkl"], "export_cache", ("postgresql_array", "backup.pkl", None)),
            (["cache", "import", "--type", "postgresql_array", "--file", "backup.pkl"], "restore_cache", ("postgresql_array", "backup.pkl", None, None)),
            (["cache", "delete", "--type", "postgresql_array"], "delete_cache", ("postgresql_array",)),
            # Queue commands
            (["queue", "count"], "count_queue", None),
            (["queue", "clear"], "clear_queue", None),
            (["queue", "clear", "--original"], "clear_original_query_queue", None),
            (["queue", "clear", "--fragment"], "clear_query_fragment_queue", None),
            # Maintenance commands
            (["maintenance", "prune", "--days", "30"], "prune_all_caches", (30,)),
            (["maintenance", "prune", "--days", "7", "--type", "postgresql_array"], "prune_old_queries", ("postgresql_array", 7)),
            (["maintenance", "cleanup", "--type", "postgresql_array", "--remove-termination"], "remove_termination_entries", ("postgresql_array",)),
            (["maintenance", "cleanup", "--type", "postgresql_array", "--remove-large", "1000"], "remove_large_entries", ("postgresql_array", 1000)),
            (["maintenance", "partition", "--type", "postgresql_array", "--delete", "old_partition"], "delete_partition", ("postgresql_array", "old_partition")),
        ],
    )
    def test_cli_routing(self, argv, mock_path, mock_args):
        """Test that CLI commands route to correct functions."""
        with patch(f"partitioncache.cli.manage_cache.{mock_path}") as mock_func:
            with patch("sys.argv", ["manage_cache.py"] + argv):
                main()
                if mock_args:
                    mock_func.assert_called_once_with(*mock_args)
                else:
                    mock_func.assert_called_once()

    # Tests for --all flag variants
    @pytest.mark.parametrize(
        "argv,mock_path",
        [
            (["cache", "count", "--type", "postgresql_array", "--all"], "count_all_caches"),
            (["cache", "delete", "--type", "postgresql_array", "--all"], "delete_all_caches"),
        ],
    )
    def test_cli_all_flag_routing(self, argv, mock_path):
        """Test that --all flag routes to *_all functions."""
        with patch(f"partitioncache.cli.manage_cache.{mock_path}") as mock_func:
            with patch("sys.argv", ["manage_cache.py"] + argv):
                main()
                mock_func.assert_called_once()

    # Tests for environment fallback
    @pytest.mark.parametrize(
        "argv,mock_path,env_backend,expected_args",
        [
            (["cache", "count"], "count_cache", "redis_set", ("redis_set",)),
            (["cache", "export", "--file", "test.pkl"], "export_cache", "postgresql_bit", ("postgresql_bit", "test.pkl", None)),
            (["cache", "import", "--file", "test.pkl"], "restore_cache", "rocksdb_set", ("rocksdb_set", "test.pkl", None, None)),
            (["cache", "delete"], "delete_cache", "redis_bit", ("redis_bit",)),
            (["maintenance", "cleanup", "--remove-termination"], "remove_termination_entries", "postgresql_array", ("postgresql_array",)),
            (["maintenance", "partition", "--delete", "test_partition"], "delete_partition", "postgresql_bit", ("postgresql_bit", "test_partition")),
        ],
    )
    def test_cli_env_backend_fallback(self, argv, mock_path, env_backend, expected_args):
        """Test that commands use CACHE_BACKEND from env when --type is omitted."""
        with patch(f"partitioncache.cli.manage_cache.{mock_path}") as mock_func:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value=env_backend):
                with patch("sys.argv", ["manage_cache.py"] + argv):
                    main()
                    mock_func.assert_called_once_with(*expected_args)

    def test_no_command_shows_help(self):
        """Test that running without commands shows help."""

        with patch("sys.argv", ["manage_cache.py"]):
            with patch("argparse.ArgumentParser.print_help") as mock_help:
                main()
                mock_help.assert_called_once()

    def test_partial_command_shows_subhelp(self):
        """Test that partial commands show subcommand help."""

        # Test setup without subcommand
        with patch("sys.argv", ["manage_cache.py", "setup"]):
            with patch("argparse.ArgumentParser.print_help") as mock_help:
                main()
                mock_help.assert_called_once()

    @patch("partitioncache.cli.manage_cache.load_environment_with_validation")
    def test_env_file_loading(self, mock_load_env):
        """Test that environment file loading works."""

        with patch("partitioncache.cli.manage_cache.validate_environment"):
            with patch("sys.argv", ["manage_cache.py", "--env-file", "custom.env", "status", "env"]):
                main()
                mock_load_env.assert_called_once_with("custom.env")

    def test_error_handling(self):
        """Test that exceptions are handled gracefully."""

        with patch("partitioncache.cli.manage_cache.validate_environment", side_effect=Exception("Test error")):
            with patch("sys.argv", ["manage_cache.py", "status", "env"]):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1

    @patch("os.getenv")
    def test_get_cache_type_from_env_function(self, mock_getenv):
        """Test the get_cache_type_from_env function."""
        from partitioncache.cli.manage_cache import get_cache_type_from_env

        # Test with CACHE_BACKEND set
        mock_getenv.return_value = "redis_set"
        result = get_cache_type_from_env()
        assert result == "redis_set"
        mock_getenv.assert_called_with("CACHE_BACKEND", "postgresql_array")

        # Test with default fallback - when getenv returns the default value
        mock_getenv.return_value = "postgresql_array"  # This is what happens when getenv gets the default
        result = get_cache_type_from_env()
        assert result == "postgresql_array"


class TestCLITimeoutProtection:
    """Test the CLI timeout protection for cache backends."""

    @patch("partitioncache.cli.manage_cache.detect_configured_cache_backends")
    @patch("partitioncache.cli.manage_cache.detect_configured_queue_providers")
    @patch("partitioncache.cli.manage_cache.get_queue_lengths")
    @patch("partitioncache.cli.manage_cache.get_cache_handler")
    def test_redis_connection_timeout_handling(self, mock_get_cache_handler, mock_get_queue_lengths,
                                             mock_detect_queue_providers, mock_detect_cache_backends):
        """Test that Redis connection timeouts are handled gracefully."""

        # Setup mocks
        mock_detect_cache_backends.return_value = ["redis_set"]
        mock_detect_queue_providers.return_value = []
        mock_get_queue_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}

        # Mock Redis handler with connection timeout
        mock_redis_handler = MagicMock()
        # Make isinstance check pass
        mock_redis_handler.__class__ = RedisAbstractCacheHandler
        mock_redis_handler.db.ping.side_effect = Exception("Connection timeout")
        mock_get_cache_handler.return_value = mock_redis_handler

        # Call show_comprehensive_status - should not raise exception
        with patch("partitioncache.cli.manage_cache.logger") as mock_logger:
            with patch("partitioncache.cli.manage_cache.get_all_keys", return_value=[]):
                show_comprehensive_status()

            # Verify that the connection was attempted
            mock_redis_handler.db.ping.assert_called_once()
            mock_redis_handler.close.assert_called()

            # Verify that error was logged appropriately
            mock_logger.error.assert_called()
            error_call = mock_logger.error.call_args[0][0]
            assert "Error accessing redis_set cache" in error_call

    @patch("partitioncache.cli.manage_cache.detect_configured_cache_backends")
    @patch("partitioncache.cli.manage_cache.detect_configured_queue_providers")
    @patch("partitioncache.cli.manage_cache.get_queue_lengths")
    @patch("partitioncache.cli.manage_cache.get_cache_handler")
    def test_rocksdb_connection_timeout_handling(self, mock_get_cache_handler, mock_get_queue_lengths,
                                                mock_detect_queue_providers, mock_detect_cache_backends):
        """Test that RocksDB connection timeouts are handled gracefully."""

        # Setup mocks
        mock_detect_cache_backends.return_value = ["rocksdb_set"]
        mock_detect_queue_providers.return_value = []
        mock_get_queue_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}

        # Mock RocksDB handler that fails during get_cache_handler creation
        mock_get_cache_handler.side_effect = Exception("RocksDB connection failed")

        # Call show_comprehensive_status - should not raise exception
        with patch("partitioncache.cli.manage_cache.logger") as mock_logger:
            show_comprehensive_status()

            # Verify that the handler creation was attempted
            mock_get_cache_handler.assert_called_once_with("rocksdb_set")

            # Verify that error was logged appropriately
            mock_logger.error.assert_called()
            error_call = mock_logger.error.call_args[0][0]
            assert "Error accessing rocksdb_set cache" in error_call

    @patch("partitioncache.cli.manage_cache.detect_configured_cache_backends")
    @patch("partitioncache.cli.manage_cache.detect_configured_queue_providers")
    @patch("partitioncache.cli.manage_cache.get_queue_lengths")
    @patch("partitioncache.cli.manage_cache.get_cache_handler")
    @patch("partitioncache.cli.manage_cache.get_all_keys")
    def test_redis_successful_connection(self, mock_get_all_keys, mock_get_cache_handler,
                                       mock_get_queue_lengths, mock_detect_queue_providers,
                                       mock_detect_cache_backends):
        """Test that Redis connections work when successful."""

        # Setup mocks
        mock_detect_cache_backends.return_value = ["redis_set"]
        mock_detect_queue_providers.return_value = []
        mock_get_queue_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}

        # Mock successful Redis handler
        mock_redis_handler = MagicMock()
        # Make isinstance check pass
        mock_redis_handler.__class__ = RedisAbstractCacheHandler
        mock_redis_handler.db.ping.return_value = True  # Successful ping
        mock_get_cache_handler.return_value = mock_redis_handler
        mock_get_all_keys.return_value = ["query_1", "query_2", "_LIMIT_query_3", "_TIMEOUT_query_4"]

        # Call show_comprehensive_status - should work normally
        with patch("partitioncache.cli.manage_cache.logger") as mock_logger:
            show_comprehensive_status()

            # Verify that the connection was successful
            mock_redis_handler.db.ping.assert_called_once()
            mock_get_all_keys.assert_called_once_with(mock_redis_handler)
            mock_redis_handler.close.assert_called()

            # Verify that cache statistics were logged
            info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
            cache_entries_logged = any("Total Cache Entries: 4" in call for call in info_calls)
            valid_entries_logged = any("Valid Entries: 2" in call for call in info_calls)
            assert cache_entries_logged, f"Cache entries not logged. Info calls: {info_calls}"
            assert valid_entries_logged, f"Valid entries not logged. Info calls: {info_calls}"

    @patch("partitioncache.cli.manage_cache.detect_configured_cache_backends")
    @patch("partitioncache.cli.manage_cache.detect_configured_queue_providers")
    @patch("partitioncache.cli.manage_cache.get_queue_lengths")
    @patch("partitioncache.cli.manage_cache.get_cache_handler")
    def test_postgresql_backend_skips_connectivity_test(self, mock_get_cache_handler, mock_get_queue_lengths,
                                                       mock_detect_queue_providers, mock_detect_cache_backends):
        """Test that PostgreSQL backends skip the quick connectivity test."""

        # Setup mocks
        mock_detect_cache_backends.return_value = ["postgresql_array"]
        mock_detect_queue_providers.return_value = []
        mock_get_queue_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}

        # Mock PostgreSQL-specific logic (get_partition_overview)
        with patch("partitioncache.cli.manage_cache.get_partition_overview") as mock_get_partition_overview:
            mock_get_partition_overview.return_value = [
                {
                    "partition_key": "city_id",
                    "total_entries": 100,
                    "valid_entries": 95,
                    "limit_entries": 3,
                    "timeout_entries": 2
                }
            ]

            # Call show_comprehensive_status
            with patch("partitioncache.cli.manage_cache.logger") as mock_logger:
                show_comprehensive_status()

                # Verify get_cache_handler was NOT called for PostgreSQL (it uses get_partition_overview instead)
                mock_get_cache_handler.assert_not_called()

                # Verify PostgreSQL-specific function was called
                mock_get_partition_overview.assert_called_once_with("postgresql_array")

                # Verify that statistics were logged
                info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                total_entries_logged = any("Total Cache Entries: 100" in call for call in info_calls)
                assert total_entries_logged, f"PostgreSQL stats not logged. Info calls: {info_calls}"

    @patch("partitioncache.cli.manage_cache.detect_configured_cache_backends")
    @patch("partitioncache.cli.manage_cache.detect_configured_queue_providers")
    @patch("partitioncache.cli.manage_cache.get_queue_lengths")
    def test_multiple_backend_timeout_isolation(self, mock_get_queue_lengths, mock_detect_queue_providers,
                                               mock_detect_cache_backends):
        """Test that timeout in one backend doesn't affect others."""

        # Setup mocks
        mock_detect_cache_backends.return_value = ["redis_set", "rocksdb_bit"]
        mock_detect_queue_providers.return_value = []
        mock_get_queue_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}

        def mock_get_cache_handler_side_effect(backend):
            if backend == "redis_set":
                # Redis times out
                mock_handler = MagicMock()
                mock_handler.__class__ = RedisAbstractCacheHandler
                mock_handler.db.ping.side_effect = Exception("Redis timeout")
                return mock_handler
            elif backend == "rocksdb_bit":
                # RocksDB works fine
                mock_handler = MagicMock()
                # Create a mock class to represent RocksDBAbstractCacheHandler
                # This avoids import issues in CI environments without RocksDB
                MockRocksDBClass = type('RocksDBAbstractCacheHandler', (), {})
                mock_handler.__class__ = MockRocksDBClass
                mock_handler.db.iterkeys.return_value = iter(["key1", "key2"])
                mock_handler.close.return_value = None
                mock_handler.get_partition_keys.return_value = ["test_partition"]
                return mock_handler

        with patch("partitioncache.cli.manage_cache.get_cache_handler", side_effect=mock_get_cache_handler_side_effect):
            with patch("partitioncache.cli.manage_cache.get_all_keys") as mock_get_all_keys:
                mock_get_all_keys.return_value = ["query_1", "query_2"]

                with patch("partitioncache.cli.manage_cache.logger") as mock_logger:
                    show_comprehensive_status()

                    # Check that both backends were processed
                    error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
                    info_calls = [call[0][0] for call in mock_logger.info.call_args_list]

                    # Redis should have error
                    redis_error = any("Error accessing redis_set cache" in call for call in error_calls)
                    assert redis_error, f"Redis error not logged. Error calls: {error_calls}"

                    # RocksDB should have success (Total Cache Entries logged)
                    rocksdb_success = any("Total Cache Entries: 2" in call for call in info_calls)
                    assert rocksdb_success, f"RocksDB success not logged. Info calls: {info_calls}"

    @patch("partitioncache.cli.manage_cache.detect_configured_cache_backends")
    @patch("partitioncache.cli.manage_cache.detect_configured_queue_providers")
    @patch("partitioncache.cli.manage_cache.get_queue_lengths")
    @patch("partitioncache.cli.manage_cache.get_cache_handler")
    def test_cache_handler_without_redis_or_db_attributes(self, mock_get_cache_handler, mock_get_queue_lengths,
                                                         mock_detect_queue_providers, mock_detect_cache_backends):
        """Test handling of cache handlers without redis_client or db attributes."""

        # Setup mocks
        mock_detect_cache_backends.return_value = ["redis_set"]
        mock_detect_queue_providers.return_value = []
        mock_get_queue_lengths.return_value = {"original_query_queue": 0, "query_fragment_queue": 0}

        # Mock handler without db attribute (edge case)
        mock_handler = MagicMock()
        del mock_handler.db  # Remove the attribute
        mock_get_cache_handler.return_value = mock_handler

        with patch("partitioncache.cli.manage_cache.get_all_keys") as mock_get_all_keys:
            mock_get_all_keys.return_value = ["query_1"]

            with patch("partitioncache.cli.manage_cache.logger") as mock_logger:
                show_comprehensive_status()

                # Should skip connectivity test and proceed normally
                mock_get_all_keys.assert_called_once_with(mock_handler)
                mock_handler.close.assert_called()

                # Should log cache statistics
                info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                cache_entries_logged = any("Total Cache Entries: 1" in call for call in info_calls)
                assert cache_entries_logged, f"Cache entries not logged. Info calls: {info_calls}"

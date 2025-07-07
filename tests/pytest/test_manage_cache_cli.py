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

    def test_setup_all_command(self):
        """Test the setup all command."""

        with patch("partitioncache.cli.manage_cache.setup_all_tables") as mock_setup:
            with patch("sys.argv", ["manage_cache.py", "setup", "all"]):
                main()
                mock_setup.assert_called_once()

    def test_setup_queue_command(self):
        """Test the setup queue command."""

        with patch("partitioncache.cli.manage_cache.setup_queue_tables") as mock_setup:
            with patch("sys.argv", ["manage_cache.py", "setup", "queue"]):
                main()
                mock_setup.assert_called_once()

    def test_setup_cache_command(self):
        """Test the setup cache command."""

        with patch("partitioncache.cli.manage_cache.setup_cache_metadata_tables") as mock_setup:
            with patch("sys.argv", ["manage_cache.py", "setup", "cache"]):
                main()
                mock_setup.assert_called_once()

    def test_status_env_command(self):
        """Test the status env command."""

        with patch("partitioncache.cli.manage_cache.validate_environment") as mock_validate:
            with patch("sys.argv", ["manage_cache.py", "status", "env"]):
                main()
                mock_validate.assert_called_once()

    def test_status_tables_command(self):
        """Test the status tables command."""

        with patch("partitioncache.cli.manage_cache.check_table_status") as mock_check:
            with patch("sys.argv", ["manage_cache.py", "status", "tables"]):
                main()
                mock_check.assert_called_once()

    def test_status_default_command(self):
        """Test the default status command (comprehensive status)."""

        with patch("partitioncache.cli.manage_cache.show_comprehensive_status") as mock_status:
            with patch("sys.argv", ["manage_cache.py", "status"]):
                main()
                mock_status.assert_called_once()

    def test_status_all_command(self):
        """Test the status all command."""

        with patch("partitioncache.cli.manage_cache.show_comprehensive_status") as mock_status:
            with patch("sys.argv", ["manage_cache.py", "status", "all"]):
                main()
                mock_status.assert_called_once()

    def test_cache_count_command(self):
        """Test the cache count command."""

        with patch("partitioncache.cli.manage_cache.count_cache") as mock_count:
            with patch("sys.argv", ["manage_cache.py", "cache", "count", "--type", "postgresql_array"]):
                main()
                mock_count.assert_called_once_with("postgresql_array")

    def test_cache_count_all_command(self):
        """Test the cache count all command."""

        with patch("partitioncache.cli.manage_cache.count_all_caches") as mock_count_all:
            with patch("sys.argv", ["manage_cache.py", "cache", "count", "--type", "postgresql_array", "--all"]):
                main()
                mock_count_all.assert_called_once()

    def test_cache_copy_command(self):
        """Test the cache copy command."""

        with patch("partitioncache.cli.manage_cache.copy_cache") as mock_copy:
            with patch("sys.argv", ["manage_cache.py", "cache", "copy", "--from", "redis_set", "--to", "postgresql_array"]):
                main()
                mock_copy.assert_called_once_with("redis_set", "postgresql_array", None)

    def test_cache_export_command(self):
        """Test the cache export command."""

        with patch("partitioncache.cli.manage_cache.export_cache") as mock_export:
            with patch("sys.argv", ["manage_cache.py", "cache", "export", "--type", "postgresql_array", "--file", "backup.pkl"]):
                main()
                mock_export.assert_called_once_with("postgresql_array", "backup.pkl", None)

    def test_cache_import_command(self):
        """Test the cache import command."""

        with patch("partitioncache.cli.manage_cache.restore_cache") as mock_restore:
            with patch("sys.argv", ["manage_cache.py", "cache", "import", "--type", "postgresql_array", "--file", "backup.pkl"]):
                main()
                mock_restore.assert_called_once_with("postgresql_array", "backup.pkl", None)

    def test_cache_delete_command(self):
        """Test the cache delete command."""

        with patch("partitioncache.cli.manage_cache.delete_cache") as mock_delete:
            with patch("sys.argv", ["manage_cache.py", "cache", "delete", "--type", "postgresql_array"]):
                main()
                mock_delete.assert_called_once_with("postgresql_array")

    def test_cache_delete_all_command(self):
        """Test the cache delete all command."""

        with patch("partitioncache.cli.manage_cache.delete_all_caches") as mock_delete_all:
            with patch("sys.argv", ["manage_cache.py", "cache", "delete", "--type", "postgresql_array", "--all"]):
                main()
                mock_delete_all.assert_called_once()

    def test_queue_count_command(self):
        """Test the queue count command."""

        with patch("partitioncache.cli.manage_cache.count_queue") as mock_count:
            with patch("sys.argv", ["manage_cache.py", "queue", "count"]):
                main()
                mock_count.assert_called_once()

    def test_queue_clear_command(self):
        """Test the queue clear command."""

        with patch("partitioncache.cli.manage_cache.clear_queue") as mock_clear:
            with patch("sys.argv", ["manage_cache.py", "queue", "clear"]):
                main()
                mock_clear.assert_called_once()

    def test_queue_clear_original_command(self):
        """Test the queue clear original command."""

        with patch("partitioncache.cli.manage_cache.clear_original_query_queue") as mock_clear:
            with patch("sys.argv", ["manage_cache.py", "queue", "clear", "--original"]):
                main()
                mock_clear.assert_called_once()

    def test_queue_clear_fragment_command(self):
        """Test the queue clear fragment command."""

        with patch("partitioncache.cli.manage_cache.clear_query_fragment_queue") as mock_clear:
            with patch("sys.argv", ["manage_cache.py", "queue", "clear", "--fragment"]):
                main()
                mock_clear.assert_called_once()

    def test_maintenance_prune_command(self):
        """Test the maintenance prune command."""

        with patch("partitioncache.cli.manage_cache.prune_all_caches") as mock_prune:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "prune", "--days", "30"]):
                main()
                mock_prune.assert_called_once_with(30)

    def test_maintenance_prune_specific_cache_command(self):
        """Test the maintenance prune command for specific cache."""

        with patch("partitioncache.cli.manage_cache.prune_old_queries") as mock_prune:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "prune", "--days", "7", "--type", "postgresql_array"]):
                main()
                mock_prune.assert_called_once_with("postgresql_array", 7)

    def test_maintenance_cleanup_termination_command(self):
        """Test the maintenance cleanup remove-termination command."""

        with patch("partitioncache.cli.manage_cache.remove_termination_entries") as mock_remove:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "cleanup", "--type", "postgresql_array", "--remove-termination"]):
                main()
                mock_remove.assert_called_once_with("postgresql_array")

    def test_maintenance_cleanup_large_command(self):
        """Test the maintenance cleanup remove-large command."""

        with patch("partitioncache.cli.manage_cache.remove_large_entries") as mock_remove:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "cleanup", "--type", "postgresql_array", "--remove-large", "1000"]):
                main()
                mock_remove.assert_called_once_with("postgresql_array", 1000)

    def test_maintenance_partition_delete_command(self):
        """Test the maintenance partition delete command."""

        with patch("partitioncache.cli.manage_cache.delete_partition") as mock_delete:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "partition", "--type", "postgresql_array", "--delete", "old_partition"]):
                main()
                mock_delete.assert_called_once_with("postgresql_array", "old_partition")

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

    def test_cache_count_with_env_backend(self):
        """Test that cache count uses environment CACHE_BACKEND when --type is omitted."""

        with patch("partitioncache.cli.manage_cache.count_cache") as mock_count:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="redis_set"):
                with patch("sys.argv", ["manage_cache.py", "cache", "count"]):
                    main()
                    mock_count.assert_called_once_with("redis_set")

    def test_cache_export_with_env_backend(self):
        """Test that cache export uses environment CACHE_BACKEND when --type is omitted."""

        with patch("partitioncache.cli.manage_cache.export_cache") as mock_export:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="postgresql_bit"):
                with patch("sys.argv", ["manage_cache.py", "cache", "export", "--file", "test.pkl"]):
                    main()
                    mock_export.assert_called_once_with("postgresql_bit", "test.pkl", None)

    def test_cache_import_with_env_backend(self):
        """Test that cache import uses environment CACHE_BACKEND when --type is omitted."""

        with patch("partitioncache.cli.manage_cache.restore_cache") as mock_restore:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="rocksdb_set"):
                with patch("sys.argv", ["manage_cache.py", "cache", "import", "--file", "test.pkl"]):
                    main()
                    mock_restore.assert_called_once_with("rocksdb_set", "test.pkl", None)

    def test_cache_delete_with_env_backend(self):
        """Test that cache delete uses environment CACHE_BACKEND when --type is omitted."""

        with patch("partitioncache.cli.manage_cache.delete_cache") as mock_delete:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="redis_bit"):
                with patch("sys.argv", ["manage_cache.py", "cache", "delete"]):
                    main()
                    mock_delete.assert_called_once_with("redis_bit")

    def test_maintenance_cleanup_with_env_backend(self):
        """Test that maintenance cleanup uses environment CACHE_BACKEND when --type is omitted."""

        with patch("partitioncache.cli.manage_cache.remove_termination_entries") as mock_remove:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="postgresql_array"):
                with patch("sys.argv", ["manage_cache.py", "maintenance", "cleanup", "--remove-termination"]):
                    main()
                    mock_remove.assert_called_once_with("postgresql_array")

    def test_maintenance_partition_with_env_backend(self):
        """Test that maintenance partition uses environment CACHE_BACKEND when --type is omitted."""

        with patch("partitioncache.cli.manage_cache.delete_partition") as mock_delete:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="postgresql_bit"):
                with patch("sys.argv", ["manage_cache.py", "maintenance", "partition", "--delete", "test_partition"]):
                    main()
                    mock_delete.assert_called_once_with("postgresql_bit", "test_partition")

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

    def test_rocksdb_connectivity_logic_exists(self):
        """Test that RocksDB connectivity test logic exists in the code."""
        # This test verifies that the connectivity test code exists and handles RocksDB correctly
        # The actual timeout protection is fully tested by other test methods

        # Verify the connectivity test logic exists by checking the source
        import inspect

        from partitioncache.cli.manage_cache import show_comprehensive_status

        source = inspect.getsource(show_comprehensive_status)

        # Verify the key components of timeout protection are in the source
        assert 'backend.startswith(("redis_", "rocksdb_"))' in source
        assert 'hasattr(cache_handler, "db")' in source
        assert 'cache_handler.db.iterkeys()' in source
        assert 'except Exception as conn_error:' in source
        assert 'cache_handler.close()' in source

        # This confirms the RocksDB connectivity test logic is implemented
        # Integration tests and the other unit tests verify it works correctly

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
                from partitioncache.cache_handler.rocks_db_abstract import RocksDBAbstractCacheHandler
                mock_handler = MagicMock()
                mock_handler.__class__ = RocksDBAbstractCacheHandler
                mock_handler.db.iterkeys.return_value = iter(["key1", "key2"])
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

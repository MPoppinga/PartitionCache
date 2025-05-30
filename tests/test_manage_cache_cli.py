"""Tests for the improved manage_cache CLI with subparsers."""

import pytest
from unittest.mock import patch, Mock, MagicMock
import sys
from io import StringIO
import os


class TestManageCacheCLI:
    """Test the new subparser-based CLI structure."""
    
    def test_help_displays_commands(self):
        """Test that the main help shows available commands."""
        from partitioncache.cli.manage_cache import main
        
        with patch("sys.argv", ["manage_cache.py", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    def test_setup_all_command(self):
        """Test the setup all command."""
        from partitioncache.cli.manage_cache import main, setup_all_tables
        
        with patch("partitioncache.cli.manage_cache.setup_all_tables") as mock_setup:
            with patch("sys.argv", ["manage_cache.py", "setup", "all"]):
                main()
                mock_setup.assert_called_once()

    def test_setup_queue_command(self):
        """Test the setup queue command."""
        from partitioncache.cli.manage_cache import main, setup_queue_tables
        
        with patch("partitioncache.cli.manage_cache.setup_queue_tables") as mock_setup:
            with patch("sys.argv", ["manage_cache.py", "setup", "queue"]):
                main()
                mock_setup.assert_called_once()

    def test_setup_cache_command(self):
        """Test the setup cache command."""
        from partitioncache.cli.manage_cache import main, setup_cache_metadata_tables
        
        with patch("partitioncache.cli.manage_cache.setup_cache_metadata_tables") as mock_setup:
            with patch("sys.argv", ["manage_cache.py", "setup", "cache"]):
                main()
                mock_setup.assert_called_once()

    def test_status_env_command(self):
        """Test the status env command."""
        from partitioncache.cli.manage_cache import main, validate_environment
        
        with patch("partitioncache.cli.manage_cache.validate_environment") as mock_validate:
            with patch("sys.argv", ["manage_cache.py", "status", "env"]):
                main()
                mock_validate.assert_called_once()

    def test_status_tables_command(self):
        """Test the status tables command."""
        from partitioncache.cli.manage_cache import main, check_table_status
        
        with patch("partitioncache.cli.manage_cache.check_table_status") as mock_check:
            with patch("sys.argv", ["manage_cache.py", "status", "tables"]):
                main()
                mock_check.assert_called_once()

    def test_status_default_command(self):
        """Test the default status command (both env and tables)."""
        from partitioncache.cli.manage_cache import main
        
        with patch("partitioncache.cli.manage_cache.validate_environment") as mock_validate:
            with patch("partitioncache.cli.manage_cache.check_table_status") as mock_check:
                with patch("sys.argv", ["manage_cache.py", "status"]):
                    main()
                    mock_validate.assert_called_once()
                    mock_check.assert_called_once()

    def test_cache_count_command(self):
        """Test the cache count command."""
        from partitioncache.cli.manage_cache import main, count_cache
        
        with patch("partitioncache.cli.manage_cache.count_cache") as mock_count:
            with patch("sys.argv", ["manage_cache.py", "cache", "count", "--type", "postgresql_array"]):
                main()
                mock_count.assert_called_once_with("postgresql_array")

    def test_cache_count_all_command(self):
        """Test the cache count all command."""
        from partitioncache.cli.manage_cache import main, count_all_caches
        
        with patch("partitioncache.cli.manage_cache.count_all_caches") as mock_count_all:
            with patch("sys.argv", ["manage_cache.py", "cache", "count", "--type", "postgresql_array", "--all"]):
                main()
                mock_count_all.assert_called_once()

    def test_cache_copy_command(self):
        """Test the cache copy command."""
        from partitioncache.cli.manage_cache import main, copy_cache
        
        with patch("partitioncache.cli.manage_cache.copy_cache") as mock_copy:
            with patch("sys.argv", ["manage_cache.py", "cache", "copy", "--from", "redis", "--to", "postgresql_array"]):
                main()
                mock_copy.assert_called_once_with("redis", "postgresql_array")

    def test_cache_export_command(self):
        """Test the cache export command."""
        from partitioncache.cli.manage_cache import main, export_cache
        
        with patch("partitioncache.cli.manage_cache.export_cache") as mock_export:
            with patch("sys.argv", ["manage_cache.py", "cache", "export", "--type", "postgresql_array", "--file", "backup.pkl"]):
                main()
                mock_export.assert_called_once_with("postgresql_array", "backup.pkl")

    def test_cache_import_command(self):
        """Test the cache import command."""
        from partitioncache.cli.manage_cache import main, restore_cache
        
        with patch("partitioncache.cli.manage_cache.restore_cache") as mock_restore:
            with patch("sys.argv", ["manage_cache.py", "cache", "import", "--type", "postgresql_array", "--file", "backup.pkl"]):
                main()
                mock_restore.assert_called_once_with("postgresql_array", "backup.pkl")

    def test_cache_delete_command(self):
        """Test the cache delete command."""
        from partitioncache.cli.manage_cache import main, delete_cache
        
        with patch("partitioncache.cli.manage_cache.delete_cache") as mock_delete:
            with patch("sys.argv", ["manage_cache.py", "cache", "delete", "--type", "postgresql_array"]):
                main()
                mock_delete.assert_called_once_with("postgresql_array")

    def test_cache_delete_all_command(self):
        """Test the cache delete all command."""
        from partitioncache.cli.manage_cache import main, delete_all_caches
        
        with patch("partitioncache.cli.manage_cache.delete_all_caches") as mock_delete_all:
            with patch("sys.argv", ["manage_cache.py", "cache", "delete", "--type", "postgresql_array", "--all"]):
                main()
                mock_delete_all.assert_called_once()

    def test_queue_count_command(self):
        """Test the queue count command."""
        from partitioncache.cli.manage_cache import main, count_queue
        
        with patch("partitioncache.cli.manage_cache.count_queue") as mock_count:
            with patch("sys.argv", ["manage_cache.py", "queue", "count"]):
                main()
                mock_count.assert_called_once()

    def test_queue_clear_command(self):
        """Test the queue clear command."""
        from partitioncache.cli.manage_cache import main, clear_queue
        
        with patch("partitioncache.cli.manage_cache.clear_queue") as mock_clear:
            with patch("sys.argv", ["manage_cache.py", "queue", "clear"]):
                main()
                mock_clear.assert_called_once()

    def test_queue_clear_original_command(self):
        """Test the queue clear original command."""
        from partitioncache.cli.manage_cache import main, clear_original_query_queue
        
        with patch("partitioncache.cli.manage_cache.clear_original_query_queue") as mock_clear:
            with patch("sys.argv", ["manage_cache.py", "queue", "clear", "--original"]):
                main()
                mock_clear.assert_called_once()

    def test_queue_clear_fragment_command(self):
        """Test the queue clear fragment command."""
        from partitioncache.cli.manage_cache import main, clear_query_fragment_queue
        
        with patch("partitioncache.cli.manage_cache.clear_query_fragment_queue") as mock_clear:
            with patch("sys.argv", ["manage_cache.py", "queue", "clear", "--fragment"]):
                main()
                mock_clear.assert_called_once()

    def test_maintenance_prune_command(self):
        """Test the maintenance prune command."""
        from partitioncache.cli.manage_cache import main, prune_all_caches
        
        with patch("partitioncache.cli.manage_cache.prune_all_caches") as mock_prune:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "prune", "--days", "30"]):
                main()
                mock_prune.assert_called_once_with(30)

    def test_maintenance_prune_specific_cache_command(self):
        """Test the maintenance prune command for specific cache."""
        from partitioncache.cli.manage_cache import main, prune_old_queries
        
        with patch("partitioncache.cli.manage_cache.prune_old_queries") as mock_prune:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "prune", "--days", "7", "--type", "postgresql_array"]):
                main()
                mock_prune.assert_called_once_with("postgresql_array", 7)

    def test_maintenance_cleanup_termination_command(self):
        """Test the maintenance cleanup remove-termination command."""
        from partitioncache.cli.manage_cache import main, remove_termination_entries
        
        with patch("partitioncache.cli.manage_cache.remove_termination_entries") as mock_remove:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "cleanup", "--type", "postgresql_array", "--remove-termination"]):
                main()
                mock_remove.assert_called_once_with("postgresql_array")

    def test_maintenance_cleanup_large_command(self):
        """Test the maintenance cleanup remove-large command."""
        from partitioncache.cli.manage_cache import main, remove_large_entries
        
        with patch("partitioncache.cli.manage_cache.remove_large_entries") as mock_remove:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "cleanup", "--type", "postgresql_array", "--remove-large", "1000"]):
                main()
                mock_remove.assert_called_once_with("postgresql_array", 1000)

    def test_maintenance_partition_delete_command(self):
        """Test the maintenance partition delete command."""
        from partitioncache.cli.manage_cache import main, delete_partition
        
        with patch("partitioncache.cli.manage_cache.delete_partition") as mock_delete:
            with patch("sys.argv", ["manage_cache.py", "maintenance", "partition", "--type", "postgresql_array", "--delete", "old_partition"]):
                main()
                mock_delete.assert_called_once_with("postgresql_array", "old_partition")

    def test_no_command_shows_help(self):
        """Test that running without commands shows help."""
        from partitioncache.cli.manage_cache import main
        
        with patch("sys.argv", ["manage_cache.py"]):
            with patch("argparse.ArgumentParser.print_help") as mock_help:
                main()
                mock_help.assert_called_once()

    def test_partial_command_shows_subhelp(self):
        """Test that partial commands show subcommand help."""
        from partitioncache.cli.manage_cache import main
        
        # Test setup without subcommand
        with patch("sys.argv", ["manage_cache.py", "setup"]):
            with patch("argparse.ArgumentParser.print_help") as mock_help:
                main()
                mock_help.assert_called_once()

    @patch("partitioncache.cli.manage_cache.load_dotenv")
    def test_env_file_loading(self, mock_load_dotenv):
        """Test that environment file loading works."""
        from partitioncache.cli.manage_cache import main
        
        with patch("partitioncache.cli.manage_cache.validate_environment"):
            with patch("sys.argv", ["manage_cache.py", "--env", "custom.env", "status", "env"]):
                main()
                mock_load_dotenv.assert_called_once_with("custom.env")

    def test_error_handling(self):
        """Test that exceptions are handled gracefully."""
        from partitioncache.cli.manage_cache import main
        
        with patch("partitioncache.cli.manage_cache.validate_environment", side_effect=Exception("Test error")):
            with patch("sys.argv", ["manage_cache.py", "status", "env"]):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1

    def test_cache_count_with_env_backend(self):
        """Test that cache count uses environment CACHE_BACKEND when --type is omitted."""
        from partitioncache.cli.manage_cache import main, count_cache
        
        with patch("partitioncache.cli.manage_cache.count_cache") as mock_count:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="redis"):
                with patch("sys.argv", ["manage_cache.py", "cache", "count"]):
                    main()
                    mock_count.assert_called_once_with("redis")

    def test_cache_export_with_env_backend(self):
        """Test that cache export uses environment CACHE_BACKEND when --type is omitted."""
        from partitioncache.cli.manage_cache import main, export_cache
        
        with patch("partitioncache.cli.manage_cache.export_cache") as mock_export:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="postgresql_bit"):
                with patch("sys.argv", ["manage_cache.py", "cache", "export", "--file", "test.pkl"]):
                    main()
                    mock_export.assert_called_once_with("postgresql_bit", "test.pkl")

    def test_cache_import_with_env_backend(self):
        """Test that cache import uses environment CACHE_BACKEND when --type is omitted."""
        from partitioncache.cli.manage_cache import main, restore_cache
        
        with patch("partitioncache.cli.manage_cache.restore_cache") as mock_restore:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="rocksdb"):
                with patch("sys.argv", ["manage_cache.py", "cache", "import", "--file", "test.pkl"]):
                    main()
                    mock_restore.assert_called_once_with("rocksdb", "test.pkl")

    def test_cache_delete_with_env_backend(self):
        """Test that cache delete uses environment CACHE_BACKEND when --type is omitted."""
        from partitioncache.cli.manage_cache import main, delete_cache
        
        with patch("partitioncache.cli.manage_cache.delete_cache") as mock_delete:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="redis_bit"):
                with patch("sys.argv", ["manage_cache.py", "cache", "delete"]):
                    main()
                    mock_delete.assert_called_once_with("redis_bit")

    def test_maintenance_cleanup_with_env_backend(self):
        """Test that maintenance cleanup uses environment CACHE_BACKEND when --type is omitted."""
        from partitioncache.cli.manage_cache import main, remove_termination_entries
        
        with patch("partitioncache.cli.manage_cache.remove_termination_entries") as mock_remove:
            with patch("partitioncache.cli.manage_cache.get_cache_type_from_env", return_value="postgresql_array"):
                with patch("sys.argv", ["manage_cache.py", "maintenance", "cleanup", "--remove-termination"]):
                    main()
                    mock_remove.assert_called_once_with("postgresql_array")

    def test_maintenance_partition_with_env_backend(self):
        """Test that maintenance partition uses environment CACHE_BACKEND when --type is omitted."""
        from partitioncache.cli.manage_cache import main, delete_partition
        
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
        mock_getenv.return_value = "redis"
        result = get_cache_type_from_env()
        assert result == "redis"
        mock_getenv.assert_called_with("CACHE_BACKEND", "postgresql_array")
        
        # Test with default fallback - when getenv returns the default value
        mock_getenv.return_value = "postgresql_array"  # This is what happens when getenv gets the default
        result = get_cache_type_from_env()
        assert result == "postgresql_array" 
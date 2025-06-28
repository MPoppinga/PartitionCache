"""Tests for PostgreSQL direct queue processor."""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from partitioncache.cli.setup_postgresql_queue_processor import (
    validate_environment,
    check_pg_cron_installed,
    get_processor_status,
    SQL_FILE,
)


class TestEnvironmentValidation:
    """Test environment validation functionality."""

    def test_validate_environment_missing_vars(self):
        """Test validation fails when required variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            valid, message = validate_environment()
            assert not valid
            assert "Missing environment variables" in message

    def test_validate_environment_different_instances(self):
        """Test validation fails when queue and cache are on different instances."""
        env = {
            "PG_QUEUE_HOST": "host1",
            "PG_QUEUE_PORT": "5432",
            "PG_QUEUE_USER": "user",
            "PG_QUEUE_PASSWORD": "pass",
            "PG_QUEUE_DB": "db1",
            "DB_HOST": "host2",  # Different host
            "DB_PORT": "5432",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "db1",
            "CACHE_BACKEND": "postgresql_array",
        }
        with patch.dict(os.environ, env, clear=True):
            valid, message = validate_environment()
            assert not valid
            assert "must be the same instance" in message

    def test_validate_environment_unsupported_cache(self):
        """Test validation fails for unsupported cache backend."""
        env = {
            "PG_QUEUE_HOST": "localhost",
            "PG_QUEUE_PORT": "5432",
            "PG_QUEUE_USER": "user",
            "PG_QUEUE_PASSWORD": "pass",
            "PG_QUEUE_DB": "testdb",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb",
            "CACHE_BACKEND": "redis_set",  # Not supported
        }
        with patch.dict(os.environ, env, clear=True):
            valid, message = validate_environment()
            assert not valid
            assert "Unsupported CACHE_BACKEND for direct processor" in message

    def test_validate_environment_success(self):
        """Test validation succeeds with correct configuration."""
        env = {
            "PG_QUEUE_HOST": "localhost",
            "PG_QUEUE_PORT": "5432",
            "PG_QUEUE_USER": "user",
            "PG_QUEUE_PASSWORD": "pass",
            "PG_QUEUE_DB": "testdb",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb",
            "CACHE_BACKEND": "postgresql_array",
            "PG_ARRAY_CACHE_TABLE_PREFIX": "partitioncache",
        }
        with patch.dict(os.environ, env, clear=True):
            valid, message = validate_environment()
            assert valid
            assert "validated successfully" in message


class TestPgCronCheck:
    """Test pg_cron extension checking."""

    def test_check_pg_cron_installed_true(self):
        """Test when pg_cron is already installed."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (1,)

        assert check_pg_cron_installed(mock_conn) is True

        # Should only make one call to check if it exists (since it does)
        assert mock_cursor.execute.call_count == 1
        calls = mock_cursor.execute.call_args_list
        assert "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'" in calls[0][0][0]

    def test_check_pg_cron_installed_false(self):
        """Test when pg_cron is not installed and cannot be created."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = None
        # Simulate CREATE EXTENSION failing
        mock_cursor.execute.side_effect = [None, Exception("permission denied")]

        assert check_pg_cron_installed(mock_conn) is False

    def test_check_pg_cron_installed_create_success(self):
        """Test when pg_cron is not installed but can be created."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        # First check returns None (not installed), CREATE succeeds
        mock_cursor.fetchone.return_value = None
        mock_cursor.execute.side_effect = [None, None]  # SELECT succeeds, CREATE succeeds

        assert check_pg_cron_installed(mock_conn) is True

        # Should make two calls: SELECT to check and CREATE EXTENSION
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        assert "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'" in calls[0][0][0]
        assert "CREATE EXTENSION IF NOT EXISTS pg_cron" in calls[1][0][0]


class TestProcessorStatus:
    """Test processor status retrieval."""

    def test_get_processor_status_success(self):
        """Test successful status retrieval."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Mock status result
        mock_cursor.fetchone.return_value = (
            True,  # enabled
            5,  # max_parallel_jobs
            1,  # frequency_seconds
            2,  # active_jobs
            10,  # queue_length
            50,  # recent_successes
            3,  # recent_failures
        )
        mock_cursor.description = [
            ("enabled",),
            ("max_parallel_jobs",),
            ("frequency_seconds",),
            ("active_jobs",),
            ("queue_length",),
            ("recent_successes",),
            ("recent_failures",),
        ]

        status = get_processor_status(mock_conn, "partitioncache_queue")

        assert status is not None
        assert status["enabled"] is True
        assert status["max_parallel_jobs"] == 5
        assert status["frequency_seconds"] == 1
        assert status["active_jobs"] == 2
        assert status["queue_length"] == 10
        assert status["recent_successes"] == 50
        assert status["recent_failures"] == 3

    def test_get_processor_status_not_initialized(self):
        """Test status when processor not initialized."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = None

        status = get_processor_status(mock_conn, "partitioncache_queue")
        assert status is None


class TestSQLFile:
    """Test SQL file handling."""

    def test_sql_file_exists(self):
        """Test that the SQL file path is correctly constructed."""
        assert SQL_FILE.name == "postgresql_queue_processor.sql"
        assert "queue_handler" in str(SQL_FILE)


class TestDatabaseOperations:
    """Test database operation functions."""

    @patch("partitioncache.cli.setup_postgresql_queue_processor.SQL_FILE")
    @patch("partitioncache.queue_handler.get_queue_handler")
    @patch("partitioncache.cache_handler.get_cache_handler")
    @patch("partitioncache.cli.setup_postgresql_queue_processor.get_queue_table_prefix_from_env")
    def test_setup_database_objects(self, mock_get_queue_prefix, mock_get_cache_handler, mock_get_queue_handler, mock_sql_file):
        """Test setting up database objects."""
        from partitioncache.cli.setup_postgresql_queue_processor import setup_database_objects

        # Mock handlers
        mock_queue_handler = Mock()
        mock_get_queue_handler.return_value = mock_queue_handler

        mock_cache_handler = Mock()
        mock_get_cache_handler.return_value = mock_cache_handler

        # Mock queue prefix
        mock_get_queue_prefix.return_value = "partitioncache_queue"

        # Mock SQL file content
        mock_sql_file.read_text.return_value = "CREATE TABLE test_table;"

        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Test database objects setup
        setup_database_objects(mock_conn)

        # Verify queue handler was initialized and closed
        mock_get_queue_handler.assert_called_once()
        mock_queue_handler.close.assert_called_once()

        # Verify cache handler was initialized and closed
        mock_get_cache_handler.assert_called_once_with("postgresql_array")
        mock_cache_handler.close.assert_called_once()

        # Verify SQL execution - should be called four times:
        # 1. Main SQL file content
        # 2. Info SQL file content
        # 3. partitioncache_initialize_processor_tables function call
        # 4. partitioncache_create_config_trigger function call
        assert mock_cursor.execute.call_count == 4

        # Check the first call (main SQL content)
        first_call = mock_cursor.execute.call_args_list[0]
        assert first_call[0][0] == "CREATE TABLE test_table;"

        # Check the second call (info SQL content)
        second_call = mock_cursor.execute.call_args_list[1]
        assert isinstance(second_call[0][0], str)

        # Check the third call (partitioncache_initialize_processor_tables)
        third_call = mock_cursor.execute.call_args_list[2]
        assert "SELECT partitioncache_initialize_processor_tables(%s)" in third_call[0][0]
        assert third_call[0][1] == ["partitioncache_queue"]

        # Check the fourth call (partitioncache_create_config_trigger)
        fourth_call = mock_cursor.execute.call_args_list[3]
        assert "SELECT partitioncache_create_config_trigger(%s)" in fourth_call[0][0]
        assert fourth_call[0][1] == ["partitioncache_queue"]

        mock_conn.commit.assert_called_once()

    def test_insert_initial_config(self):
        """Test inserting initial configuration."""
        from partitioncache.cli.setup_postgresql_queue_processor import insert_initial_config

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Test inserting configuration
        insert_initial_config(mock_conn, "partitioncache_process_queue", "partitioncache", "partitioncache_queue", "array", 1, True, 1800)

        # Should execute INSERT statement
        assert mock_cursor.execute.call_count == 1
        insert_call = mock_cursor.execute.call_args_list[0]
        assert "INSERT INTO" in str(insert_call[0][0])
        assert "partitioncache_queue_processor_config" in str(insert_call[0][0])

        mock_conn.commit.assert_called_once()


class TestCLIIntegration:
    """Test CLI integration points."""

    @patch("partitioncache.cli.setup_postgresql_queue_processor.get_queue_table_prefix_from_env")
    @patch("partitioncache.cli.setup_postgresql_queue_processor.get_db_connection")
    @patch("partitioncache.cli.setup_postgresql_queue_processor.validate_environment")
    @patch("partitioncache.cli.setup_postgresql_queue_processor.dotenv.load_dotenv")
    def test_main_status_command(self, mock_load_env, mock_validate, mock_get_conn, mock_get_queue_prefix):
        """Test the status command."""
        from partitioncache.cli.setup_postgresql_queue_processor import main

        # Mock environment validation
        mock_validate.return_value = (True, "OK")

        # Mock environment functions
        mock_get_queue_prefix.return_value = "partitioncache_queue"

        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (True, 5, 1, 0, 0, 0, 0)
        mock_cursor.description = [
            ("enabled",),
            ("max_parallel_jobs",),
            ("frequency_seconds",),
            ("active_jobs",),
            ("queue_length",),
            ("recent_successes",),
            ("recent_failures",),
        ]

        # Mock command line arguments
        with patch("sys.argv", ["setup_postgresql_queue_processor.py", "status"]):
            main()

        # Should execute status query with queue prefix only
        mock_cursor.execute.assert_called_with(
            "SELECT * FROM partitioncache_get_processor_status(%s, %s)", ["partitioncache_queue", "partitioncache_process_queue"]
        )

    @patch("partitioncache.cli.setup_postgresql_queue_processor.get_db_connection")
    @patch("partitioncache.cli.setup_postgresql_queue_processor.validate_environment")
    def test_main_invalid_environment(self, mock_validate, mock_get_conn):
        """Test handling of invalid environment."""
        from partitioncache.cli.setup_postgresql_queue_processor import main

        # Mock environment validation failure
        mock_validate.return_value = (False, "Invalid configuration")

        # Mock command line arguments
        with patch("sys.argv", ["setup_postgresql_queue_processor.py", "status"]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        mock_get_conn.assert_not_called()

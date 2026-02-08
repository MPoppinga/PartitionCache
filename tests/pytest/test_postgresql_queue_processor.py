"""Tests for PostgreSQL direct queue processor."""

import os
from unittest.mock import Mock, patch

import pytest

from partitioncache.cli.postgresql_queue_processor import (
    SQL_CRON_FILE,
    check_and_grant_pg_cron_permissions,
    check_pg_cron_installed,
    get_pg_cron_connection,
    get_processor_status,
    validate_environment,
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

    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    def test_check_pg_cron_installed_true(self, mock_get_conn):
        """Test when pg_cron is already installed."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (1,)

        assert check_pg_cron_installed() is True

        # Should only make one call to check if it exists (since it does)
        assert mock_cursor.execute.call_count == 1
        calls = mock_cursor.execute.call_args_list
        assert "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'" in calls[0][0][0]

    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    def test_check_pg_cron_installed_false(self, mock_get_conn):
        """Test when pg_cron is not installed and cannot be created."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = None
        # Simulate CREATE EXTENSION failing
        mock_cursor.execute.side_effect = [None, Exception("permission denied")]

        assert check_pg_cron_installed() is False

    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    def test_check_pg_cron_installed_create_success(self, mock_get_conn):
        """Test when pg_cron is not installed but can be created."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        # First check returns None (not installed), CREATE succeeds
        mock_cursor.fetchone.return_value = None
        mock_cursor.execute.side_effect = [None, None]  # SELECT succeeds, CREATE succeeds
        mock_conn.commit = Mock()

        assert check_pg_cron_installed() is True

        # Should make two calls: SELECT to check and CREATE EXTENSION
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        assert "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'" in calls[0][0][0]
        assert "CREATE EXTENSION IF NOT EXISTS pg_cron" in calls[1][0][0]


class TestPgCronConnection:
    """Test pg_cron connection functions."""

    @patch.dict(
        os.environ,
        {"PG_CRON_HOST": "cron_host", "PG_CRON_PORT": "5433", "PG_CRON_USER": "cron_user", "PG_CRON_PASSWORD": "cron_pass", "PG_CRON_DATABASE": "cron_db"},
    )
    @patch("psycopg.connect")
    def test_get_pg_cron_connection_with_explicit_vars(self, mock_connect):
        """Test connection with explicit PG_CRON_* variables."""
        get_pg_cron_connection()

        mock_connect.assert_called_once_with(host="cron_host", port=5433, user="cron_user", password="cron_pass", dbname="cron_db")

    @patch.dict(os.environ, {"DB_HOST": "db_host", "DB_PORT": "5432", "DB_USER": "db_user", "DB_PASSWORD": "db_pass"})
    @patch("psycopg.connect")
    def test_get_pg_cron_connection_with_fallback(self, mock_connect):
        """Test connection falls back to DB_* variables."""
        get_pg_cron_connection()

        mock_connect.assert_called_once_with(
            host="db_host",
            port=5432,
            user="db_user",
            password="db_pass",
            dbname="postgres",  # Default database
        )


class TestPgCronPermissions:
    """Test pg_cron permission checking and granting."""

    @patch.dict(os.environ, {"DB_USER": "test_user"})
    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    def test_check_and_grant_permissions_already_granted(self, mock_get_conn):
        """Test when user already has all required permissions."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # All permissions are already granted
        mock_cursor.fetchone.return_value = (True, True, True)

        success, message = check_and_grant_pg_cron_permissions()

        assert success is True
        assert "already granted" in message


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

        # Patch get_pg_cron_connection to return our mock connection
        with patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection", return_value=mock_conn):
            status = get_processor_status("partitioncache_queue")

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

        # Patch get_pg_cron_connection to return our mock connection
        with patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection", return_value=mock_conn):
            status = get_processor_status("partitioncache_queue")
            assert status is None


class TestSQLFile:
    """Test SQL file handling."""

    def test_sql_file_exists(self):
        """Test that the SQL file path is correctly constructed."""
        assert SQL_CRON_FILE.name == "postgresql_queue_processor_cron.sql"
        assert "queue_handler" in str(SQL_CRON_FILE)


class TestDatabaseOperations:
    """Test database operation functions."""

    @patch("partitioncache.cli.postgresql_queue_processor.SQL_CRON_FILE")
    @patch("partitioncache.queue_handler.get_queue_handler")
    @patch("partitioncache.cache_handler.get_cache_handler")
    @patch("partitioncache.cli.postgresql_queue_processor.get_queue_table_prefix_from_env")
    def test_setup_database_objects(self, mock_get_queue_prefix, mock_get_cache_handler, mock_get_queue_handler, mock_sql_file):
        """Test setting up database objects."""
        from partitioncache.cli.postgresql_queue_processor import setup_database_objects

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

        # Verify SQL execution - should be called at least four times:
        # 1. Main SQL file content
        # 2. Info SQL file content
        # 3. partitioncache_initialize_processor_tables function call
        # 4. partitioncache_create_config_trigger function call
        # (Additional calls may be made for cache table setup)
        assert mock_cursor.execute.call_count >= 4

        # Check that essential SQL calls were made (order may vary)
        all_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        # Check that main SQL content was executed
        assert "CREATE TABLE test_table;" in all_calls

        # Check that processor tables initialization was called (could be either variant)
        assert any("partitioncache_initialize_cron_config_table" in call for call in all_calls) or any(
            "partitioncache_initialize_cache_processor_tables" in call for call in all_calls
        )

        # Check that config trigger creation was called (could be either variant)
        assert any("partitioncache_create_cron_config_trigger" in call for call in all_calls) or any(
            "partitioncache_create_config_trigger" in call for call in all_calls
        )

        # Verify that commit was called (may be called multiple times)
        assert mock_conn.commit.call_count >= 1

    def test_insert_initial_config(self):
        """Test inserting initial configuration."""
        from partitioncache.cli.postgresql_queue_processor import insert_initial_config

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Test inserting configuration with dynamic job name
        insert_initial_config(mock_conn, "partitioncache_process_queue_test_database", "partitioncache", "partitioncache_queue", "array", 1, True, 1800, "test_database", None)

        # Should execute INSERT statement
        assert mock_cursor.execute.call_count == 1
        insert_call = mock_cursor.execute.call_args_list[0]
        assert "INSERT INTO" in str(insert_call[0][0])
        assert "partitioncache_queue_processor_config" in str(insert_call[0][0])

        # Verify that commit was called (may be called multiple times)
        assert mock_conn.commit.call_count >= 1


class TestCLIIntegration:
    """Test CLI integration points."""

    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name")
    @patch("partitioncache.cli.postgresql_queue_processor.get_queue_table_prefix_from_env")
    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    @patch("partitioncache.cli.postgresql_queue_processor.validate_environment")
    @patch("partitioncache.cli.postgresql_queue_processor.load_environment_with_validation")
    def test_main_status_command(self, mock_load_env, mock_validate, mock_get_conn, mock_get_queue_prefix, mock_get_cache_db):
        """Test the status command."""
        from partitioncache.cli.postgresql_queue_processor import main

        # Mock environment validation
        mock_validate.return_value = (True, "OK")

        # Mock environment functions
        mock_get_queue_prefix.return_value = "partitioncache_queue"
        mock_get_cache_db.return_value = "test_db"

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
        with patch("sys.argv", ["postgresql_queue_processor.py", "status"]):
            main()

        # Should execute status query with new signature including target_database
        mock_cursor.execute.assert_called_with(
            "SELECT * FROM partitioncache_get_processor_status(%s, %s)", ["partitioncache_queue", "test_db"]
        )

    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    @patch("partitioncache.cli.postgresql_queue_processor.validate_environment")
    def test_main_invalid_environment(self, mock_validate, mock_get_conn):
        """Test handling of invalid environment."""
        from partitioncache.cli.postgresql_queue_processor import main

        # Mock environment validation failure
        mock_validate.return_value = (False, "Invalid configuration")

        # Mock command line arguments
        with patch("sys.argv", ["postgresql_queue_processor.py", "status"]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        mock_get_conn.assert_not_called()


class TestProcessorEnableDisable:
    """Test enable/disable processor functionality."""

    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name")
    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_enable_processor_success(self, mock_get_conn, mock_get_cache_db):
        """Test successful processor enable."""
        from partitioncache.cli.postgresql_queue_processor import enable_processor

        # Setup mocks
        mock_get_cache_db.return_value = "test_db"
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_conn.commit = Mock()

        # Call the function
        with patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection", return_value=mock_conn):
            enable_processor("test_queue_prefix")

        # Verify the correct function was called with new signature including target_database
        mock_cursor.execute.assert_called_once_with(
            "SELECT partitioncache_set_processor_enabled_cron(true, %s, %s)",
            ["test_queue_prefix", "test_db"]  # test_db from mock_get_cache_db
        )
        mock_conn.commit.assert_called_once()

    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name")
    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_disable_processor_success(self, mock_get_conn, mock_get_cache_db):
        """Test successful processor disable."""
        from partitioncache.cli.postgresql_queue_processor import disable_processor

        # Setup mocks
        mock_get_cache_db.return_value = "test_db"
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_conn.commit = Mock()

        # Call the function
        with patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection", return_value=mock_conn):
            disable_processor("test_queue_prefix")

        # Verify the correct function was called with new signature including target_database
        mock_cursor.execute.assert_called_once_with(
            "SELECT partitioncache_set_processor_enabled_cron(false, %s, %s)",
            ["test_queue_prefix", "test_db"]  # test_db from mock_get_cache_db
        )
        mock_conn.commit.assert_called_once()

    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_enable_processor_database_error(self, mock_get_conn):
        """Test enable processor handles database errors."""
        from partitioncache.cli.postgresql_queue_processor import enable_processor

        # Setup mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Simulate database error
        mock_cursor.execute.side_effect = Exception("Function does not exist")

        # Call should raise the exception
        with patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection", return_value=mock_conn):
            with pytest.raises(Exception, match="Function does not exist"):
                enable_processor("test_queue_prefix")

    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_disable_processor_database_error(self, mock_get_conn):
        """Test disable processor handles database errors."""
        from partitioncache.cli.postgresql_queue_processor import disable_processor

        # Setup mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Simulate database error
        mock_cursor.execute.side_effect = Exception("Function does not exist")

        # Call should raise the exception
        with patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection", return_value=mock_conn):
            with pytest.raises(Exception, match="Function does not exist"):
                disable_processor("test_queue_prefix")

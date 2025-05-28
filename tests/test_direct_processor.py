"""Tests for PostgreSQL direct queue processor."""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from partitioncache.cli.setup_direct_processor import (
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
            "CACHE_BACKEND": "redis",  # Not supported
        }
        with patch.dict(os.environ, env, clear=True):
            valid, message = validate_environment()
            assert not valid
            assert "only supports postgresql_array or postgresql_bit" in message
    
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
        }
        with patch.dict(os.environ, env, clear=True):
            valid, message = validate_environment()
            assert valid
            assert "validated successfully" in message


class TestPgCronCheck:
    """Test pg_cron extension checking."""
    
    def test_check_pg_cron_installed_true(self):
        """Test when pg_cron is installed."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (1,)
        
        assert check_pg_cron_installed(mock_conn) is True
        
        # Should make two calls: CREATE EXTENSION and SELECT to check
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        assert "CREATE EXTENSION IF NOT EXISTS pg_cron" in calls[0][0][0]
        assert "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'" in calls[1][0][0]
    
    def test_check_pg_cron_installed_false(self):
        """Test when pg_cron is not installed."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = None
        
        assert check_pg_cron_installed(mock_conn) is False


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
            True,   # enabled
            5,      # max_parallel_jobs
            1,      # frequency_seconds
            2,      # active_jobs
            10,     # queue_length
            50,     # recent_successes
            3       # recent_failures
        )
        
        status = get_processor_status(mock_conn)
        
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
        
        status = get_processor_status(mock_conn)
        assert status is None


class TestSQLFile:
    """Test SQL file handling."""
    
    def test_sql_file_exists(self):
        """Test that the SQL file path is correctly constructed."""
        assert SQL_FILE.name == "postgresql_direct_processor.sql"
        assert "queue_handler" in str(SQL_FILE)
    
    @patch("pathlib.Path.read_text")
    def test_sql_file_content(self, mock_read_text):
        """Test reading SQL file content."""
        mock_read_text.return_value = "CREATE TABLE test;"
        
        content = SQL_FILE.read_text()
        assert content == "CREATE TABLE test;"
        mock_read_text.assert_called_once()


class TestDatabaseOperations:
    """Test database operation functions."""
    
    @patch("partitioncache.cli.setup_direct_processor.SQL_FILE")
    def test_setup_database_objects(self, mock_sql_file):
        """Test setting up database objects."""
        from partitioncache.cli.setup_direct_processor import setup_database_objects
        
        # Mock SQL file content
        mock_sql_file.read_text.return_value = "CREATE TABLE partitioncache_test;"
        
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        
        # Test with default prefix
        setup_database_objects(mock_conn, "partitioncache")
        mock_cursor.execute.assert_called_with("CREATE TABLE partitioncache_test;")
        mock_conn.commit.assert_called_once()
        
        # Reset mocks
        mock_cursor.reset_mock()
        mock_conn.reset_mock()
        
        # Test with custom prefix
        setup_database_objects(mock_conn, "myapp")
        # Should replace prefix in SQL
        mock_cursor.execute.assert_called_with("CREATE TABLE myapp_test;")
        mock_conn.commit.assert_called_once()
    
    def test_setup_pg_cron_job(self):
        """Test setting up pg_cron job."""
        from partitioncache.cli.setup_direct_processor import setup_pg_cron_job
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        
        # Test with 1 second frequency
        setup_pg_cron_job(mock_conn, "partitioncache", 1)
        
        # Should delete existing job and create new one
        assert mock_cursor.execute.call_count == 2
        delete_call = mock_cursor.execute.call_args_list[0]
        assert "DELETE FROM cron.job" in delete_call[0][0]
        
        insert_call = mock_cursor.execute.call_args_list[1]
        assert "INSERT INTO cron.job" in insert_call[0][0]
        assert "partitioncache_process_queue" in str(insert_call)
        
        mock_conn.commit.assert_called_once()


class TestCLIIntegration:
    """Test CLI integration points."""
    
    @patch("partitioncache.cli.setup_direct_processor.get_db_connection")
    @patch("partitioncache.cli.setup_direct_processor.validate_environment")
    @patch("partitioncache.cli.setup_direct_processor.dotenv.load_dotenv")
    def test_main_status_command(self, mock_load_env, mock_validate, mock_get_conn):
        """Test the status command."""
        from partitioncache.cli.setup_direct_processor import main
        
        # Mock environment validation
        mock_validate.return_value = (True, "OK")
        
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (True, 5, 1, 0, 0, 0, 0)
        
        # Mock command line arguments
        with patch("sys.argv", ["setup_direct_processor.py", "status"]):
            main()
        
        # Should execute status query
        mock_cursor.execute.assert_called_with("SELECT * FROM get_processor_status()")
        mock_conn.close.assert_called_once()
    
    @patch("partitioncache.cli.setup_direct_processor.get_db_connection")
    @patch("partitioncache.cli.setup_direct_processor.validate_environment")
    def test_main_invalid_environment(self, mock_validate, mock_get_conn):
        """Test handling of invalid environment."""
        from partitioncache.cli.setup_direct_processor import main
        
        # Mock environment validation failure
        mock_validate.return_value = (False, "Invalid configuration")
        
        # Mock command line arguments
        with patch("sys.argv", ["setup_direct_processor.py", "status"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
        
        assert exc_info.value.code == 1
        mock_get_conn.assert_not_called() 
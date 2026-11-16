"""Tests for PostgreSQL direct queue processor."""

import os
from unittest.mock import Mock, patch

import pytest

from partitioncache.cli.postgresql_queue_processor import (
    SQL_CRON_FILE,
    check_and_grant_pg_cron_permissions,
    check_pg_cron_installed,
    get_cache_backend_from_env,
    get_pg_cron_connection,
    get_processor_status,
    get_table_prefix_from_env,
    validate_environment,
)


class TestBackendEnvMapping:
    """Test backend/env mapping for direct processor."""

    @pytest.mark.parametrize(
        ("cache_backend", "expected_backend"),
        [
            ("postgresql_array", "array"),
            ("postgresql_bit", "bit"),
            ("postgresql_roaringbit", "roaringbit"),
        ],
    )
    def test_get_cache_backend_from_env_supported(self, cache_backend, expected_backend):
        with patch.dict(os.environ, {"CACHE_BACKEND": cache_backend}, clear=True):
            assert get_cache_backend_from_env() == expected_backend

    def test_postgis_bbox_is_refused_with_a_pointer_to_the_monitor(self):
        """The PL/pgSQL processor does not write the bbox cache format; setup must not install it for that backend."""
        from partitioncache.cli.postgresql_queue_processor import DIRECT_PROCESSOR_BACKENDS

        assert "postgis_bbox" not in DIRECT_PROCESSOR_BACKENDS
        with patch.dict(os.environ, {"CACHE_BACKEND": "postgis_bbox"}, clear=True):
            with pytest.raises(ValueError, match="does not support the postgis_bbox cache backend.*pcache-monitor"):
                get_cache_backend_from_env()

    @patch("partitioncache.cli.postgresql_queue_processor.handle_setup")
    @patch("partitioncache.cli.postgresql_queue_processor.load_environment_with_validation")
    def test_setup_refuses_postgis_bbox(self, mock_load_env, mock_handle_setup, caplog):
        from partitioncache.cli.postgresql_queue_processor import main

        env = {
            **{f"PG_QUEUE_{k}": v for k, v in {"HOST": "localhost", "PORT": "5432", "USER": "u", "PASSWORD": "p", "DB": "db"}.items()},
            **{f"DB_{k}": v for k, v in {"HOST": "localhost", "PORT": "5432", "USER": "u", "PASSWORD": "p", "NAME": "db"}.items()},
            "CACHE_BACKEND": "postgis_bbox",
            "PG_BBOX_CACHE_TABLE_PREFIX": "partitioncache_bbox",
        }
        with patch.dict(os.environ, env, clear=True), patch("sys.argv", ["postgresql_queue_processor.py", "setup"]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1
        mock_handle_setup.assert_not_called()
        assert "pcache-monitor" in caplog.text

    @pytest.mark.parametrize(
        ("cache_backend", "prefix_env", "prefix_value"),
        [
            ("postgresql_array", "PG_ARRAY_CACHE_TABLE_PREFIX", "array_cache"),
            ("postgresql_bit", "PG_BIT_CACHE_TABLE_PREFIX", "bit_cache"),
            ("postgresql_roaringbit", "PG_ROARINGBIT_CACHE_TABLE_PREFIX", "roaring_cache"),
            ("postgis_bbox", "PG_BBOX_CACHE_TABLE_PREFIX", "bbox_cache"),
        ],
    )
    def test_get_table_prefix_from_env_supported(self, cache_backend, prefix_env, prefix_value):
        with patch.dict(os.environ, {"CACHE_BACKEND": cache_backend, prefix_env: prefix_value}, clear=True):
            assert get_table_prefix_from_env() == prefix_value


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

    @pytest.mark.parametrize(
        ("cache_backend", "prefix_env", "prefix_value"),
        [
            ("postgresql_array", "PG_ARRAY_CACHE_TABLE_PREFIX", "partitioncache"),
            ("postgis_bbox", "PG_BBOX_CACHE_TABLE_PREFIX", "partitioncache_bbox"),
        ],
    )
    def test_validate_environment_success(self, cache_backend, prefix_env, prefix_value):
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
            "CACHE_BACKEND": cache_backend,
            prefix_env: prefix_value,
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


    @patch.dict(os.environ, {"DB_USER": "app user"})
    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    def test_missing_permissions_are_granted_with_the_role_as_identifier(self, mock_get_conn):
        """The grantee is an identifier, not a bound parameter: PostgreSQL rejects 'TO $1'."""
        from psycopg import sql

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (False, False, False)

        success, message = check_and_grant_pg_cron_permissions()

        role = sql.Identifier("app user")
        grants = [c.args for c in mock_cursor.execute.call_args_list[1:]]
        assert grants == [
            (sql.SQL("GRANT USAGE ON SCHEMA cron TO {}").format(role),),
            (sql.SQL("GRANT EXECUTE ON FUNCTION cron.schedule_in_database(text, text, text, text, text, boolean) TO {}").format(role),),
            (sql.SQL("GRANT EXECUTE ON FUNCTION cron.unschedule(bigint), cron.unschedule(text) TO {}").format(role),),
            (sql.SQL("GRANT EXECUTE ON FUNCTION cron.alter_job(bigint, text, text, text, text, boolean) TO {}").format(role),),
            (sql.SQL("GRANT SELECT ON cron.job TO {}").format(role),),
        ]
        mock_conn.commit.assert_called_once()
        assert success is True

    @patch.dict(os.environ, {"DB_USER": "app"})
    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    def test_failed_grant_reports_the_error(self, mock_get_conn):
        import psycopg

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (False, True, True)
        mock_cursor.execute.side_effect = [None, psycopg.errors.InsufficientPrivilege("permission denied for schema cron")]

        success, message = check_and_grant_pg_cron_permissions()

        assert success is False
        assert "permission denied for schema cron" in message
        assert "superuser" not in message


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

    def test_insert_initial_config_uses_upsert(self):
        """Test that re-running setup updates config instead of silently doing nothing."""
        from partitioncache.cli.postgresql_queue_processor import insert_initial_config

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Insert without job_owner
        insert_initial_config(mock_conn, "test_job", "prefix", "queue", "array", 5, False, 1800, "testdb", None)
        sql_text = str(mock_cursor.execute.call_args_list[0][0][0])
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE SET" in sql_text
        assert "DO NOTHING" not in sql_text

        # Insert with job_owner
        mock_cursor.reset_mock()
        insert_initial_config(mock_conn, "test_job", "prefix", "queue", "array", 5, False, 1800, "testdb", None, job_owner="testworker")
        sql_text = str(mock_cursor.execute.call_args_list[0][0][0])
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE SET" in sql_text
        assert "job_owner = EXCLUDED.job_owner" in sql_text


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


class TestDurationParsingAndVerifyCommand:
    """Test duration parsing and verify command wiring."""

    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_backend_from_env")
    @patch("partitioncache.cli.postgresql_queue_processor.get_queue_table_prefix_from_env")
    @patch("partitioncache.cli.postgresql_queue_processor.get_table_prefix_from_env")
    @patch("partitioncache.cli.postgresql_queue_processor.handle_setup")
    @patch("partitioncache.cli.postgresql_queue_processor.validate_environment")
    @patch("partitioncache.cli.postgresql_queue_processor.load_environment_with_validation")
    def test_main_setup_parses_duration_units(
        self,
        mock_load_env,
        mock_validate,
        mock_handle_setup,
        mock_get_table_prefix,
        mock_get_queue_prefix,
        mock_get_cache_backend,
    ):
        from partitioncache.cli.postgresql_queue_processor import main

        mock_validate.return_value = (True, "OK")
        mock_get_table_prefix.return_value = "partitioncache"
        mock_get_queue_prefix.return_value = "partitioncache_queue"
        mock_get_cache_backend.return_value = "array"

        with patch("sys.argv", ["postgresql_queue_processor.py", "setup", "--frequency", "5m", "--timeout", "2h"]):
            main()

        mock_handle_setup.assert_called_once_with("partitioncache", "partitioncache_queue", "array", 300, False, 7200, None, job_owner=None, create_role=False)

    @patch("partitioncache.cli.postgresql_queue_processor.get_queue_table_prefix_from_env")
    @patch("partitioncache.cli.postgresql_queue_processor.get_table_prefix_from_env")
    @patch("partitioncache.cli.postgresql_queue_processor.verify_processor_setup")
    @patch("partitioncache.cli.postgresql_queue_processor.validate_environment")
    @patch("partitioncache.cli.postgresql_queue_processor.load_environment_with_validation")
    def test_main_verify_uses_return_code(
        self,
        mock_load_env,
        mock_validate,
        mock_verify,
        mock_get_table_prefix,
        mock_get_queue_prefix,
    ):
        from partitioncache.cli.postgresql_queue_processor import main

        mock_validate.return_value = (True, "OK")
        mock_verify.return_value = 0
        mock_get_table_prefix.return_value = "partitioncache"
        mock_get_queue_prefix.return_value = "partitioncache_queue"

        with patch("sys.argv", ["postgresql_queue_processor.py", "verify"]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 0
        mock_verify.assert_called_once_with("partitioncache", "partitioncache_queue")


class TestScopedRemove:
    """Verify remove_all_processor_objects uses scoped function lists, not blanket LIKE."""

    @patch("partitioncache.cli.postgresql_queue_processor.release_legacy_role_statement_timeout")
    @patch("partitioncache.cli.postgresql_queue_processor.processor_job_owners", return_value={})
    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_queue_processor.get_cron_database_name", return_value="testdb")
    def test_scoped_function_drop_uses_any_not_like(self, mock_cron_db, mock_cache_db, mock_owners, mock_release):
        from partitioncache.cli.postgresql_queue_processor import remove_all_processor_objects

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        remove_all_processor_objects(mock_conn, "partitioncache_queue")

        # Collect all SQL executed
        all_sql = ""
        for c in mock_cursor.execute.call_args_list:
            all_sql += str(c[0][0]) + "\n"

        # Must use ANY(...) for scoped drops
        assert "ANY(processor_funcs)" in all_sql
        # Must NOT use LIKE pattern for function discovery
        assert "LIKE 'partitioncache_%'" not in all_sql or "jobname LIKE" in all_sql  # jobname LIKE is OK for cron.job cleanup

    @patch("partitioncache.cli.postgresql_queue_processor.release_legacy_role_statement_timeout")
    @patch("partitioncache.cli.postgresql_queue_processor.processor_job_owners", return_value={})
    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_queue_processor.get_cron_database_name", return_value="testdb")
    def test_does_not_drop_eviction_functions(self, mock_cron_db, mock_cache_db, mock_owners, mock_release):
        """Processor remove must not include eviction-specific function names."""
        from partitioncache.cli.postgresql_queue_processor import remove_all_processor_objects

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        remove_all_processor_objects(mock_conn, "partitioncache_queue")

        all_sql = ""
        for c in mock_cursor.execute.call_args_list:
            all_sql += str(c[0][0]) + "\n"

        # These are eviction-specific functions that must NOT appear
        eviction_only = [
            "partitioncache_construct_eviction_job_name",
            "partitioncache_sync_eviction_cron_job",
            "partitioncache_run_eviction_job_with_params",
            "_partitioncache_evict_oldest_from_partition",
            "_partitioncache_evict_largest_from_partition",
        ]
        for fn in eviction_only:
            assert fn not in all_sql, f"Processor remove must not drop eviction function {fn}"


class TestLiveMetrics:
    """Test _fetch_processor_live_metrics with mocked connection."""

    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_returns_defaults_when_tables_missing(self, mock_get_conn):
        from partitioncache.cli.postgresql_queue_processor import _fetch_processor_live_metrics

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_get_conn.return_value = mock_conn

        # All queries raise UndefinedTable
        import psycopg.errors
        mock_cursor.execute.side_effect = psycopg.errors.UndefinedTable("relation does not exist")

        metrics = _fetch_processor_live_metrics("partitioncache_queue")

        assert metrics["queue_length"] == 0
        assert metrics["active_jobs_count"] == 0
        assert metrics["recent_successes_5m"] == 0
        assert metrics["recent_failures_5m"] == 0

    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_returns_queue_length(self, mock_get_conn):
        from partitioncache.cli.postgresql_queue_processor import _fetch_processor_live_metrics

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_get_conn.return_value = mock_conn

        # Queue count returns 42, then log queries raise UndefinedTable
        import psycopg.errors
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return None  # queue count query
            raise psycopg.errors.UndefinedTable("relation does not exist")

        mock_cursor.execute.side_effect = side_effect
        mock_cursor.fetchone.return_value = (42,)

        metrics = _fetch_processor_live_metrics("partitioncache_queue")

        assert metrics["queue_length"] == 42


def _mock_conn():
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=None)
    return conn, cursor


class TestLegacyRoleStatementTimeout:
    """Earlier versions set statement_timeout on the job owner role; setup and remove take it back without touching other values."""

    def test_job_owners_with_the_values_earlier_versions_set(self):
        from partitioncache.cli.postgresql_queue_processor import processor_job_owners

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = ("app", True, True)
        cursor.fetchall.return_value = [("cron_owner", 1800), ("app", 30)]

        assert processor_job_owners(conn, "partitioncache_queue") == {"app": {"30000ms"}, "cron_owner": {"1800000ms"}}

    def test_job_owners_without_configuration_table_is_the_current_user(self):
        from partitioncache.cli.postgresql_queue_processor import processor_job_owners

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = ("app", False, False)

        assert processor_job_owners(conn, "partitioncache_queue") == {"app": set()}
        cursor.fetchall.assert_not_called()

    def test_value_set_by_an_earlier_version_is_reset(self):
        from psycopg import sql

        from partitioncache.cli.postgresql_queue_processor import release_legacy_role_statement_timeout

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = ("statement_timeout=1800000ms",)

        release_legacy_role_statement_timeout(conn, {"app": {"1800000ms"}})

        assert cursor.execute.call_args_list[-1][0][0] == sql.SQL("ALTER ROLE {} RESET statement_timeout").format(sql.Identifier("app"))
        conn.commit.assert_called_once()

    def test_other_value_is_kept_with_a_warning(self, caplog):
        from partitioncache.cli.postgresql_queue_processor import release_legacy_role_statement_timeout

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = ("statement_timeout=5min",)

        with caplog.at_level("WARNING", logger="PartitionCache.PostgreSQLQueueProcessor"):
            release_legacy_role_statement_timeout(conn, {"app": {"1800000ms"}})

        assert cursor.execute.call_count == 1  # only the lookup
        conn.commit.assert_not_called()
        assert "Role app has statement_timeout = 5min" in caplog.text
        assert 'ALTER ROLE "app" RESET statement_timeout;' in caplog.text

    def test_role_without_setting_is_left_alone(self, caplog):
        from partitioncache.cli.postgresql_queue_processor import release_legacy_role_statement_timeout

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = None

        with caplog.at_level("WARNING", logger="PartitionCache.PostgreSQLQueueProcessor"):
            release_legacy_role_statement_timeout(conn, {"app": {"1800000ms"}})

        assert cursor.execute.call_count == 1
        assert caplog.text == ""

    def test_reset_without_privilege_warns_with_the_command(self, caplog):
        import psycopg

        from partitioncache.cli.postgresql_queue_processor import release_legacy_role_statement_timeout

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = ("statement_timeout=1800000ms",)
        cursor.execute.side_effect = [None, psycopg.errors.InsufficientPrivilege("permission denied to alter role")]

        with caplog.at_level("WARNING", logger="PartitionCache.PostgreSQLQueueProcessor"):
            release_legacy_role_statement_timeout(conn, {"cron owner": {"1800000ms"}})

        conn.rollback.assert_called_once()
        assert "permission denied to alter role" in caplog.text
        assert 'ALTER ROLE "cron owner" RESET statement_timeout;' in caplog.text

    @patch("partitioncache.cli.postgresql_queue_processor.release_legacy_role_statement_timeout")
    @patch("partitioncache.cli.postgresql_queue_processor.processor_job_owners", return_value={"app": {"1800000ms"}})
    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_queue_processor.get_cron_database_name", return_value="testdb")
    def test_remove_releases_the_role_setting_before_dropping_the_configuration(self, mock_cron_db, mock_cache_db, mock_owners, mock_release):
        from partitioncache.cli.postgresql_queue_processor import remove_all_processor_objects

        conn, cursor = _mock_conn()
        dropped_before_release = []
        mock_release.side_effect = lambda *args: dropped_before_release.append(cursor.execute.call_count)

        remove_all_processor_objects(conn, "partitioncache_queue")

        mock_owners.assert_called_once_with(conn, "partitioncache_queue")
        mock_release.assert_called_once_with(conn, {"app": {"1800000ms"}})
        assert dropped_before_release == [0]


class TestManualProcessPrefixes:
    """manual-process resolves table and queue prefix like the other subcommands and passes both to the SQL function."""

    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name", return_value="appdb")
    def test_job_name_and_queue_prefix_are_passed(self, mock_cache_db):
        from partitioncache.cli.postgresql_queue_processor import manual_process_queue

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = (2, "Attempted to process 2 items")

        manual_process_queue(conn, 5, "orders_cache", "orders_queue")

        cursor.execute.assert_called_once_with(
            "SELECT * FROM partitioncache_manual_process_queue(%s, %s, %s)", (5, "partitioncache_process_queue_appdb_orderscache", "orders_queue")
        )
        conn.commit.assert_called_once()

    @pytest.mark.parametrize(("argv", "table_prefix"), [(["--table-prefix", "orders_cache"], "orders_cache"), ([], "env_cache")])
    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    @patch("partitioncache.cli.postgresql_queue_processor.get_queue_table_prefix_from_env", return_value="orders_queue")
    @patch("partitioncache.cli.postgresql_queue_processor.get_table_prefix_from_env", return_value="env_cache")
    @patch("partitioncache.cli.postgresql_queue_processor.manual_process_queue")
    @patch("partitioncache.cli.postgresql_queue_processor.validate_environment", return_value=(True, "OK"))
    @patch("partitioncache.cli.postgresql_queue_processor.load_environment_with_validation")
    def test_cli_passes_the_prefixes(self, mock_load_env, mock_validate, mock_manual, mock_table_prefix, mock_queue_prefix, mock_get_conn, argv, table_prefix):
        from partitioncache.cli.postgresql_queue_processor import main

        with patch("sys.argv", ["postgresql_queue_processor.py", "manual-process", "5", *argv]):
            main()

        mock_manual.assert_called_once_with(mock_get_conn.return_value, 5, table_prefix, "orders_queue")


class TestSetupRoleTimeout:
    """setup reads the job owners before it replaces the configuration row, and releases their legacy role timeout after."""

    @patch("partitioncache.cli.postgresql_queue_processor.warn_if_pg_cron_background_workers")
    @patch("partitioncache.cli.postgresql_queue_processor.check_pg_cron_installed", return_value=True)
    @patch("partitioncache.cli.postgresql_queue_processor.get_cron_database_name", return_value="appdb")
    @patch("partitioncache.cli.postgresql_queue_processor.get_cache_database_name", return_value="appdb")
    @patch("partitioncache.cli.postgresql_queue_processor.get_pg_cron_connection")
    @patch("partitioncache.cli.postgresql_queue_processor.get_db_connection")
    def test_setup_order(self, mock_db_conn, mock_cron_conn, mock_cache_db, mock_cron_db, mock_pg_cron, mock_warn_bgw):
        from partitioncache.cli import postgresql_queue_processor as module

        order = Mock()
        with (
            patch.object(module, "setup_database_objects", order.setup_database_objects),
            patch.object(module, "processor_job_owners", order.processor_job_owners),
            patch.object(module, "insert_initial_config", order.insert_initial_config),
            patch.object(module, "release_legacy_role_statement_timeout", order.release),
        ):
            order.processor_job_owners.return_value = {"app": {"1800000ms"}}
            module.handle_setup("orders_cache", "orders_queue", "array", 1, False, 600, None, job_owner="cron_owner")

        names = [c[0] for c in order.mock_calls if c[0] in ("setup_database_objects", "processor_job_owners", "insert_initial_config", "release")]
        assert names == ["setup_database_objects", "processor_job_owners", "insert_initial_config", "release"]
        order.release.assert_called_once_with(mock_db_conn.return_value, {"app": {"1800000ms"}, "cron_owner": set()})
        mock_warn_bgw.assert_called_once_with(mock_db_conn.return_value)

    @pytest.mark.parametrize(("setting", "warned"), [("on", True), ("off", False), (None, False)])
    def test_background_worker_warning(self, setting, warned, caplog):
        from partitioncache.cli.postgresql_queue_processor import warn_if_pg_cron_background_workers

        conn, cursor = _mock_conn()
        cursor.fetchone.return_value = (setting,)
        with caplog.at_level("WARNING", logger="PartitionCache.PostgreSQLQueueProcessor"):
            warn_if_pg_cron_background_workers(conn)

        assert ("cron.use_background_workers = on" in caplog.text) is warned

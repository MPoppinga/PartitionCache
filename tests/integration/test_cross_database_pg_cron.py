"""Integration tests for cross-database pg_cron setup."""

import os
from unittest.mock import patch

import pytest

from partitioncache.cli.setup_postgresql_queue_processor import (
    check_and_grant_pg_cron_permissions,
    check_pg_cron_installed,
    get_pg_cron_connection,
)


class TestCrossDatabasePgCron:
    """Test cross-database pg_cron functionality."""

    @pytest.mark.integration
    def test_cross_database_connection_with_env_vars(self):
        """Test that PG_CRON_* environment variables are used correctly."""
        # This test verifies environment variable usage without actual connection
        with patch.dict(os.environ, {
            "PG_CRON_HOST": "pgcron.example.com",
            "PG_CRON_PORT": "5433",
            "PG_CRON_USER": "cron_admin",
            "PG_CRON_PASSWORD": "cron_secret",
            "PG_CRON_DATABASE": "cron_management",
            "DB_HOST": "app.example.com",
            "DB_PORT": "5432",
            "DB_USER": "app_user",
            "DB_PASSWORD": "app_secret",
            "DB_NAME": "app_db"
        }):
            # Mock the connection to avoid actual database connection
            with patch('psycopg.connect') as mock_connect:
                get_pg_cron_connection()

                # Verify it uses PG_CRON_* variables, not DB_* variables
                mock_connect.assert_called_once_with(
                    host="pgcron.example.com",
                    port=5433,
                    user="cron_admin",
                    password="cron_secret",
                    dbname="cron_management"
                )

    @pytest.mark.integration
    @pytest.mark.slow
    def test_cross_database_pg_cron_setup(self, db_session, postgresql_queue_processor):
        """Test setting up pg_cron in a cross-database configuration.

        This test simulates a cross-database setup where pg_cron is in 'postgres'
        database while the work database is different.
        """
        from partitioncache.queue import get_queue_provider_name

        # Only run for PostgreSQL queue provider
        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL with pg_cron")

        # Check if pg_cron is available (may be in different database)
        pg_cron_available = check_pg_cron_installed()
        if not pg_cron_available:
            pytest.skip("pg_cron extension not available for cross-database testing")

        # Test permission checking (won't actually grant in test environment)
        success, message = check_and_grant_pg_cron_permissions()

        # The function should at least run without errors
        assert isinstance(success, bool)
        assert isinstance(message, str)

        # Get current database name (work database)
        with db_session.cursor() as cur:
            cur.execute("SELECT current_database()")
            work_db = cur.fetchone()[0]

        # Get pg_cron database name from environment
        pg_cron_db = os.getenv("PG_CRON_DATABASE", "postgres")

        # Log the setup for debugging
        print(f"Work database: {work_db}")
        print(f"pg_cron database: {pg_cron_db}")
        print(f"Permission check result: {success} - {message}")

        # If they're different, we have a cross-database setup
        if work_db != pg_cron_db:
            print("Cross-database pg_cron configuration detected")

            # Verify we can still interact with work database tables
            queue_prefix = postgresql_queue_processor["queue_prefix"]

            with db_session.cursor() as cur:
                # Check that processor config table exists in work database
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """, (f"{queue_prefix}_processor_config",))

                config_exists = cur.fetchone()[0]
                assert config_exists, "Processor config table should exist in work database"

    @pytest.mark.integration
    def test_cross_database_trigger_function(self, db_session, postgresql_queue_processor):
        """Test that the trigger function correctly handles cross-database job scheduling."""
        from partitioncache.queue import get_queue_provider_name

        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL")

        # Check if the trigger uses the new cross-database aware functions
        with db_session.cursor() as cur:
            # Get the trigger function definition
            cur.execute("""
                SELECT prosrc
                FROM pg_proc
                WHERE proname = 'partitioncache_sync_cron_job'
            """)

            result = cur.fetchone()
            if result:
                function_src = result[0]

                # Verify the function uses schedule_in_database
                assert "schedule_in_database" in function_src, \
                    "Trigger should use cron.schedule_in_database for cross-database support"

                # Verify it stores job_id
                assert "job_id" in function_src, \
                    "Trigger should store job_id for cross-database job management"

                # Verify it gets current database
                assert "current_database()" in function_src, \
                    "Trigger should determine current database for cross-database scheduling"

    @pytest.mark.integration
    def test_fallback_to_db_vars(self):
        """Test that connection falls back to DB_* vars when PG_CRON_* vars are not set."""
        # Clear any PG_CRON_* variables
        env_backup = {}
        for key in ["PG_CRON_HOST", "PG_CRON_PORT", "PG_CRON_USER", "PG_CRON_PASSWORD", "PG_CRON_DATABASE"]:
            if key in os.environ:
                env_backup[key] = os.environ.pop(key)

        try:
            with patch.dict(os.environ, {
                "DB_HOST": "fallback.example.com",
                "DB_PORT": "5432",
                "DB_USER": "fallback_user",
                "DB_PASSWORD": "fallback_pass"
            }):
                with patch('psycopg.connect') as mock_connect:
                    get_pg_cron_connection()

                    # Should use DB_* variables as fallback
                    mock_connect.assert_called_once_with(
                        host="fallback.example.com",
                        port=5432,
                        user="fallback_user",
                        password="fallback_pass",
                        dbname="postgres"  # Default pg_cron database
                    )
        finally:
            # Restore environment
            os.environ.update(env_backup)

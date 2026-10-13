from unittest.mock import Mock, patch

from partitioncache.cli.postgresql_cache_eviction import (
    _parse_eviction_frequency_minutes_arg,
    remove_all_eviction_objects,
)


def test_parse_eviction_frequency_minutes_back_compat_and_units():
    assert _parse_eviction_frequency_minutes_arg("30") == 30
    assert _parse_eviction_frequency_minutes_arg("5m") == 5
    assert _parse_eviction_frequency_minutes_arg("2h") == 120
    assert _parse_eviction_frequency_minutes_arg("45s") == 1


class TestEvictionCliParsingAndVerify:
    @patch("partitioncache.cli.postgresql_cache_eviction.get_db_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_table_prefix")
    @patch("partitioncache.cli.postgresql_cache_eviction.handle_setup")
    @patch("partitioncache.cli.postgresql_cache_eviction.validate_environment")
    @patch("partitioncache.cli.postgresql_cache_eviction.load_environment_with_validation")
    def test_main_setup_parses_duration_units(
        self,
        mock_load_env,
        mock_validate,
        mock_handle_setup,
        mock_get_table_prefix,
        mock_get_conn,
    ):
        from partitioncache.cli.postgresql_cache_eviction import main

        mock_validate.return_value = (True, "OK")
        mock_get_table_prefix.return_value = "partitioncache"
        mock_get_conn.return_value = Mock()

        with patch("sys.argv", ["postgresql_cache_eviction.py", "setup", "--frequency", "2h"]):
            main()

        mock_handle_setup.assert_called_once_with(
            "partitioncache", 120, False, "oldest", 1000,
            job_owner=None, create_role=False,
        )

    @patch("partitioncache.cli.postgresql_cache_eviction.get_db_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_table_prefix")
    @patch("partitioncache.cli.postgresql_cache_eviction.verify_eviction_setup")
    @patch("partitioncache.cli.postgresql_cache_eviction.validate_environment")
    @patch("partitioncache.cli.postgresql_cache_eviction.load_environment_with_validation")
    def test_main_verify_uses_return_code(
        self,
        mock_load_env,
        mock_validate,
        mock_verify,
        mock_get_table_prefix,
        mock_get_conn,
    ):
        from partitioncache.cli.postgresql_cache_eviction import main

        mock_validate.return_value = (True, "OK")
        mock_verify.return_value = 0
        mock_get_table_prefix.return_value = "partitioncache"
        mock_get_conn.return_value = Mock()

        with patch("sys.argv", ["postgresql_cache_eviction.py", "verify"]):
            main()

        mock_verify.assert_called_once_with("partitioncache")


class TestEvictionScopedRemove:
    """Verify remove_all_eviction_objects uses scoped function lists, not blanket LIKE."""

    @patch("partitioncache.cli.postgresql_cache_eviction.get_db_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_pg_cron_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_cache_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_cron_database_name", return_value="postgres")
    def test_scoped_function_drop_uses_any_not_like(
        self, mock_cron_db, mock_cache_db, mock_cron_conn_fn, mock_cache_conn_fn,
    ):
        """The DO block must use = ANY(...) not LIKE 'partitioncache_%'."""
        mock_cron_conn = Mock()
        mock_cron_cursor = Mock()
        mock_cron_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cron_cursor)
        mock_cron_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cron_conn_fn.return_value = mock_cron_conn

        mock_cache_conn = Mock()
        mock_cache_cursor = Mock()
        mock_cache_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cache_cursor)
        mock_cache_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cache_conn_fn.return_value = mock_cache_conn

        remove_all_eviction_objects("partitioncache")

        # Collect all SQL executed on both connections
        all_sql = ""
        for c in mock_cron_cursor.execute.call_args_list:
            arg = c[0][0]
            all_sql += str(arg) + "\n"
        for c in mock_cache_cursor.execute.call_args_list:
            arg = c[0][0]
            all_sql += str(arg) + "\n"

        # Must use ANY(...) for scoped drops
        assert "ANY(eviction_cron_funcs)" in all_sql
        assert "ANY(eviction_cache_funcs)" in all_sql
        # Must NOT use LIKE pattern
        assert "LIKE 'partitioncache_%'" not in all_sql

    @patch("partitioncache.cli.postgresql_cache_eviction.get_db_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_pg_cron_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_cache_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_cron_database_name", return_value="postgres")
    def test_does_not_drop_processor_functions(
        self, mock_cron_db, mock_cache_db, mock_cron_conn_fn, mock_cache_conn_fn,
    ):
        """Eviction remove must not include queue-processor function names."""
        mock_cron_conn = Mock()
        mock_cron_cursor = Mock()
        mock_cron_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cron_cursor)
        mock_cron_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cron_conn_fn.return_value = mock_cron_conn

        mock_cache_conn = Mock()
        mock_cache_cursor = Mock()
        mock_cache_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cache_cursor)
        mock_cache_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cache_conn_fn.return_value = mock_cache_conn

        remove_all_eviction_objects("partitioncache")

        all_sql = ""
        for c in mock_cron_cursor.execute.call_args_list + mock_cache_cursor.execute.call_args_list:
            all_sql += str(c[0][0]) + "\n"

        # These are processor-specific functions that must NOT appear
        processor_only = [
            "partitioncache_sync_cron_job",
            "partitioncache_run_single_job_with_params",
            "_partitioncache_execute_job",
            "partitioncache_manual_process_queue",
        ]
        for fn in processor_only:
            assert fn not in all_sql, f"Eviction remove must not drop processor function {fn}"


class TestEvictionLogCleanup:
    """Verify handle_setup schedules eviction log cleanup."""

    @patch("partitioncache.cli.postgresql_cache_eviction.get_db_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_pg_cron_connection")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_cache_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_cache_eviction.get_cron_database_name", return_value="testdb")
    @patch("partitioncache.cli.postgresql_cache_eviction.check_pg_cron_installed", return_value=True)
    @patch("partitioncache.cli.postgresql_cache_eviction.setup_database_objects")
    @patch("partitioncache.cli.postgresql_cache_eviction.insert_initial_config")
    def test_handle_setup_schedules_log_cleanup(
        self, mock_insert, mock_setup_db, mock_pg_cron, mock_cron_db, mock_cache_db, mock_cron_conn_fn, mock_cache_conn_fn,
    ):
        from partitioncache.cli.postgresql_cache_eviction import handle_setup

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cache_conn_fn.return_value = mock_conn
        mock_cron_conn_fn.return_value = mock_conn

        handle_setup("partitioncache", 60, False, "oldest", 1000)

        # Verify that log cleanup scheduling was attempted
        log_cleanup_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "partitioncache_schedule_eviction_log_cleanup" in str(c)
        ]
        assert len(log_cleanup_calls) == 1, "handle_setup should schedule eviction log cleanup"


class TestEvictionConfigUpsert:
    """Test that insert_initial_config uses ON CONFLICT DO UPDATE."""

    def test_insert_initial_config_uses_upsert(self):
        from partitioncache.cli.postgresql_cache_eviction import insert_initial_config

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        insert_initial_config(mock_conn, "test_evict", "prefix", 5, True, "oldest", 100, "testdb")
        sql_text = str(mock_cursor.execute.call_args_list[0][0][0])
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE SET" in sql_text
        assert "DO NOTHING" not in sql_text

    def test_insert_initial_config_with_job_owner_uses_upsert(self):
        from partitioncache.cli.postgresql_cache_eviction import insert_initial_config

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        insert_initial_config(mock_conn, "test_evict", "prefix", 5, True, "oldest", 100, "testdb", job_owner="worker")
        sql_text = str(mock_cursor.execute.call_args_list[0][0][0])
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE SET" in sql_text
        assert "job_owner = EXCLUDED.job_owner" in sql_text

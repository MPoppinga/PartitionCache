"""
Unit tests for DuckDB query accelerator.

This test suite validates the DuckDB query acceleration functionality
with mocked dependencies to avoid requiring actual database connections.
"""

from unittest.mock import Mock, patch

import pytest


@pytest.fixture(autouse=True)
def _no_timezone_query(request, monkeypatch):
    """The mocked PostgreSQL connections cannot answer SHOW TimeZone; it has its own test below."""
    if request.node.name != "test_duckdb_uses_the_postgresql_session_time_zone":
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        monkeypatch.setattr(DuckDBQueryAccelerator, "_use_postgresql_timezone", lambda self: None)


class TestDuckDBQueryAccelerator:
    """Test suite for DuckDB query accelerator implementation."""

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_duckdb_import_error_handling(self, mock_psycopg, mock_duckdb):
        """Test graceful handling when DuckDB is not available."""
        # Mock DuckDB to raise ImportError when connect is called
        mock_duckdb.connect.side_effect = ImportError("No module named 'duckdb'")

        from partitioncache.query_accelerator import create_query_accelerator

        # Should return None when initialization fails
        result = create_query_accelerator({"host": "localhost"})
        assert result is None

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_accelerator_initialization(self, mock_psycopg, mock_duckdb):
        """Test accelerator initialization with mocked dependencies."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock DuckDB connection
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn

        # Mock PostgreSQL connection
        mock_pg_conn = Mock()
        mock_psycopg.connect.return_value = mock_pg_conn

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=["test_table"],
            duckdb_memory_limit="1GB",
            duckdb_threads=2,
            duckdb_database_path=":memory:",
        )

        # Test initialization
        result = accelerator.initialize()
        assert result is True
        assert accelerator._initialized is True
        assert accelerator.duckdb_conn is not None
        assert accelerator.postgresql_conn is not None

        # Verify connections were created
        mock_duckdb.connect.assert_called_once_with(":memory:")
        mock_psycopg.connect.assert_called_once_with(**postgresql_params, autocommit=True)

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_accelerator_configuration(self, mock_psycopg, mock_duckdb):
        """Test accelerator configuration settings."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock connections
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        mock_psycopg.connect.return_value = Mock()

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=["table1", "table2"],
            duckdb_memory_limit="2GB",
            duckdb_threads=8,
            enable_statistics=True,
        )

        accelerator.initialize()

        # Verify configuration
        assert accelerator.tables_to_preload == ["table1", "table2"]
        assert accelerator.duckdb_memory_limit == "2GB"
        assert accelerator.duckdb_threads == 8
        assert accelerator.enable_statistics is True

        # Verify DuckDB configuration was applied
        execute_calls = mock_duckdb_conn.execute.call_args_list
        config_calls = [call for call in execute_calls if "SET" in str(call)]
        assert len(config_calls) == 3  # Exactly: memory_limit, threads and integer_division

        # Verify the specific SET calls
        config_sql = [str(call) for call in config_calls]
        assert any("SET memory_limit = '2GB'" in sql for sql in config_sql), f"Expected memory_limit SET call, got: {config_sql}"
        assert any("SET threads = 8" in sql for sql in config_sql), f"Expected threads SET call, got: {config_sql}"

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_table_preloading(self, mock_psycopg, mock_duckdb):
        """Test table preloading functionality."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock connections
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        mock_pg_conn = Mock()
        mock_psycopg.connect.return_value = mock_pg_conn

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=["test_table"],
            enable_statistics=True,
            duckdb_database_path=":memory:",  # Use memory database to ensure fresh state
        )

        accelerator.initialize()

        # Mock table existence checks
        accelerator._resolve_postgresql_relation = Mock(return_value=("public", "test_table"))
        accelerator._tables_exist_in_duckdb = Mock(return_value=False)  # Force reload from PostgreSQL

        # Set up mock for the database check and table creation
        # The execute method needs to return different results for different queries
        execute_results = []
        def mock_execute(query, *args, **kwargs):
            result = Mock()
            if "duckdb_databases()" in query:
                # Database check query - return None to indicate not attached
                result.fetchone.return_value = None
            elif "CREATE" in query and "TABLE" in query:
                # Table creation succeeds
                result.fetchone.return_value = None
            elif "SELECT COUNT" in query:
                # Count query returns a row count
                result.fetchone.return_value = (100,)
            else:
                # Default for other queries
                result.fetchone.return_value = None
            execute_results.append(query)
            return result

        mock_duckdb_conn.execute.side_effect = mock_execute

        # Test table preloading
        result = accelerator.preload_tables()
        assert result is True
        assert accelerator._preload_completed is True

        # Verify PostgreSQL extension was installed and table was created
        install_calls = [query for query in execute_results if "INSTALL" in query]
        assert len(install_calls) == 1  # Exactly one INSTALL call for postgres extension
        assert "INSTALL postgres" in install_calls[0], f"Expected 'INSTALL postgres', got: {install_calls[0]}"

        create_calls = [query for query in execute_results if "CREATE" in query and "TABLE" in query]
        assert len(create_calls) == 1  # Exactly one CREATE TABLE for 'test_table'
        assert "test_table" in create_calls[0], f"Expected CREATE TABLE for 'test_table', got: {create_calls[0]}"

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_query_execution_success(self, mock_psycopg, mock_duckdb):
        """Test successful query execution with acceleration."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock DuckDB connection and results
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        # Fix: Set up both direct execute and cursor().execute paths
        mock_result = Mock()
        mock_result.fetchall.return_value = [(1,), (2,), (3,)]
        # Direct execute path
        mock_duckdb_conn.execute.return_value = mock_result
        # Cursor execute path
        mock_cursor = Mock()
        mock_cursor.execute.return_value = mock_result
        mock_cursor.extract_statements.return_value = [Mock()]  # DuckDB's parser finds one statement
        mock_cursor.close.return_value = None
        mock_duckdb_conn.cursor.return_value = mock_cursor

        # Mock PostgreSQL connection with context manager support for cursor
        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_conn.cursor.return_value.__enter__ = Mock(return_value=mock_pg_cursor)
        mock_pg_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_psycopg.connect.return_value = mock_pg_conn

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params, enable_statistics=True)

        accelerator.initialize()

        # Test query execution
        result = accelerator.execute_query("SELECT partition_key FROM test_table")

        assert result == {1, 2, 3}

        # Verify statistics were updated
        stats = accelerator.get_statistics()
        assert stats["queries_accelerated"] == 1
        assert stats["queries_fallback"] == 0
        assert stats["total_queries"] == 1

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_query_execution_fallback(self, mock_psycopg, mock_duckdb):
        """Test fallback to PostgreSQL when DuckDB query fails."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock DuckDB connection - allow initialization but fail during query execution
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        # Let initialization succeed, but make query execution fail
        mock_duckdb_conn.execute.side_effect = [
            None,  # First call for "SET memory_limit = '2GB'"
            None,  # Second call for "SET threads = 4"
            None,  # "SET GLOBAL integer_division = true"
            None,  # Third call for "INSTALL postgres"
            None,  # Fourth call for "LOAD postgres"
            Exception("DuckDB query failed"),  # Fifth call (actual query) fails
        ]

        # Mock PostgreSQL connection with successful result
        mock_pg_conn = Mock()
        mock_psycopg.connect.return_value = mock_pg_conn
        mock_pg_cursor = Mock()
        # Properly mock the context manager protocol
        mock_cursor_context = Mock()
        mock_cursor_context.__enter__ = Mock(return_value=mock_pg_cursor)
        mock_cursor_context.__exit__ = Mock(return_value=None)
        mock_pg_conn.cursor.return_value = mock_cursor_context
        mock_pg_cursor.fetchall.return_value = [(4,), (5,), (6,)]
        mock_pg_cursor.nextset.return_value = None  # one result set, as a single SELECT gives

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params, enable_statistics=True)

        accelerator.initialize()

        # Test query execution with fallback
        result = accelerator.execute_query("SELECT partition_key FROM test_table")

        assert result == {4, 5, 6}

        # Verify statistics show fallback usage
        stats = accelerator.get_statistics()
        assert stats["queries_accelerated"] == 0
        assert stats["queries_fallback"] == 1
        assert stats["total_queries"] == 1

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_statistics_tracking(self, mock_psycopg, mock_duckdb):
        """Test comprehensive statistics tracking."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock connections
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        mock_psycopg.connect.return_value = Mock()

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params, preload_tables=["table1", "table2"], enable_statistics=True)

        accelerator.initialize()

        # Mock the preload_tables method to avoid the 'list' object error
        def mock_preload_tables():
            accelerator.stats["tables_preloaded"] = 2  # Match the number of tables in preload_tables
            accelerator._preload_completed = True
            return True

        accelerator.preload_tables = Mock(side_effect=mock_preload_tables)
        accelerator.preload_tables()

        # Test initial statistics
        stats = accelerator.get_statistics()

        expected_keys = [
            "initialized",
            "total_queries",
            "queries_accelerated",
            "queries_fallback",
            "acceleration_rate",
            "avg_acceleration_time",
            "avg_fallback_time",
            "tables_preloaded",
            "preload_time",
            "last_query_time",
        ]

        for key in expected_keys:
            assert key in stats

        assert stats["initialized"] is True
        assert stats["tables_preloaded"] == 2
        assert isinstance(stats["preload_time"], float)
        assert stats["preload_time"] == 0.0  # Mocked preload sets stats directly, no actual timing

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_context_manager_usage(self, mock_psycopg, mock_duckdb):
        """Test context manager functionality."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock connections
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        mock_pg_conn = Mock()
        mock_psycopg.connect.return_value = mock_pg_conn

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        with DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params) as accelerator:
            assert accelerator._initialized is True
            assert accelerator.duckdb_conn is not None
            assert accelerator.postgresql_conn is not None

        # Should be closed after context
        assert accelerator._initialized is False
        mock_duckdb_conn.close.assert_called_once()
        # PostgreSQL connection close is called through _cleanup_connections

    @patch("partitioncache.query_accelerator.duckdb")
    def test_initialization_failure_handling(self, mock_duckdb):
        """Test graceful handling of initialization failures."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock DuckDB connection failure
        mock_duckdb.connect.side_effect = Exception("Connection failed")

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params)

        # Initialization should fail gracefully
        result = accelerator.initialize()
        assert result is False
        assert accelerator._initialized is False

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_factory_function(self, mock_psycopg, mock_duckdb):
        """Test the factory function for creating accelerators."""
        from partitioncache.query_accelerator import create_query_accelerator

        # Mock connections
        mock_duckdb.connect.return_value = Mock()
        mock_psycopg.connect.return_value = Mock()

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = create_query_accelerator(postgresql_connection_params=postgresql_params, preload_tables=["test_table"], duckdb_memory_limit="512MB")

        assert accelerator is not None
        assert accelerator._initialized is True
        assert accelerator.duckdb_memory_limit == "512MB"
        assert accelerator.tables_to_preload == ["test_table"]

    def test_error_handling_with_statistics_disabled(self):
        """Test error handling when statistics are disabled."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params, enable_statistics=False)

        # Should handle statistics gracefully when disabled
        stats = accelerator.get_statistics()
        assert "initialized" in stats
        assert stats["initialized"] is False  # Not initialized yet

        # Log statistics should not raise error
        accelerator.log_statistics()  # Should not raise exception

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_query_timeout_configuration(self, mock_psycopg, mock_duckdb):
        """Test timeout configuration and statistics tracking."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock connections
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        mock_psycopg.connect.return_value = Mock()

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        # Test with timeout configured
        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params, query_timeout=5.0, enable_statistics=True)

        accelerator.initialize()

        # Verify timeout is set
        assert accelerator.query_timeout == 5.0

        # Verify timeout statistics are initialized
        stats = accelerator.get_statistics()
        assert "queries_timeout" in stats
        assert stats["queries_timeout"] == 0

    @patch("partitioncache.query_accelerator.threading.Thread")
    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_query_timeout_fallback(self, mock_psycopg, mock_duckdb, mock_thread_class):
        """Test query timeout triggers fallback to PostgreSQL."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock DuckDB connection
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn

        # Mock PostgreSQL connection with successful result
        mock_pg_conn = Mock()
        mock_psycopg.connect.return_value = mock_pg_conn
        mock_pg_cursor = Mock()
        mock_cursor_context = Mock()
        mock_cursor_context.__enter__ = Mock(return_value=mock_pg_cursor)
        mock_cursor_context.__exit__ = Mock(return_value=None)
        mock_pg_conn.cursor.return_value = mock_cursor_context
        mock_pg_cursor.fetchall.return_value = [(7,), (8,), (9,)]
        mock_pg_cursor.nextset.return_value = None  # one result set, as a single SELECT gives

        # Mock Thread to simulate timeout
        mock_thread = Mock()
        mock_thread.is_alive.return_value = True  # Simulate thread still running (timeout)
        mock_thread_class.return_value = mock_thread

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_params,
            query_timeout=1.0,  # Short timeout
            enable_statistics=True,
        )

        accelerator.initialize()

        # Test query execution with timeout
        result = accelerator.execute_query("SELECT partition_key FROM test_table")

        # Should fallback to PostgreSQL and return PostgreSQL results
        assert result == {7, 8, 9}

        # Verify statistics show timeout and fallback
        stats = accelerator.get_statistics()
        assert stats["queries_timeout"] == 1
        assert stats["queries_fallback"] == 1
        assert stats["queries_accelerated"] == 0

        # Verify DuckDB connection interrupt was attempted
        mock_duckdb_conn.interrupt.assert_called_once()

        # Verify thread was created and managed properly
        mock_thread_class.assert_called_once()
        mock_thread.start.assert_called_once()
        mock_thread.join.assert_called()

    @patch("partitioncache.query_accelerator.duckdb")
    @patch("partitioncache.query_accelerator.psycopg")
    def test_sql_error_fallback(self, mock_psycopg, mock_duckdb):
        """Test SQL errors (like missing tables) trigger fallback to PostgreSQL."""
        from partitioncache.query_accelerator import DuckDBQueryAccelerator

        # Mock DuckDB connection to raise SQL error (missing table)
        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value = mock_duckdb_conn
        # Let initialization succeed, but make query execution fail with SQL error
        mock_duckdb_conn.execute.side_effect = [
            None,  # First call for "SET memory_limit = '2GB'"
            None,  # Second call for "SET threads = 4"
            None,  # "SET GLOBAL integer_division = true"
            None,  # Third call for "INSTALL postgres"
            None,  # Fourth call for "LOAD postgres"
            Exception("Catalog Error: Table with name 'missing_table' does not exist"),  # Query fails with missing table
        ]

        # Mock PostgreSQL connection with successful result
        mock_pg_conn = Mock()
        mock_psycopg.connect.return_value = mock_pg_conn
        mock_pg_cursor = Mock()
        mock_cursor_context = Mock()
        mock_cursor_context.__enter__ = Mock(return_value=mock_pg_cursor)
        mock_cursor_context.__exit__ = Mock(return_value=None)
        mock_pg_conn.cursor.return_value = mock_cursor_context
        mock_pg_cursor.fetchall.return_value = [(10,), (11,), (12,)]
        mock_pg_cursor.nextset.return_value = None  # one result set, as a single SELECT gives

        postgresql_params = {"host": "localhost", "port": 5432, "user": "test", "password": "test", "dbname": "test"}

        accelerator = DuckDBQueryAccelerator(postgresql_connection_params=postgresql_params, enable_statistics=True)

        accelerator.initialize()

        # Test query execution with SQL error
        result = accelerator.execute_query("SELECT partition_key FROM missing_table")

        # Should fallback to PostgreSQL and return PostgreSQL results
        assert result == {10, 11, 12}

        # Verify statistics show fallback due to SQL error (not timeout)
        stats = accelerator.get_statistics()
        assert stats["queries_fallback"] == 1
        assert stats["queries_accelerated"] == 0
        assert stats["queries_timeout"] == 0  # Should be 0 since this was SQL error, not timeout


class TestDuckDBIntegrationHelpers:
    """Test helper functions and integration utilities."""

    def test_factory_function_error_handling(self):
        """Test factory function error handling."""
        from partitioncache.query_accelerator import create_query_accelerator

        # Test with invalid parameters
        result = create_query_accelerator({})
        # Should return None for invalid parameters
        assert result is None

    @patch("partitioncache.query_accelerator.DuckDBQueryAccelerator")
    def test_factory_function_exception_handling(self, mock_accelerator_class):
        """Test factory function exception handling."""
        from partitioncache.query_accelerator import create_query_accelerator

        # Mock accelerator class to raise exception
        mock_accelerator_class.side_effect = Exception("Test error")

        result = create_query_accelerator({"host": "localhost"})
        assert result is None


if __name__ == "__main__":
    # Allow running tests directly for development
    pytest.main([__file__, "-v", "--tb=short"])


def test_connection_string_values_are_quoted():
    from psycopg.conninfo import conninfo_to_dict

    from partitioncache.query_accelerator import DuckDBQueryAccelerator

    params = {"host": "db host", "port": 5432, "user": "u", "password": "p w'd\\x", "dbname": "my db"}
    conninfo = DuckDBQueryAccelerator(params)._build_duckdb_postgres_connection_string()
    assert conninfo_to_dict(conninfo) == {"host": "db host", "port": "5432", "user": "u", "password": "p w'd\\x", "dbname": "my db"}


def test_default_database_is_in_memory():
    from partitioncache.query_accelerator import DuckDBQueryAccelerator

    assert DuckDBQueryAccelerator({}).duckdb_database_path == ":memory:"


def test_duckdb_uses_the_postgresql_session_time_zone():
    from partitioncache.query_accelerator import DuckDBQueryAccelerator

    accelerator = DuckDBQueryAccelerator({})
    accelerator.postgresql_conn = Mock()
    accelerator.postgresql_conn.execute.return_value.fetchone.return_value = ("America/St_John's",)
    accelerator.duckdb_conn = Mock()
    accelerator._use_postgresql_timezone()
    accelerator.duckdb_conn.execute.assert_called_once_with("SET GLOBAL TimeZone = 'America/St_John''s'")


class _ReferencePostgresCursor:
    """Stands in for the PostgreSQL fallback: runs a script statement by statement on its own DuckDB connection and
    exposes the result sets as psycopg does (a SELECT has rows, the other statements have no description)."""

    def __init__(self, conn):
        self._conn = conn
        self._sets: list = []
        self._index = 0
        self.description = None
        self.queries: list[str] = []

    def execute(self, query):
        import duckdb

        self.queries.append(query)
        self._sets = []
        for statement in self._conn.extract_statements(query):
            rows = self._conn.execute(statement.query).fetchall()
            self._sets.append(rows if statement.type == duckdb.StatementType.SELECT else None)
        self._index = 0
        self._position()

    def _position(self):
        self.description = None if self._sets[self._index] is None else [("key",)]

    def fetchall(self):
        return self._sets[self._index]

    def nextset(self):
        if self._index + 1 >= len(self._sets):
            return None
        self._index += 1
        self._position()
        return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


class _ReferencePostgresConnection:
    def __init__(self, conn):
        self.cursor_ = _ReferencePostgresCursor(conn)

    def cursor(self):
        return self.cursor_


_READINGS = "CREATE TABLE readings (region_id INT, value INT, label VARCHAR); INSERT INTO readings VALUES (1, 10, 'a_b'), (2, 1, 'axb'), (3, 10, 'axb'), (NULL, 10, 'a_b')"


def _accelerator_on_duckdb(query_timeout: float = 0):
    """An accelerator with a real in-memory DuckDB and a reference fallback, both holding the readings table."""
    import duckdb

    from partitioncache.query_accelerator import DuckDBQueryAccelerator

    accelerator = DuckDBQueryAccelerator({}, query_timeout=query_timeout)
    accelerator.duckdb_conn = duckdb.connect()
    accelerator.duckdb_conn.execute(_READINGS)
    reference = duckdb.connect()
    reference.execute(_READINGS)
    accelerator.postgresql_conn = _ReferencePostgresConnection(reference)
    accelerator._initialized = True
    return accelerator


@pytest.mark.parametrize("query_timeout", [0, 30])
@pytest.mark.parametrize("method", ["UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"])
def test_key_table_script_returns_the_select_keys_not_the_trailing_drop(method, query_timeout):
    """DuckDB returns the LAST statement's result, here the DROP's: a script runs on the fallback, which reads the SELECT."""
    from partitioncache.apply_cache import extend_query_with_partition_keys

    script = extend_query_with_partition_keys("SELECT r.region_id FROM readings AS r WHERE r.value > 5", {1, 2}, "region_id", method=method, p0_alias="r")
    accelerator = _accelerator_on_duckdb(query_timeout)

    assert accelerator.execute_query(script) == {1}
    assert accelerator.postgresql_conn.cursor_.queries == [script]
    assert accelerator.stats["queries_accelerated"] == 0
    assert accelerator.stats["queries_fallback"] == 1


@pytest.mark.parametrize("query_timeout", [0, 30])
def test_single_statement_fragment_runs_in_duckdb(query_timeout):
    accelerator = _accelerator_on_duckdb(query_timeout)
    # A semicolon in a string literal and in a trailing comment does not make a second statement
    query = "SELECT r.region_id FROM readings AS r WHERE r.value > 5 AND r.label <> 'x;y'; -- done; really"

    assert accelerator.execute_query(query) == {1, 3}
    assert accelerator.postgresql_conn.cursor_.queries == []
    assert accelerator.stats["queries_accelerated"] == 1


def test_script_without_fallback_connection_raises_instead_of_returning_the_drop_result():
    from partitioncache.apply_cache import extend_query_with_partition_keys

    script = extend_query_with_partition_keys("SELECT r.region_id FROM readings AS r", {1}, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="r")
    accelerator = _accelerator_on_duckdb()
    accelerator.postgresql_conn = None

    with pytest.raises(RuntimeError, match="fallback"):
        accelerator.execute_query(script)


@pytest.mark.parametrize("path", ["duckdb", "duckdb_timeout", "fallback"])
def test_null_keys_are_dropped_on_every_path(path):
    """PostgresDBHandler.execute drops NULL keys; the accelerator must return the same set."""
    accelerator = _accelerator_on_duckdb(query_timeout=30 if path == "duckdb_timeout" else 0)
    if path == "fallback":
        accelerator._initialized = False

    assert accelerator.execute_query("SELECT r.region_id FROM readings AS r WHERE r.value > 5") == {1, 3}
    assert len(accelerator.postgresql_conn.cursor_.queries) == (1 if path == "fallback" else 0)


def test_like_pattern_with_backslash_runs_on_the_fallback():
    """DuckDB's LIKE has no default escape character ('a_b' LIKE 'a\\_b' is false), PostgreSQL's is the backslash."""
    accelerator = _accelerator_on_duckdb()
    query = "SELECT r.region_id FROM readings AS r WHERE r.label LIKE 'a\\_b'"

    accelerator.execute_query(query)

    assert accelerator.postgresql_conn.cursor_.queries == [query]
    assert accelerator.stats["queries_accelerated"] == 0


def test_like_pattern_without_backslash_runs_in_duckdb():
    accelerator = _accelerator_on_duckdb()

    assert accelerator.execute_query("SELECT r.region_id FROM readings AS r WHERE r.label LIKE 'a_b'") == {1, 2, 3}
    assert accelerator.postgresql_conn.cursor_.queries == []

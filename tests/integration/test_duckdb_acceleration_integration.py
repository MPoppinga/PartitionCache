"""
Integration tests for DuckDB query acceleration feature.

This test suite validates the DuckDB query acceleration functionality
with real database connections and query execution scenarios.
"""

import os
import time

import pytest

from partitioncache.cli.common_args import get_database_connection_params
from partitioncache.query_accelerator import DuckDBQueryAccelerator, create_query_accelerator


@pytest.fixture(scope="module")
def postgresql_params():
    """Get PostgreSQL connection parameters from environment."""
    # Mock args object for get_database_connection_params
    class MockArgs:
        def __init__(self):
            self.db_host = os.getenv("DB_HOST", "localhost")
            self.db_port = int(os.getenv("DB_PORT", "5432"))
            self.db_user = os.getenv("DB_USER", "test_user")
            self.db_password = os.getenv("DB_PASSWORD", "test_password")
            self.db_name = os.getenv("DB_NAME", "test_db")
            self.db_backend = "postgresql"

    mock_args = MockArgs()

    # Skip if PostgreSQL not available
    try:
        import psycopg
        # Use a short timeout to avoid hanging
        conn = psycopg.connect(
            host=mock_args.db_host,
            port=mock_args.db_port,
            user=mock_args.db_user,
            password=mock_args.db_password,
            dbname=mock_args.db_name,
            connect_timeout=5  # 5 second timeout
        )
        conn.close()
    except Exception:
        pytest.skip("PostgreSQL not available for integration tests")

    params = get_database_connection_params(mock_args)
    # Add connection timeout to prevent hanging in CI/local environments
    params['connect_timeout'] = 5
    return params


@pytest.mark.integration
class TestDuckDBAccelerationIntegration:
    """Integration tests for DuckDB query acceleration."""

    @pytest.fixture(scope="class")
    def test_table_setup(self, postgresql_params):
        """Set up test tables in PostgreSQL for acceleration testing."""
        import psycopg

        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            # Create test table with sample data
            cursor.execute("""
                DROP TABLE IF EXISTS acceleration_test_table
            """)

            cursor.execute("""
                CREATE TABLE acceleration_test_table (
                    id SERIAL PRIMARY KEY,
                    partition_key INTEGER,
                    category TEXT,
                    value DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Insert test data
            cursor.execute("""
                INSERT INTO acceleration_test_table (partition_key, category, value)
                SELECT
                    i % 100 as partition_key,
                    CASE (i % 3)
                        WHEN 0 THEN 'category_a'
                        WHEN 1 THEN 'category_b'
                        ELSE 'category_c'
                    END as category,
                    RANDOM() * 1000 as value
                FROM generate_series(1, 1000) i
            """)

            conn.commit()

        conn.close()

        yield "acceleration_test_table"

        # Cleanup
        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS acceleration_test_table")
            conn.commit()
        conn.close()

    def test_accelerator_initialization(self, postgresql_params):
        """Test that DuckDB accelerator initializes correctly."""
        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[],
            duckdb_memory_limit="1GB",
            duckdb_threads=2
        )

        assert not accelerator._initialized

        # Initialize
        success = accelerator.initialize()
        assert success
        assert accelerator._initialized
        assert accelerator.duckdb_conn is not None
        assert accelerator.postgresql_conn is not None

        # Test statistics
        stats = accelerator.get_statistics()
        assert stats["initialized"] is True
        assert stats["queries_accelerated"] == 0
        assert stats["queries_fallback"] == 0

        accelerator.close()
        assert not accelerator._initialized

    def test_factory_function(self, postgresql_params):
        """Test the factory function for creating accelerators."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[],
            duckdb_memory_limit="512MB",
            duckdb_database_path=":memory:"
        )

        assert accelerator is not None
        assert accelerator._initialized

        accelerator.close()

    def test_table_preloading(self, postgresql_params, test_table_setup):
        """Test table preloading functionality."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_memory_limit="1GB",
            duckdb_database_path=":memory:"
        )

        assert accelerator is not None

        # Preload tables
        success = accelerator.preload_tables()
        assert success
        assert accelerator._preload_completed

        stats = accelerator.get_statistics()
        assert stats["tables_preloaded"] == 1
        assert stats["preload_time"] > 0

        # Test that preloaded table is accessible in DuckDB
        result = accelerator.duckdb_conn.execute(
            f"SELECT COUNT(*) FROM {test_table_setup}"
        ).fetchone()
        assert result[0] == 1000  # Should match our test data

        accelerator.close()

    def test_query_acceleration_basic(self, postgresql_params, test_table_setup):
        """Test basic query acceleration functionality."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_database_path=":memory:"
        )

        # Preload tables
        accelerator.preload_tables()

        # Test simple query
        query = f"SELECT partition_key FROM {test_table_setup} WHERE partition_key < 10"
        result = accelerator.execute_query(query)

        assert isinstance(result, set)
        assert len(result) > 0
        assert all(isinstance(x, int) for x in result)
        assert all(x < 10 for x in result)

        # Check statistics
        stats = accelerator.get_statistics()
        assert stats["queries_accelerated"] == 1
        assert stats["queries_fallback"] == 0

        accelerator.close()

    def test_query_fallback_behavior(self, postgresql_params):
        """Test fallback to PostgreSQL when DuckDB queries fail."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[],
            duckdb_database_path=":memory:"
        )

        # Query using PostgreSQL-specific function that doesn't exist in DuckDB
        # This should trigger fallback to PostgreSQL
        query = "SELECT pg_backend_pid() as test_value"
        result = accelerator.execute_query(query)

        assert isinstance(result, set)
        assert len(result) == 1
        # Should contain a process ID (integer)
        pid = next(iter(result))
        assert isinstance(pid, int) and pid > 0

        # Check that fallback was used
        stats = accelerator.get_statistics()
        assert stats["queries_fallback"] >= 1

        accelerator.close()

    def test_performance_comparison(self, postgresql_params, test_table_setup):
        """Test that acceleration provides performance benefits."""
        # Create accelerator with preloaded table
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_database_path=":memory:"
        )
        accelerator.preload_tables()

        # Complex analytical query
        query = f"""
        SELECT partition_key
        FROM {test_table_setup}
        WHERE category = 'category_a'
        AND value > 500
        ORDER BY partition_key
        """

        # Time accelerated query
        start_time = time.perf_counter()
        accelerated_result = accelerator.execute_query(query)
        accelerated_time = time.perf_counter() - start_time

        # Time fallback query (force fallback by breaking DuckDB)
        original_conn = accelerator.duckdb_conn
        accelerator.duckdb_conn = None  # Force fallback

        start_time = time.perf_counter()
        fallback_result = accelerator.execute_query(query)
        fallback_time = time.perf_counter() - start_time

        # Restore connection
        accelerator.duckdb_conn = original_conn

        # Results should be identical
        assert accelerated_result == fallback_result

        # Log performance comparison
        print(f"Accelerated query: {accelerated_time:.4f}s")
        print(f"Fallback query: {fallback_time:.4f}s")

        # Note: In CI, performance may vary, so we just ensure both work
        assert len(accelerated_result) > 0

        accelerator.close()

    def test_error_handling_and_recovery(self, postgresql_params):
        """Test error handling and recovery scenarios."""
        # Test initialization with invalid parameters
        invalid_params = postgresql_params.copy()
        invalid_params["host"] = "nonexistent_host"

        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=invalid_params,
            preload_tables=[]
        )

        # Initialization should fail gracefully
        success = accelerator.initialize()
        assert not success
        assert not accelerator._initialized

        # Test with invalid table preloading
        valid_accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=["nonexistent_table"],
            duckdb_database_path=":memory:"
        )

        # Should still initialize but preloading should handle missing table
        assert valid_accelerator._initialized

        # Preloading should complete but with warnings
        success = valid_accelerator.preload_tables()
        # Should return True even if some tables missing
        assert success

        stats = valid_accelerator.get_statistics()
        assert stats["tables_preloaded"] == 0  # No tables actually loaded

        valid_accelerator.close()

    def test_context_manager_usage(self, postgresql_params):
        """Test context manager functionality."""
        with DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[]
        ) as accelerator:
            assert accelerator._initialized

            # Test basic query
            result = accelerator.execute_query("SELECT 42 as answer")
            assert 42 in result

        # Should be closed after context
        assert not accelerator._initialized

    def test_statistics_tracking(self, postgresql_params, test_table_setup):
        """Test comprehensive statistics tracking."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            enable_statistics=True,
            duckdb_database_path=":memory:"  # Use in-memory for test isolation
        )

        accelerator.preload_tables()

        # Execute multiple queries
        queries = [
            f"SELECT partition_key FROM {test_table_setup} WHERE partition_key < 5",
            f"SELECT partition_key FROM {test_table_setup} WHERE category = 'category_a'",
            "SELECT 1 as test",  # Simple query
        ]

        for query in queries:
            accelerator.execute_query(query)

        stats = accelerator.get_statistics()

        # Verify statistics structure
        assert "total_queries" in stats
        assert "queries_accelerated" in stats
        assert "queries_fallback" in stats
        assert "acceleration_rate" in stats
        assert "avg_acceleration_time" in stats
        assert "tables_preloaded" in stats
        assert "preload_time" in stats

        # Verify values
        assert stats["total_queries"] == 3
        assert stats["queries_accelerated"] >= 1
        assert stats["acceleration_rate"] >= 0.0
        assert stats["tables_preloaded"] == 1

        # Test statistics logging
        accelerator.log_statistics()  # Should not raise error

        accelerator.close()

    def test_memory_limit_configuration(self, postgresql_params):
        """Test memory limit configuration."""
        # Test with different memory limits
        memory_limits = ["512MB", "1GB", "2GB"]

        for memory_limit in memory_limits:
            accelerator = create_query_accelerator(
                postgresql_connection_params=postgresql_params,
                duckdb_memory_limit=memory_limit,
                preload_tables=[],
                duckdb_database_path=":memory:"
            )

            assert accelerator.duckdb_memory_limit == memory_limit

            # Execute a query to ensure it works
            result = accelerator.execute_query("SELECT 1 as test")
            assert 1 in result

            accelerator.close()

    def test_acceleration_with_monitor_integration(self, postgresql_params, test_table_setup):
        """Test integration with pcache-monitor workflow."""
        # Simulate the monitor workflow integration
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_database_path=":memory:"
        )

        accelerator.preload_tables()

        # Test query that would be executed by monitor
        monitor_query = f"""
        SELECT partition_key
        FROM {test_table_setup}
        WHERE category = 'category_a'
        AND value BETWEEN 100 AND 900
        """

        result = accelerator.execute_query(monitor_query)

        assert isinstance(result, set)
        assert len(result) > 0

        # Verify statistics tracking
        stats = accelerator.get_statistics()
        assert stats["queries_accelerated"] >= 1

        accelerator.close()

    def test_concurrent_query_execution(self, postgresql_params, test_table_setup):
        """Test concurrent query execution scenarios."""
        import queue
        import threading

        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_database_path=":memory:"
        )

        accelerator.preload_tables()

        results_queue = queue.Queue()
        errors_queue = queue.Queue()

        def execute_query(query_id):
            try:
                query = f"SELECT partition_key FROM {test_table_setup} WHERE partition_key < {10 + query_id}"
                result = accelerator.execute_query(query)
                results_queue.put((query_id, result))
            except Exception as e:
                errors_queue.put((query_id, str(e)))

        # Execute 5 concurrent queries
        threads = []
        for i in range(5):
            thread = threading.Thread(target=execute_query, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=10)

        # Check results
        assert results_queue.qsize() == 5
        assert errors_queue.empty()

        # Verify all queries returned results
        while not results_queue.empty():
            query_id, result = results_queue.get()
            assert isinstance(result, set)
            assert len(result) > 0

        accelerator.close()

    def test_large_result_set_handling(self, postgresql_params, test_table_setup):
        """Test handling of large result sets."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_database_path=":memory:"
        )

        accelerator.preload_tables()

        # Query that returns most of the test data
        large_query = f"SELECT partition_key FROM {test_table_setup} WHERE partition_key < 90"

        result = accelerator.execute_query(large_query)

        assert isinstance(result, set)
        assert len(result) >= 80  # Should return most partition keys < 90

        # Verify statistics
        stats = accelerator.get_statistics()
        assert stats["queries_accelerated"] >= 1

        accelerator.close()

    def test_query_with_complex_conditions(self, postgresql_params, test_table_setup):
        """Test complex SQL queries with multiple conditions."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_database_path=":memory:"
        )

        accelerator.preload_tables()

        # Complex query with multiple conditions and functions
        complex_query = f"""
        SELECT DISTINCT partition_key
        FROM {test_table_setup}
        WHERE category IN ('category_a', 'category_b')
        AND value > 200
        AND EXTRACT(YEAR FROM created_at) >= 2024
        ORDER BY partition_key
        LIMIT 50
        """

        result = accelerator.execute_query(complex_query)

        assert isinstance(result, set)
        assert len(result) <= 50  # Limited by LIMIT clause

        # Verify all results are integers (partition keys)
        assert all(isinstance(x, int) for x in result)

        accelerator.close()

    def test_threading_configuration(self, postgresql_params):
        """Test thread configuration for DuckDB."""
        thread_counts = [1, 2, 4]

        for thread_count in thread_counts:
            accelerator = create_query_accelerator(
                postgresql_connection_params=postgresql_params,
                duckdb_threads=thread_count,
                preload_tables=[],
                duckdb_database_path=":memory:"
            )

            assert accelerator.duckdb_threads == thread_count

            # Execute a query to ensure it works
            result = accelerator.execute_query("SELECT 1 as test")
            assert 1 in result

            accelerator.close()


@pytest.mark.integration
@pytest.mark.slow
class TestDuckDBAccelerationPerformance:
    """Performance-focused integration tests for DuckDB acceleration."""

    @pytest.fixture(scope="class")
    def large_dataset_setup(self, postgresql_params):
        """Set up a larger dataset for performance testing."""
        import psycopg

        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            # Create larger test table
            cursor.execute("""
                DROP TABLE IF EXISTS perf_test_table
            """)

            cursor.execute("""
                CREATE TABLE perf_test_table (
                    id SERIAL PRIMARY KEY,
                    partition_key INTEGER,
                    category TEXT,
                    subcategory TEXT,
                    value DOUBLE PRECISION,
                    amount DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Insert larger dataset (10K rows)
            cursor.execute("""
                INSERT INTO perf_test_table (partition_key, category, subcategory, value, amount)
                SELECT
                    i % 1000 as partition_key,
                    CASE (i % 5)
                        WHEN 0 THEN 'electronics'
                        WHEN 1 THEN 'clothing'
                        WHEN 2 THEN 'books'
                        WHEN 3 THEN 'sports'
                        ELSE 'home'
                    END as category,
                    'sub_' || ((i % 10)::TEXT) as subcategory,
                    RANDOM() * 10000 as value,
                    (RANDOM() * 999.99)::DECIMAL(10,2) as amount
                FROM generate_series(1, 10000) i
            """)

            conn.commit()

        conn.close()

        yield "perf_test_table"

        # Cleanup
        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS perf_test_table")
            conn.commit()
        conn.close()

    def test_complex_query_performance(self, postgresql_params, large_dataset_setup):
        """Test performance on complex analytical queries."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[large_dataset_setup],
            duckdb_memory_limit="2GB",
            duckdb_database_path=":memory:"
        )

        accelerator.preload_tables()

        # Complex analytical query
        complex_query = f"""
        SELECT partition_key
        FROM {large_dataset_setup}
        WHERE category IN ('electronics', 'books')
        AND value > 5000
        AND amount > 500
        AND subcategory LIKE 'sub_%'
        ORDER BY partition_key
        """

        # Execute query multiple times to get consistent timing
        times = []
        for _ in range(3):
            start_time = time.perf_counter()
            result = accelerator.execute_query(complex_query)
            duration = time.perf_counter() - start_time
            times.append(duration)

        avg_time = sum(times) / len(times)

        # Verify results
        assert len(result) > 0
        assert all(isinstance(x, int) for x in result)

        # Log performance metrics
        stats = accelerator.get_statistics()
        print(f"Complex query average time: {avg_time:.4f}s")
        print(f"Result count: {len(result)}")
        print(f"Acceleration rate: {stats.get('acceleration_rate', 0):.1%}")

        accelerator.close()

    def test_aggregation_performance(self, postgresql_params, large_dataset_setup):
        """Test performance on aggregation queries."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[large_dataset_setup],
            duckdb_database_path=":memory:"
        )

        accelerator.preload_tables()

        # Aggregation query
        agg_query = f"""
        SELECT partition_key
        FROM {large_dataset_setup}
        WHERE category = 'electronics'
        GROUP BY partition_key
        HAVING COUNT(*) > 5
        """

        start_time = time.perf_counter()
        result = accelerator.execute_query(agg_query)
        duration = time.perf_counter() - start_time

        assert len(result) > 0
        print(f"Aggregation query time: {duration:.4f}s")
        print(f"Groups found: {len(result)}")

        accelerator.close()


@pytest.mark.integration
class TestDuckDBAccelerationMonitorIntegration:
    """Tests specifically for pcache-monitor integration scenarios."""

    def test_monitor_integration_with_cache_handlers(self, postgresql_params):
        """Test acceleration with different cache handler backends."""
        from partitioncache.cache_handler import get_cache_handler

        # Test with postgresql_array backend (most common)
        cache_handler = get_cache_handler("postgresql_array")

        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[],
            duckdb_database_path=":memory:"
        )

        # Simulate monitor query execution
        test_query = "SELECT 1 as partition_key UNION SELECT 2 UNION SELECT 3"
        result = accelerator.execute_query(test_query)

        assert result == {1, 2, 3}

        # Verify cache handler can store results
        if hasattr(cache_handler, 'register_partition_key'):
            cache_handler.register_partition_key("test_partition", "integer")
            cache_handler.set_cache("test_hash", result, "test_partition")
            cached_result = cache_handler.get("test_hash", "test_partition")
            assert cached_result == result

        accelerator.close()

    def test_monitor_workflow_simulation(self, postgresql_params):
        """Test complete monitor workflow with acceleration."""
        # Create test data table
        import psycopg

        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("""
                DROP TABLE IF EXISTS monitor_test_data
            """)

            cursor.execute("""
                CREATE TABLE monitor_test_data (
                    id SERIAL PRIMARY KEY,
                    partition_key INTEGER,
                    data_value TEXT
                )
            """)

            cursor.execute("""
                INSERT INTO monitor_test_data (partition_key, data_value)
                SELECT i % 100, 'data_' || i
                FROM generate_series(1, 500) i
            """)

            conn.commit()

        conn.close()

        # Test acceleration with this data
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=["monitor_test_data"],
            duckdb_database_path=":memory:"  # Use in-memory for test isolation
        )

        accelerator.preload_tables()

        # Simulate monitor queries
        monitor_queries = [
            "SELECT partition_key FROM monitor_test_data WHERE partition_key < 10",
            "SELECT partition_key FROM monitor_test_data WHERE partition_key BETWEEN 20 AND 30",
            "SELECT partition_key FROM monitor_test_data WHERE data_value LIKE 'data_1%'"
        ]

        results = []
        for query in monitor_queries:
            result = accelerator.execute_query(query)
            results.append(result)
            assert isinstance(result, set)
            assert len(result) > 0

        # Verify statistics
        stats = accelerator.get_statistics()
        assert stats["queries_accelerated"] == len(monitor_queries)
        assert stats["acceleration_rate"] == 1.0  # 100% acceleration

        # Cleanup
        accelerator.close()

        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS monitor_test_data")
            conn.commit()
        conn.close()

    def test_error_recovery_in_monitor_context(self, postgresql_params):
        """Test error recovery scenarios in monitor context."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=["nonexistent_table"],  # This will cause preload warnings
            duckdb_database_path=":memory:"
        )

        # Even with preload warnings, accelerator should work
        assert accelerator._initialized

        # Test fallback with invalid DuckDB query
        invalid_query = "SELECT partition_key FROM nonexistent_table_in_duckdb"

        # Should fallback to PostgreSQL gracefully
        try:
            accelerator.execute_query(invalid_query)
            # If this doesn't raise an exception, it means fallback worked
            # (though query will likely fail in PostgreSQL too)
        except Exception:
            # Expected if table doesn't exist in PostgreSQL either
            pass

        # Verify fallback statistics
        stats = accelerator.get_statistics()
        assert stats["queries_fallback"] >= 1

        accelerator.close()


if __name__ == "__main__":
    # Allow running tests directly for development
    pytest.main([__file__, "-v", "--tb=short"])


@pytest.mark.integration
class TestDuckDBAccelerationRobustness:
    """Objects beyond plain tables, PostgreSQL semantics, and recovery from errors and timeouts."""

    @pytest.fixture(scope="class")
    def objects(self, postgresql_params):
        import psycopg

        conn = psycopg.connect(**postgresql_params, autocommit=True)
        statements = [
            "DROP SCHEMA IF EXISTS acc_rb_s CASCADE",
            'DROP TABLE IF EXISTS acc_rb_points, acc_rb_part, "AccRbMixed", "acc-rb-dash" CASCADE',
            "CREATE TABLE acc_rb_points (pk int, x float8, kind text)",
            "INSERT INTO acc_rb_points SELECT g, g * 0.5, CASE WHEN g % 2 = 0 THEN 'a' ELSE 'b' END FROM generate_series(1, 200) g",
            "CREATE VIEW acc_rb_view AS SELECT pk, x FROM acc_rb_points WHERE kind = 'a'",
            "CREATE MATERIALIZED VIEW acc_rb_mview AS SELECT pk, x * 2 AS x2 FROM acc_rb_points WHERE pk <= 100",
            "CREATE TABLE acc_rb_part (pk int, region int) PARTITION BY RANGE (region)",
            "CREATE TABLE acc_rb_part_1 PARTITION OF acc_rb_part FOR VALUES FROM (0) TO (10)",
            "CREATE TABLE acc_rb_part_2 PARTITION OF acc_rb_part FOR VALUES FROM (10) TO (20)",
            "INSERT INTO acc_rb_part SELECT g, g % 20 FROM generate_series(1, 200) g",
            "CREATE SCHEMA acc_rb_s",
            "CREATE TABLE acc_rb_s.tab (pk int, v int)",
            "INSERT INTO acc_rb_s.tab SELECT g, g FROM generate_series(1, 50) g",
            'CREATE TABLE "AccRbMixed" (pk int)',
            'INSERT INTO "AccRbMixed" SELECT generate_series(1, 10)',
            'CREATE TABLE "acc-rb-dash" (pk int)',
            'INSERT INTO "acc-rb-dash" VALUES (1), (2)',
        ]
        for statement in statements:
            conn.execute(statement)
        yield conn
        conn.execute("DROP SCHEMA IF EXISTS acc_rb_s CASCADE")
        conn.execute('DROP TABLE IF EXISTS acc_rb_points, acc_rb_part, "AccRbMixed", "acc-rb-dash" CASCADE')
        conn.close()

    @staticmethod
    def _accelerator(postgresql_params, tables, **kwargs):
        accelerator = DuckDBQueryAccelerator(postgresql_params, preload_tables=tables, enable_statistics=False, duckdb_database_path=":memory:", **kwargs)
        assert accelerator.initialize()
        assert accelerator.preload_tables()
        return accelerator

    @staticmethod
    def _assert_runs_in_duckdb(accelerator, conn, query):
        accelerated = accelerator.stats["queries_accelerated"]
        result = accelerator.execute_query(query)
        assert accelerator.stats["queries_accelerated"] == accelerated + 1, f"ran on PostgreSQL: {query}"
        assert result == {row[0] for row in conn.execute(query).fetchall()}

    def test_views_partitions_schemas_and_quoted_names_are_preloaded(self, postgresql_params, objects):
        tables = ["acc_rb_points", "acc_rb_view", "acc_rb_mview", "acc_rb_part", "acc_rb_s.tab", "AccRbMixed", "acc-rb-dash"]
        with self._accelerator(postgresql_params, tables) as accelerator:
            assert accelerator.stats["tables_preloaded"] == len(tables)
            for query in (
                "SELECT pk FROM acc_rb_view WHERE x > 50",
                "SELECT pk FROM acc_rb_mview WHERE x2 > 50",
                "SELECT pk FROM acc_rb_part WHERE region = 3",
                "SELECT pk FROM acc_rb_s.tab WHERE v > 25",
                'SELECT pk FROM "AccRbMixed"',
                'SELECT pk FROM "acc-rb-dash"',
            ):
                self._assert_runs_in_duckdb(accelerator, objects, query)

    def test_integer_division_as_in_postgresql(self, postgresql_params, objects):
        with self._accelerator(postgresql_params, ["acc_rb_points"]) as accelerator:
            self._assert_runs_in_duckdb(accelerator, objects, "SELECT pk FROM acc_rb_points WHERE pk / 3 = 10")

    def test_tables_not_preloaded_are_read_from_postgresql(self, postgresql_params, objects):
        with self._accelerator(postgresql_params, ["acc_rb_points"], read_unloaded_from_postgres=True) as accelerator:
            self._assert_runs_in_duckdb(accelerator, objects, "SELECT p.pk FROM acc_rb_points p, acc_rb_part q WHERE p.pk = q.pk AND q.region = 1")

    def test_fallback_keeps_working_after_an_error(self, postgresql_params, objects):
        with self._accelerator(postgresql_params, []) as accelerator:
            import psycopg

            with pytest.raises(psycopg.errors.UndefinedColumn):
                accelerator.execute_query("SELECT no_such_column FROM acc_rb_points")
            assert accelerator.execute_query("SELECT pk FROM acc_rb_points WHERE pk < 3") == {1, 2}
            assert accelerator.postgresql_conn.info.transaction_status.name == "IDLE"

    def test_timed_out_query_is_stopped(self, postgresql_params, objects):
        import threading

        with self._accelerator(postgresql_params, [], query_timeout=1) as accelerator:
            accelerator._execute_fallback = lambda query: set()
            start = time.perf_counter()
            accelerator.execute_query("SELECT count(*) FROM range(3000000000) t(i) WHERE hash(i) % 7 = 3")
            assert accelerator.stats["queries_timeout"] == 1
            assert time.perf_counter() - start < 15
            assert not [thread for thread in threading.enumerate() if thread.daemon and thread is not threading.current_thread() and thread.is_alive()]

    @pytest.mark.parametrize("preload_name", ["acc_rb_points", "public.acc_rb_points"])
    def test_both_spellings_of_a_public_table_use_the_preloaded_copy(self, postgresql_params, objects, preload_name):
        with self._accelerator(postgresql_params, [preload_name], read_unloaded_from_postgres=True) as accelerator:
            objects.execute("INSERT INTO acc_rb_points VALUES (9999, 1.0, 'a')")
            try:
                assert 9999 not in accelerator.execute_query("SELECT pk FROM acc_rb_points")
                assert 9999 not in accelerator.execute_query("SELECT pk FROM public.acc_rb_points")
            finally:
                objects.execute("DELETE FROM acc_rb_points WHERE pk = 9999")

    def test_timestamptz_is_evaluated_in_the_postgresql_session_time_zone(self, postgresql_params, objects):
        """DuckDB would use the machine's zone; the session zone here is far from any runner's (UTC+14)."""
        objects.execute("DROP TABLE IF EXISTS acc_rb_ev")
        objects.execute("CREATE TABLE acc_rb_ev (pk int, ts timestamptz)")
        objects.execute("INSERT INTO acc_rb_ev VALUES (1, '2024-01-01 00:30:00+00'), (2, '2023-12-31 23:30:00+00'), (3, '2024-01-01 12:00:00+00')")
        params = dict(postgresql_params, options="-c TimeZone=Pacific/Kiritimati")
        try:
            import psycopg

            reference = psycopg.connect(**params, autocommit=True)
            with self._accelerator(params, ["acc_rb_ev"]) as accelerator:
                for query in (
                    "SELECT pk FROM acc_rb_ev WHERE ts < '2024-01-01 13:00:00'",
                    "SELECT pk FROM acc_rb_ev WHERE extract(hour FROM ts) = 14",
                    "SELECT pk FROM acc_rb_ev WHERE ts::date = '2024-01-01'",
                    "SELECT pk FROM acc_rb_ev WHERE date_trunc('day', ts) = '2024-01-02'",
                ):
                    self._assert_runs_in_duckdb(accelerator, reference, query)
            reference.close()
        finally:
            objects.execute("DROP TABLE IF EXISTS acc_rb_ev")

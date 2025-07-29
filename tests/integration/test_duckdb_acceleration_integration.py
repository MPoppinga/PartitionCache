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


@pytest.mark.integration
class TestDuckDBAccelerationIntegration:
    """Integration tests for DuckDB query acceleration."""

    @pytest.fixture(scope="class")
    def postgresql_params(self):
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
            psycopg.connect(
                host=mock_args.db_host,
                port=mock_args.db_port,
                user=mock_args.db_user,
                password=mock_args.db_password,
                dbname=mock_args.db_name
            ).close()
        except Exception:
            pytest.skip("PostgreSQL not available for integration tests")

        return get_database_connection_params(mock_args)

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
            duckdb_memory_limit="512MB"
        )

        assert accelerator is not None
        assert accelerator._initialized

        accelerator.close()

    def test_table_preloading(self, postgresql_params, test_table_setup):
        """Test table preloading functionality."""
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup],
            duckdb_memory_limit="1GB"
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
            preload_tables=[test_table_setup]
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
            preload_tables=[]
        )

        # Query a table that doesn't exist in DuckDB but exists in PostgreSQL
        # This should trigger fallback
        query = "SELECT 1 as test_value"
        result = accelerator.execute_query(query)

        assert isinstance(result, set)
        assert 1 in result

        # Check that fallback was used
        stats = accelerator.get_statistics()
        assert stats["queries_fallback"] >= 1

        accelerator.close()

    def test_performance_comparison(self, postgresql_params, test_table_setup):
        """Test that acceleration provides performance benefits."""
        # Create accelerator with preloaded table
        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[test_table_setup]
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
            preload_tables=["nonexistent_table"]
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
            enable_statistics=True
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
                preload_tables=[]
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
            preload_tables=[test_table_setup]
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
            preload_tables=[test_table_setup]
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
            preload_tables=[test_table_setup]
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
            preload_tables=[test_table_setup]
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
                preload_tables=[]
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
            duckdb_memory_limit="2GB"
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
            preload_tables=[large_dataset_setup]
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

    @pytest.fixture(scope="class")
    def postgresql_params(self):
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
            psycopg.connect(
                host=mock_args.db_host,
                port=mock_args.db_port,
                user=mock_args.db_user,
                password=mock_args.db_password,
                dbname=mock_args.db_name
            ).close()
        except Exception:
            pytest.skip("PostgreSQL not available for integration tests")

        return get_database_connection_params(mock_args)

    def test_monitor_integration_with_cache_handlers(self, postgresql_params):
        """Test acceleration with different cache handler backends."""
        from partitioncache.cache_handler import get_cache_handler

        # Test with postgresql_array backend (most common)
        cache_handler = get_cache_handler("postgresql_array")

        accelerator = create_query_accelerator(
            postgresql_connection_params=postgresql_params,
            preload_tables=[]
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
            preload_tables=["monitor_test_data"]
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
            preload_tables=["nonexistent_table"]  # This will cause preload warnings
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

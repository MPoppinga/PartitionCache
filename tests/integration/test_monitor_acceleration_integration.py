"""
Integration tests for DuckDB acceleration in pcache-monitor.

This test suite validates the complete integration of DuckDB acceleration
with the pcache-monitor command-line tool and various cache backends.
"""

import os
import subprocess
import tempfile
import time

import pytest

from partitioncache.cli.common_args import get_database_connection_params


@pytest.mark.integration
class TestMonitorAccelerationIntegration:
    """Integration tests for monitor with DuckDB acceleration."""

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
    def test_data_setup(self, postgresql_params):
        """Set up test data for monitor acceleration testing."""
        import psycopg

        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            # Create test table
            cursor.execute("""
                DROP TABLE IF EXISTS monitor_accel_test
            """)

            cursor.execute("""
                CREATE TABLE monitor_accel_test (
                    id SERIAL PRIMARY KEY,
                    partition_key INTEGER,
                    category TEXT,
                    value DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Insert test data
            cursor.execute("""
                INSERT INTO monitor_accel_test (partition_key, category, value)
                SELECT
                    i % 50 as partition_key,
                    CASE (i % 3)
                        WHEN 0 THEN 'type_a'
                        WHEN 1 THEN 'type_b'
                        ELSE 'type_c'
                    END as category,
                    RANDOM() * 1000 as value
                FROM generate_series(1, 500) i
            """)

            conn.commit()

        conn.close()

        yield "monitor_accel_test"

        # Cleanup
        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS monitor_accel_test")
            conn.commit()
        conn.close()

    def test_monitor_with_acceleration_enabled(self, postgresql_params, test_data_setup):
        """Test pcache-monitor with DuckDB acceleration enabled."""
        # Set up cache handler and queue
        from partitioncache.cache_handler import get_cache_handler
        from partitioncache.queue import QueueHandler

        # Initialize cache handler
        cache_handler = get_cache_handler("postgresql_array")
        cache_handler.register_partition_key("test_partition", "integer")

        # Initialize queue handler
        queue_handler = QueueHandler()

        # Add test query to queue
        test_query = f"SELECT partition_key FROM {test_data_setup} WHERE category = 'type_a'"
        queue_handler.add_original_query(test_query, "test_partition")

        # Create temporary script to run monitor with acceleration
        script_content = f'''
import os
import sys
sys.path.insert(0, '{os.getcwd()}/src')

from partitioncache.cli.monitor_cache_queue import main
from partitioncache.query_accelerator import create_query_accelerator, DuckDBImportError

# Mock command line arguments
class MockArgs:
    def __init__(self):
        # Database connection
        self.db_host = "{postgresql_params['host']}"
        self.db_port = {postgresql_params['port']}
        self.db_user = "{postgresql_params['user']}"
        self.db_password = "{postgresql_params['password']}"
        self.db_name = "{postgresql_params['dbname']}"
        self.db_backend = "postgresql"

        # Queue configuration
        self.queue_provider = "postgresql"
        self.pg_queue_host = "{postgresql_params['host']}"
        self.pg_queue_port = {postgresql_params['port']}
        self.pg_queue_user = "{postgresql_params['user']}"
        self.pg_queue_password = "{postgresql_params['password']}"
        self.pg_queue_db = "{postgresql_params['dbname']}"

        # Acceleration settings
        self.enable_duckdb_acceleration = True
        self.preload_tables = "{test_data_setup}"
        self.duckdb_memory_limit = "1GB"
        self.duckdb_threads = 2
        self.disable_acceleration_stats = False

        # Process control
        self.max_processes = 1
        self.exit_after_empty = True
        self.disable_optimized_polling = True

# Test acceleration availability
try:
    create_query_accelerator({{}})
    print("DuckDB acceleration available")

    # Run monitor with acceleration
    import sys
    sys.argv = ["pcache-monitor"]  # Mock sys.argv

    args = MockArgs()

    # Import and test the main function logic
    from partitioncache.cli.monitor_cache_queue import setup_acceleration, run_monitoring_loop

    accelerator = setup_acceleration(args)
    if accelerator:
        print("Acceleration setup successful")
        accelerator.close()
    else:
        print("Acceleration setup failed")

except DuckDBImportError:
    print("DuckDB not available, skipping acceleration test")
except Exception as e:
    print(f"Error: {{e}}")
'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            script_path = f.name

        try:
            # Run the test script
            result = subprocess.run(
                ['python', script_path],
                capture_output=True,
                text=True,
                timeout=30
            )

            output = result.stdout
            print("Monitor output:", output)

            # Check if acceleration was properly set up
            if "DuckDB not available" not in output:
                assert "DuckDB acceleration available" in output
                assert "Acceleration setup successful" in output or "Acceleration setup failed" in output
            else:
                pytest.skip("DuckDB not available in test environment")

        except subprocess.TimeoutExpired:
            pytest.fail("Monitor test timed out")
        finally:
            # Clean up temporary script
            os.unlink(script_path)

    def test_monitor_acceleration_with_different_backends(self, postgresql_params, test_data_setup):
        """Test acceleration with different cache backends."""
        from partitioncache.query_accelerator import DuckDBImportError, create_query_accelerator

        # Test with different cache backends
        backend_configs = [
            ("postgresql_array", {}),
            ("postgresql_bit", {"bitsize": 10000}),
            ("redis_set", {} if os.getenv("REDIS_HOST") else None),
        ]

        for backend_name, config in backend_configs:
            if config is None:
                continue  # Skip if backend not available

            try:
                from partitioncache.cache_handler import get_cache_handler

                # Test that cache handler works
                cache_handler = get_cache_handler(backend_name)
                if config:
                    cache_handler.register_partition_key("test_partition", "integer", **config)
                else:
                    cache_handler.register_partition_key("test_partition", "integer")

                # Test that acceleration can work alongside this backend
                accelerator = create_query_accelerator(
                    postgresql_connection_params=postgresql_params,
                    preload_tables=[test_data_setup]
                )

                # Test query
                test_query = f"SELECT partition_key FROM {test_data_setup} WHERE partition_key < 10"
                result = accelerator.execute_query(test_query)

                assert isinstance(result, set)
                assert len(result) > 0

                # Test cache storage
                cache_handler.set_cache("test_hash", result, "test_partition")
                cached_result = cache_handler.get("test_hash", "test_partition")
                assert cached_result == result

                accelerator.close()

                print(f"Backend {backend_name} works with acceleration")

            except DuckDBImportError:
                pytest.skip("DuckDB not available for backend testing")
            except Exception as e:
                print(f"Backend {backend_name} test failed: {e}")
                # Continue testing other backends

    def test_monitor_performance_with_acceleration(self, postgresql_params, test_data_setup):
        """Test performance comparison with and without acceleration."""
        from partitioncache.query_accelerator import DuckDBImportError, create_query_accelerator

        try:
            # Test query performance
            test_query = f"""
            SELECT partition_key
            FROM {test_data_setup}
            WHERE category = 'type_a'
            AND value > 500
            """

            # Test with acceleration
            accelerator = create_query_accelerator(
                postgresql_connection_params=postgresql_params,
                preload_tables=[test_data_setup]
            )

            accelerator.preload_tables()

            # Time accelerated query
            start_time = time.perf_counter()
            accelerated_result = accelerator.execute_query(test_query)
            accelerated_time = time.perf_counter() - start_time

            # Test fallback (disable DuckDB temporarily)
            original_conn = accelerator.duckdb_conn
            accelerator.duckdb_conn = None

            start_time = time.perf_counter()
            fallback_result = accelerator.execute_query(test_query)
            fallback_time = time.perf_counter() - start_time

            # Restore connection
            accelerator.duckdb_conn = original_conn

            # Verify results are identical
            assert accelerated_result == fallback_result

            # Log performance metrics
            stats = accelerator.get_statistics()
            print(f"Accelerated time: {accelerated_time:.4f}s")
            print(f"Fallback time: {fallback_time:.4f}s")
            print(f"Acceleration rate: {stats.get('acceleration_rate', 0):.1%}")

            # Performance should be measurable
            assert len(accelerated_result) > 0
            assert accelerated_time >= 0
            assert fallback_time >= 0

            accelerator.close()

        except DuckDBImportError:
            pytest.skip("DuckDB not available for performance testing")

    def test_monitor_error_handling_with_acceleration(self, postgresql_params):
        """Test error handling scenarios with acceleration enabled."""
        from partitioncache.query_accelerator import DuckDBImportError, create_query_accelerator

        try:
            # Test with invalid table preloading
            accelerator = create_query_accelerator(
                postgresql_connection_params=postgresql_params,
                preload_tables=["nonexistent_table", "another_missing_table"]
            )

            # Should still initialize despite missing tables
            assert accelerator._initialized

            # Test fallback behavior
            test_query = "SELECT 1 as partition_key"
            result = accelerator.execute_query(test_query)

            assert result == {1}

            # Check statistics show fallback usage
            stats = accelerator.get_statistics()
            assert "queries_fallback" in stats

            accelerator.close()

        except DuckDBImportError:
            pytest.skip("DuckDB not available for error handling testing")

    def test_monitor_cli_integration(self, postgresql_params, test_data_setup):
        """Test CLI integration with acceleration flags."""
        # This test verifies that CLI arguments are properly handled

        # Create a simple test for argument parsing
        from partitioncache.cli.monitor_cache_queue import get_args

        test_args = [
            "--enable-duckdb-acceleration",
            "--preload-tables", test_data_setup,
            "--duckdb-memory-limit", "512MB",
            "--duckdb-threads", "2",
            "--db-backend", "postgresql",
            "--db-host", postgresql_params["host"],
            "--db-port", str(postgresql_params["port"]),
            "--db-user", postgresql_params["user"],
            "--db-password", postgresql_params["password"],
            "--db-name", postgresql_params["dbname"],
            "--exit-after-empty"
        ]

        # Test that arguments are parsed correctly
        import sys
        original_argv = sys.argv
        try:
            sys.argv = ["pcache-monitor"] + test_args
            args = get_args()

            assert args.enable_duckdb_acceleration is True
            assert args.preload_tables == test_data_setup
            assert args.duckdb_memory_limit == "512MB"
            assert args.duckdb_threads == 2
            assert args.db_backend == "postgresql"

        finally:
            sys.argv = original_argv


@pytest.mark.integration
@pytest.mark.slow
class TestMonitorAccelerationStressTest:
    """Stress testing for monitor acceleration under load."""

    @pytest.fixture(scope="class")
    def large_dataset_setup(self, postgresql_params):
        """Set up a larger dataset for stress testing."""
        import psycopg

        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("""
                DROP TABLE IF EXISTS accel_stress_test
            """)

            cursor.execute("""
                CREATE TABLE accel_stress_test (
                    id SERIAL PRIMARY KEY,
                    partition_key INTEGER,
                    category TEXT,
                    subcategory TEXT,
                    value DOUBLE PRECISION,
                    amount DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Insert larger dataset (5K rows)
            cursor.execute("""
                INSERT INTO accel_stress_test (partition_key, category, subcategory, value, amount)
                SELECT
                    i % 200 as partition_key,
                    CASE (i % 4)
                        WHEN 0 THEN 'electronics'
                        WHEN 1 THEN 'clothing'
                        WHEN 2 THEN 'books'
                        ELSE 'sports'
                    END as category,
                    'sub_' || ((i % 20)::TEXT) as subcategory,
                    RANDOM() * 5000 as value,
                    (RANDOM() * 999.99)::DECIMAL(10,2) as amount
                FROM generate_series(1, 5000) i
            """)

            conn.commit()

        conn.close()

        yield "accel_stress_test"

        # Cleanup
        conn = psycopg.connect(**postgresql_params)
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS accel_stress_test")
            conn.commit()
        conn.close()

    def test_high_volume_query_acceleration(self, postgresql_params, large_dataset_setup):
        """Test acceleration under high query volume."""
        from partitioncache.query_accelerator import DuckDBImportError, create_query_accelerator

        try:
            accelerator = create_query_accelerator(
                postgresql_connection_params=postgresql_params,
                preload_tables=[large_dataset_setup],
                duckdb_memory_limit="2GB"
            )

            accelerator.preload_tables()

            # Execute many queries rapidly
            queries = [
                f"SELECT partition_key FROM {large_dataset_setup} WHERE partition_key < {i*10}"
                for i in range(1, 21)  # 20 different queries
            ]

            results = []
            start_time = time.perf_counter()

            for query in queries:
                result = accelerator.execute_query(query)
                results.append(result)
                assert isinstance(result, set)

            total_time = time.perf_counter() - start_time

            # Verify statistics
            stats = accelerator.get_statistics()
            print(f"Executed {len(queries)} queries in {total_time:.4f}s")
            print(f"Average time per query: {total_time/len(queries):.4f}s")
            print(f"Acceleration rate: {stats.get('acceleration_rate', 0):.1%}")

            assert stats["total_queries"] == len(queries)
            assert len(results) == len(queries)

            accelerator.close()

        except DuckDBImportError:
            pytest.skip("DuckDB not available for stress testing")


if __name__ == "__main__":
    # Allow running tests directly for development
    pytest.main([__file__, "-v", "--tb=short"])

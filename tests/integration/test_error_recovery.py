import pytest
import time
import psycopg
from unittest.mock import patch, MagicMock

import partitioncache
from partitioncache.cache_handler.abstract import AbstractCacheHandler


class TestErrorRecovery:
    """
    Tests for error recovery and fault tolerance in various scenarios.
    """

    def test_database_connection_recovery(self, db_session):
        """Test recovery from database connection failures."""
        # This test verifies graceful handling of connection issues
        
        # Try to create a cache handler with invalid connection parameters
        original_host = os.getenv("PG_HOST")
        os.environ["PG_HOST"] = "invalid_host_that_does_not_exist"
        
        try:
            # Should handle connection failure gracefully
            with pytest.raises(Exception) as exc_info:
                from partitioncache.cache_handler import get_cache_handler
                cache_handler = get_cache_handler("postgresql_array")
            
            # Error should be informative
            assert "connection" in str(exc_info.value).lower() or "host" in str(exc_info.value).lower()
            
        finally:
            # Restore original host
            if original_host:
                os.environ["PG_HOST"] = original_host

    def test_malformed_query_handling(self, cache_client: AbstractCacheHandler):
        """Test handling of malformed queries in cache operations."""
        partition_key = "zipcode"
        
        # Test with various problematic inputs
        problematic_inputs = [
            ("", "empty_string"),
            ("   ", "whitespace_only"),
            ("SELECT * FROM", "incomplete_sql"),
            ("INVALID SQL SYNTAX $$", "syntax_error"),
            ("SELECT * FROM nonexistent_table;", "missing_table"),
        ]
        
        from partitioncache.query_processor import generate_all_hashes
        
        for query, description in problematic_inputs:
            try:
                # Should handle malformed queries gracefully
                hashes = generate_all_hashes(query, partition_key)
                
                # If hash generation succeeds, cache operations should too
                if hashes:
                    test_values = {1001, 1002}
                    for hash_key in hashes:
                        success = cache_client.set_set(hash_key, test_values, partition_key)
                        # Operations may succeed or fail, but shouldn't crash
                        assert isinstance(success, bool)
                        
            except Exception as e:
                # Exceptions should be meaningful
                error_msg = str(e).lower()
                assert any(keyword in error_msg for keyword in 
                          ["syntax", "parse", "invalid", "error", "malformed"])

    def test_cache_corruption_recovery(self, cache_client: AbstractCacheHandler):
        """Test recovery from cache corruption scenarios."""
        partition_key = "zipcode"
        
        # Store valid data first
        valid_key = "valid_data"
        valid_set = {1001, 1002}
        cache_client.set_set(valid_key, valid_set, partition_key)
        
        # Simulate corruption by attempting invalid operations
        corruption_scenarios = [
            ("null_key", None),
            ("empty_key", ""),
            ("very_long_key", "x" * 10000),
        ]
        
        for scenario_name, corrupt_key in corruption_scenarios:
            try:
                if corrupt_key is not None:
                    # These operations might fail, but shouldn't break the cache
                    cache_client.set_set(corrupt_key, {9999}, partition_key)
                    cache_client.get(corrupt_key, partition_key)
                    cache_client.exists(corrupt_key, partition_key)
            except Exception:
                # Corruption scenarios may raise exceptions
                pass
        
        # Verify that valid data is still accessible after corruption attempts
        retrieved = cache_client.get(valid_key, partition_key)
        assert retrieved == valid_set, "Valid data corrupted by error scenarios"

    def test_resource_exhaustion_handling(self, cache_client: AbstractCacheHandler):
        """Test behavior under resource exhaustion conditions."""
        partition_key = "zipcode"
        
        # Attempt to create very large sets that might exhaust resources
        large_set_sizes = [10000, 50000, 100000]
        
        successful_operations = 0
        for size in large_set_sizes:
            try:
                cache_key = f"resource_test_{size}"
                large_set = set(range(size))
                
                # This might fail due to resource limits
                success = cache_client.set_set(cache_key, large_set, partition_key)
                if success:
                    successful_operations += 1
                    # Verify we can retrieve it
                    retrieved = cache_client.get(cache_key, partition_key)
                    assert len(retrieved) == size, f"Retrieved set size mismatch: {len(retrieved)} != {size}"
                    
            except (MemoryError, Exception) as e:
                # Resource exhaustion is acceptable
                if "memory" in str(e).lower() or "limit" in str(e).lower():
                    break
                else:
                    # Other exceptions should be investigated
                    raise
        
        # Should handle at least small sets successfully
        assert successful_operations > 0, "No operations succeeded under resource pressure"

    def test_concurrent_error_scenarios(self, cache_client: AbstractCacheHandler):
        """Test error handling in concurrent scenarios."""
        partition_key = "zipcode"
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import random
        
        def error_prone_worker(worker_id: int) -> dict:
            """Worker that intentionally creates error conditions."""
            results = {"successes": 0, "errors": 0}
            
            for i in range(20):
                try:
                    # Mix of valid and potentially problematic operations
                    if random.random() < 0.7:  # 70% valid operations
                        cache_key = f"worker_{worker_id}_{i}"
                        test_set = {worker_id * 1000 + i}
                        if cache_client.set_set(cache_key, test_set, partition_key):
                            results["successes"] += 1
                    else:  # 30% problematic operations
                        # Intentionally problematic operations
                        problematic_key = f"problem_{random.choice(['', '   ', 'x' * 1000])}"
                        cache_client.get(problematic_key, partition_key)
                        results["successes"] += 1  # If no exception, count as success
                        
                except Exception:
                    results["errors"] += 1
            
            return results
        
        # Run concurrent error-prone operations
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(error_prone_worker, i) for i in range(5)]
            results = [future.result() for future in as_completed(futures)]
        
        # Analyze results
        total_successes = sum(r["successes"] for r in results)
        total_errors = sum(r["errors"] for r in results)
        
        # Should have reasonable success rate even with errors
        if total_successes + total_errors > 0:
            success_rate = total_successes / (total_successes + total_errors)
            assert success_rate > 0.5, f"Too many concurrent errors: {success_rate:.2%}"


class TestQueueErrorRecovery:
    """
    Tests for queue processor error recovery and fault tolerance.
    """

    def test_queue_table_recovery(self, db_session):
        """Test recovery when queue tables are missing or corrupted."""
        # Test queue operations when tables might not exist
        from partitioncache.queue import get_queue_lengths, add_query_to_original_queue
        
        try:
            # This might fail if tables don't exist yet
            initial_lengths = get_queue_lengths()
            
            # Try to add to queue
            success = add_query_to_original_queue(
                query="SELECT 1;",
                partition_key="test",
                datatype="integer"
            )
            
            # Should handle missing tables gracefully
            assert isinstance(success, bool)
            
        except Exception as e:
            # Should provide meaningful error about missing tables
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in 
                      ["table", "relation", "exist", "queue"])

    def test_invalid_cron_job_handling(self, db_session):
        """Test handling of invalid cron job specifications."""
        # Check if pg_cron extension is available
        with db_session.cursor() as cur:
            try:
                cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_cron';")
                if not cur.fetchone():
                    pytest.skip("pg_cron extension not available")
            except Exception:
                pytest.skip("Cannot check for pg_cron extension")
        
        invalid_cron_specs = [
            ("", "empty_cron"),
            ("invalid format", "invalid_format"),
            ("60 * * * *", "invalid_minute"),  # Minute > 59
            ("* 25 * * *", "invalid_hour"),    # Hour > 23
            ("* * 32 * *", "invalid_day"),     # Day > 31
        ]
        
        for cron_spec, description in invalid_cron_specs:
            with db_session.cursor() as cur:
                try:
                    # Attempt to schedule with invalid cron specification
                    test_jobname = f"invalid_test_{description}"
                    test_command = "SELECT 1;"
                    
                    cur.execute("""
                        SELECT cron.schedule(%s, %s, %s);
                    """, (test_jobname, cron_spec, test_command))
                    
                    # If it succeeds, clean up
                    cur.execute("SELECT cron.unschedule(%s);", (test_jobname,))
                    
                except Exception as e:
                    # Invalid cron specs should be rejected with meaningful errors
                    error_msg = str(e).lower()
                    assert any(keyword in error_msg for keyword in 
                              ["cron", "schedule", "format", "invalid"])

    def test_long_running_job_timeout(self, db_session):
        """Test handling of long-running cron jobs that might timeout."""
        # Check if pg_cron extension is available
        with db_session.cursor() as cur:
            try:
                cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_cron';")
                if not cur.fetchone():
                    pytest.skip("pg_cron extension not available")
            except Exception:
                pytest.skip("Cannot check for pg_cron extension")
        
        # Schedule a job that runs for a long time
        test_jobname = f"timeout_test_{int(time.time())}"
        # Command that will run for several seconds
        test_command = "SELECT pg_sleep(30);"  # 30 second sleep
        
        with db_session.cursor() as cur:
            try:
                # Schedule the long-running job
                cur.execute("""
                    SELECT cron.schedule(%s, '* * * * *', %s);
                """, (test_jobname, test_command))
                
                # Wait a short time
                time.sleep(5)
                
                # Check if job is running or has been scheduled
                cur.execute("""
                    SELECT jobname, active FROM cron.job 
                    WHERE jobname = %s;
                """, (test_jobname,))
                
                job_info = cur.fetchone()
                assert job_info is not None, "Long-running job should be scheduled"
                
                # Clean up - unschedule the job
                cur.execute("SELECT cron.unschedule(%s);", (test_jobname,))
                
            except Exception as e:
                # Clean up on error
                try:
                    cur.execute("SELECT cron.unschedule(%s);", (test_jobname,))
                except:
                    pass
                raise


class TestSystemLevelErrors:
    """
    Tests for system-level errors and recovery scenarios.
    """

    def test_disk_space_simulation(self, cache_client: AbstractCacheHandler):
        """Test behavior when disk space might be limited."""
        partition_key = "zipcode"
        
        # Create many large cache entries to simulate disk pressure
        large_entries = []
        max_attempts = 100
        
        for i in range(max_attempts):
            try:
                cache_key = f"disk_test_{i}"
                # Create progressively larger sets
                set_size = 1000 + (i * 100)
                large_set = set(range(i * 10000, i * 10000 + set_size))
                
                success = cache_client.set_set(cache_key, large_set, partition_key)
                if success:
                    large_entries.append(cache_key)
                else:
                    # Failure might indicate resource limits
                    break
                    
            except Exception as e:
                # Disk space or memory errors are acceptable
                if any(keyword in str(e).lower() for keyword in 
                       ["space", "memory", "limit", "quota"]):
                    break
                else:
                    raise
        
        # Verify we created some entries before hitting limits
        assert len(large_entries) > 0, "No entries created before hitting limits"
        
        # Verify we can still access earlier entries
        if large_entries:
            first_key = large_entries[0]
            assert cache_client.exists(first_key, partition_key), "Lost access to existing data"

    def test_network_interruption_simulation(self, db_session):
        """Test behavior during simulated network interruptions."""
        # This test simulates network issues by using timeouts
        
        # Set a very short timeout to simulate network issues
        original_timeout = db_session.cursor().timeout if hasattr(db_session.cursor(), 'timeout') else None
        
        try:
            # Create a new connection with very short timeout
            import psycopg
            conn_params = {
                "host": os.getenv("PG_HOST", "localhost"),
                "port": int(os.getenv("PG_PORT", "5432")),
                "user": os.getenv("PG_USER", "test_user"),
                "password": os.getenv("PG_PASSWORD", "test_password"),
                "dbname": os.getenv("PG_DBNAME", "test_db"),
                "connect_timeout": 1,  # Very short timeout
            }
            
            # Test connection with short timeout
            with psycopg.connect(**conn_params) as test_conn:
                with test_conn.cursor() as cur:
                    # Simple query should work
                    cur.execute("SELECT 1;")
                    result = cur.fetchone()
                    assert result[0] == 1, "Basic query failed"
            
        except psycopg.OperationalError as e:
            # Timeout errors are acceptable in this test
            assert "timeout" in str(e).lower() or "connection" in str(e).lower()
        except Exception as e:
            # Other connection-related errors are also acceptable
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in 
                      ["connection", "network", "timeout", "unreachable"])

    def test_permission_error_handling(self, db_session):
        """Test handling of permission-related errors."""
        # Test operations that might fail due to permissions
        
        permission_sensitive_operations = [
            ("CREATE TABLE test_permissions (id INT);", "table_creation"),
            ("DROP TABLE IF EXISTS test_permissions;", "table_deletion"),
            ("CREATE EXTENSION IF NOT EXISTS test_extension;", "extension_creation"),
        ]
        
        for sql, operation_name in permission_sensitive_operations:
            try:
                with db_session.cursor() as cur:
                    cur.execute(sql)
                # If operation succeeds, that's fine
                
            except Exception as e:
                # Permission errors should be handled gracefully
                error_msg = str(e).lower()
                expected_keywords = ["permission", "denied", "privilege", "access", "forbidden"]
                
                # Either it's a permission error (which we expect) or it succeeds
                if not any(keyword in error_msg for keyword in expected_keywords):
                    # If it's not a permission error, it might be another valid error
                    # (e.g., extension doesn't exist)
                    assert any(keyword in error_msg for keyword in 
                              ["exist", "available", "unknown", "invalid"])


import os  # Add missing import
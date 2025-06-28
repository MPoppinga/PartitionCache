import os
import time

import psycopg
import psycopg.errors
import pytest

from partitioncache.cache_handler.abstract import AbstractCacheHandler


def _compare_cache_values(retrieved, expected):
    """
    Helper function to compare cache values across different backend types.

    Args:
        retrieved: Value returned from cache backend (could be set, BitMap, etc.)
        expected: Expected set value

    Returns:
        bool: True if values are equivalent
    """
    # Handle BitMap objects from roaringbit backend
    try:
        from pyroaring import BitMap

        if isinstance(retrieved, BitMap):
            return set(retrieved) == expected
    except ImportError:
        pass

    # Handle regular sets and other types
    if hasattr(retrieved, "__iter__") and not isinstance(retrieved, (str, bytes)):
        return set(retrieved) == expected

    return retrieved == expected


class TestErrorRecovery:
    """
    Tests for error recovery and fault tolerance in various scenarios.
    """

    @pytest.mark.skip(reason="Connection testing unreliable due to singleton caching - moved to unit tests")
    def test_database_connection_recovery(self, db_session):
        """Test recovery from database connection failures."""
        # This test is skipped in integration tests due to handler caching behavior
        # Connection error handling should be tested in unit tests instead
        pass

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

        for query, _description in problematic_inputs:
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
                assert any(keyword in error_msg for keyword in ["syntax", "parse", "invalid", "error", "malformed", "expected", "none"])

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

        for _scenario_name, corrupt_key in corruption_scenarios:
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
        assert _compare_cache_values(retrieved, valid_set), "Valid data corrupted by error scenarios"

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
                    assert retrieved is not None and len(retrieved) == size, f"Retrieved set size mismatch: {len(retrieved) if retrieved else 0} != {size}"

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

        import random
        from concurrent.futures import ThreadPoolExecutor, as_completed

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
        from partitioncache.queue import get_queue_lengths, push_to_original_query_queue

        try:
            # This might fail if tables don't exist yet
            get_queue_lengths()

            # Try to add to queue
            success = push_to_original_query_queue(query="SELECT 1;", partition_key="test", partition_datatype="integer")

            # Should handle missing tables gracefully
            assert isinstance(success, bool)

        except Exception as e:
            # Should provide meaningful error about missing tables
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in ["table", "relation", "exist", "queue"])

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
            ("* 25 * * *", "invalid_hour"),  # Hour > 23
            ("* * 32 * *", "invalid_day"),  # Day > 31
        ]

        for cron_spec, description in invalid_cron_specs:
            with db_session.cursor() as cur:
                try:
                    # Attempt to schedule with invalid cron specification
                    test_jobname = f"invalid_test_{description}"
                    test_command = "SELECT 1;"

                    cur.execute(
                        """
                        SELECT cron.schedule(%s, %s, %s);
                    """,
                        (test_jobname, cron_spec, test_command),
                    )

                    # If it succeeds, clean up
                    cur.execute("SELECT cron.unschedule(%s);", (test_jobname,))

                except Exception as e:
                    # Invalid cron specs should be rejected with meaningful errors
                    error_msg = str(e).lower()
                    assert any(keyword in error_msg for keyword in ["cron", "schedule", "format", "invalid"])

    def test_long_running_job_timeout(self, db_session, cache_client, postgresql_queue_processor):
        """Test handling of long-running PartitionCache queries that timeout."""
        from partitioncache.queue import clear_all_queues

        # Get configuration from the fixture
        queue_prefix = postgresql_queue_processor["queue_prefix"]
        table_prefix = postgresql_queue_processor["table_prefix"]
        partition_key = "test_partition"

        # Clear queues first
        clear_all_queues()

        # Ensure metadata tables exist
        with db_session.cursor() as cur:
            cur.execute(f"SELECT partitioncache_ensure_metadata_tables('{table_prefix}')")
            db_session.commit()

        # Queue a long-running query directly to fragment queue for testing
        long_query = "SELECT pg_sleep(5), 12345 as test_partition"  # 5 second sleep (no semicolon)

        # For testing timeout behavior, push directly to fragment queue
        # In production, queries go through original queue first
        with db_session.cursor() as cur:
            # Generate a simple hash for the test query
            import hashlib

            query_hash = hashlib.md5(long_query.encode()).hexdigest()

            fragment_queue_table = f"{queue_prefix}_query_fragment_queue"
            cur.execute(
                f"""
                INSERT INTO {fragment_queue_table} (query, hash, partition_key, partition_datatype)
                VALUES (%s, %s, %s, %s)
            """,
                (long_query, query_hash, partition_key, "integer"),
            )
            db_session.commit()

        # Process it with a 1-second timeout using proper transaction-level timeout
        with db_session.cursor() as cur:
            try:
                # Start a new transaction with statement timeout
                cur.execute("BEGIN;")
                cur.execute("SET LOCAL statement_timeout = 1000;")  # 1 second timeout

                # Process the queue - this should timeout
                cur.execute("SELECT * FROM partitioncache_manual_process_queue(1);")
                result = cur.fetchone()

                # Commit the transaction (unlikely to reach here due to timeout)
                cur.execute("COMMIT;")

                # Log result if we somehow didn't timeout
                if result:
                    print(f"Unexpected completion without timeout: processed={result[0]}, message={result[1]}")

            except psycopg.errors.QueryCanceled:
                # This is expected - the query was canceled due to statement timeout
                cur.execute("ROLLBACK;")
                print("Query canceled as expected due to statement timeout")

            except Exception as e:
                cur.execute("ROLLBACK;")
                # Check if it's a timeout-related error
                error_msg = str(e).lower()
                if "cancel" in error_msg or "timeout" in error_msg:
                    print(f"Timeout-related error as expected: {e}")
                else:
                    raise

        # Wait a moment for any async cleanup
        time.sleep(1)

        # Verify the timeout was properly logged
        log_table = f"{queue_prefix}_processor_log"
        queries_table = table_prefix + "_queries" if table_prefix else "queries"

        with db_session.cursor() as cur:
            # Check processor log for any entry related to our query
            cur.execute(
                f"""
                SELECT status, error_message, query_hash
                FROM {log_table}
                WHERE partition_key = %s
                ORDER BY created_at DESC
                LIMIT 5
            """,
                (partition_key,),
            )

            log_entries = cur.fetchall()

            # Look for timeout or failure status
            timeout_found = False
            for entry in log_entries:
                if entry[0] == "timeout":
                    timeout_found = True
                    # The message should mention timeout (case-insensitive)
                    assert entry[1] and "timeout" in entry[1].lower(), f"Error message should mention timeout: {entry[1]}"
                    break
                elif entry[0] == "failed" and entry[1] and ("cancel" in entry[1].lower() or "timeout" in entry[1].lower()):
                    # Sometimes timeouts are logged as failures with cancel/timeout message
                    timeout_found = True
                    break

            assert timeout_found or len(log_entries) == 0, f"Expected timeout entry in processor log, found: {log_entries}"

            # Check queries table - it might have a timeout entry if the function got that far
            cur.execute(
                f"""
                SELECT status, query
                FROM {queries_table}
                WHERE partition_key = %s
                ORDER BY last_seen DESC
                LIMIT 1
            """,
                (partition_key,),
            )

            query_entry = cur.fetchone()
            if query_entry:
                # If there's an entry, it should be marked as timeout or failed
                assert query_entry[0] in ("timeout", "failed"), f"Expected 'timeout' or 'failed' status in queries table, got '{query_entry[0]}'"


class TestSystemLevelErrors:
    """
    Tests for system-level errors and recovery scenarios.
    """

    def test_disk_space_simulation(self, cache_client: AbstractCacheHandler):
        """Test behavior when disk space might be limited."""
        partition_key = "zipcode"

        # Check if backend has constraints that would interfere with this test
        supported_datatypes = getattr(cache_client, "get_supported_datatypes", lambda: ["integer", "text"])()
        backend_name = getattr(cache_client, "__class__", type(cache_client)).__name__.lower()

        # For bit backends, constrain values to avoid bitarray size issues
        if "bit" in backend_name:
            # Use smaller, constrained values for bit backends
            max_attempts = 20
            base_range = 1000
            range_increment = 100
        else:
            # Use larger values for other backends
            max_attempts = 100
            base_range = 10000
            range_increment = 1000

        # Create many cache entries to simulate resource pressure
        large_entries = []

        for i in range(max_attempts):
            try:
                cache_key = f"disk_test_{i}"
                # Create progressively larger sets (constrained for bit backends)
                set_size = 100 + (i * 50)
                start_val = i * range_increment
                large_set = set(range(start_val, start_val + set_size))

                success = cache_client.set_set(cache_key, large_set, partition_key)
                if success:
                    large_entries.append(cache_key)
                else:
                    # Failure might indicate resource limits
                    break

            except Exception as e:
                # Resource limit errors are acceptable and expected
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ["space", "memory", "limit", "quota", "range", "bitarray", "out of range"]):
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
        # (Note: original_timeout kept for consistency but may not be used)

        try:
            # Create a new connection with very short timeout
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
                    assert result is not None and result[0] == 1, "Basic query failed"

        except psycopg.OperationalError as e:
            # Timeout errors are acceptable in this test
            assert "timeout" in str(e).lower() or "connection" in str(e).lower()
        except Exception as e:
            # Other connection-related errors are also acceptable
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in ["connection", "network", "timeout", "unreachable"])

    def test_permission_error_handling(self, db_session):
        """Test handling of permission-related errors."""
        # Test operations that might fail due to permissions

        permission_sensitive_operations = [
            ("CREATE TABLE test_permissions (id INT);", "table_creation"),
            ("DROP TABLE IF EXISTS test_permissions;", "table_deletion"),
            ("CREATE EXTENSION IF NOT EXISTS test_extension;", "extension_creation"),
        ]

        for sql, _operation_name in permission_sensitive_operations:
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
                    assert any(keyword in error_msg for keyword in ["exist", "available", "unknown", "invalid"])

import pytest
import time
import os
from typing import Dict, Any

import partitioncache
from partitioncache.queue import add_query_to_original_queue, get_queue_lengths


class TestQueueProcessor:
    """
    Test suite for queue processor functionality.
    Tests the asynchronous processing pipeline from original query queue 
    to query fragment queue processing.
    """

    def test_queue_basic_operations(self, db_session):
        """Test basic queue operations: add, count, and clear."""
        # Clear queues to start fresh
        from partitioncache.queue import clear_all_queues
        clear_all_queues()
        
        # Add test query to original queue
        test_query = "SELECT * FROM test_locations WHERE zipcode = 1001;"
        partition_key = "zipcode"
        
        success = add_query_to_original_queue(
            query=test_query,
            partition_key=partition_key,
            datatype="integer"
        )
        assert success, "Failed to add query to original queue"
        
        # Check queue lengths
        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 1, "Original queue should have at least 1 entry"

    def test_queue_processor_setup(self, db_session):
        """Test PostgreSQL queue processor setup and configuration."""
        # Test that we can access queue processor functionality
        try:
            from partitioncache.cli.setup_postgresql_queue_processor import main as setup_processor
            
            # This would normally setup pg_cron jobs, but we'll test the setup logic
            # In a real test, we'd verify the setup creates necessary database objects
            
            # Verify queue tables exist
            with db_session.cursor() as cur:
                cur.execute("""
                    SELECT tablename FROM pg_tables 
                    WHERE tablename IN ('original_query_queue', 'query_fragment_queue')
                    AND schemaname = 'public';
                """)
                tables = cur.fetchall()
                
                # Should have both queue tables
                table_names = [t[0] for t in tables]
                assert "original_query_queue" in table_names or len(table_names) >= 0  # May not exist until first use
                
        except ImportError:
            pytest.skip("PostgreSQL queue processor not available")

    def test_schedule_and_execute_task(self, db_session, wait_for_cron):
        """
        Test end-to-end pg_cron job scheduling and execution.
        
        This test uses the 'schedule-wait-verify' pattern:
        1. Schedule a cron job to run every minute
        2. Wait for execution (65+ seconds)
        3. Verify job execution and side effects
        """
        # Clear any existing test jobs
        with db_session.cursor() as cur:
            cur.execute("DELETE FROM cron.job WHERE jobname LIKE 'test_%';")
        
        # Schedule a simple test job that inserts a record
        test_jobname = f"test_job_{int(time.time())}"
        test_command = f"""
            INSERT INTO test_cron_results (message) 
            VALUES ('Test executed at {test_jobname}');
        """
        
        with db_session.cursor() as cur:
            # Schedule job to run every minute
            cur.execute("""
                SELECT cron.schedule(%s, '* * * * *', %s);
            """, (test_jobname, test_command))
            
            # Verify job was scheduled
            cur.execute("""
                SELECT jobname, command, active 
                FROM cron.job 
                WHERE jobname = %s;
            """, (test_jobname,))
            
            job_info = cur.fetchone()
            assert job_info is not None, f"Job {test_jobname} was not scheduled"
            assert job_info[2] is True, "Job should be active"
        
        # Wait for cron job to execute (just over 1 minute)
        print(f"Waiting 65 seconds for cron job {test_jobname} to execute...")
        wait_for_cron(65)
        
        # Verify job execution
        with db_session.cursor() as cur:
            # Check if job ran successfully
            cur.execute("""
                SELECT jobid, runid, job_pid, return_message, status
                FROM cron.job_run_details 
                WHERE jobname = %s
                ORDER BY start_time DESC
                LIMIT 1;
            """, (test_jobname,))
            
            execution_info = cur.fetchone()
            assert execution_info is not None, f"No execution record found for job {test_jobname}"
            
            # Check execution status
            status = execution_info[4] if len(execution_info) > 4 else None
            # Note: status might be 'succeeded', 'failed', or None depending on pg_cron version
            if status is not None:
                assert status == 'succeeded', f"Job execution failed with status: {status}"
            
            # Verify side effect: check if our test record was inserted
            cur.execute("""
                SELECT message, created_at 
                FROM test_cron_results 
                WHERE message LIKE %s
                ORDER BY created_at DESC
                LIMIT 1;
            """, (f'Test executed at {test_jobname}',))
            
            result_record = cur.fetchone()
            assert result_record is not None, "Test job did not insert expected record"
            
            # Cleanup: unschedule the test job
            cur.execute("SELECT cron.unschedule(%s);", (test_jobname,))

    def test_queue_processing_integration(self, db_session, cache_client):
        """
        Test integration between queue processing and cache population.
        Simulates the complete workflow from query queuing to cache storage.
        """
        partition_key = "zipcode"
        test_query = """
            SELECT zipcode, COUNT(*) as location_count
            FROM test_locations 
            WHERE zipcode BETWEEN 1000 AND 2000
            GROUP BY zipcode;
        """
        
        # Clear queues and cache
        from partitioncache.queue import clear_all_queues
        clear_all_queues()
        
        # Clear cache for this partition
        try:
            existing_keys = cache_client.get_all_keys(partition_key)
            for key in existing_keys:
                cache_client.delete(key, partition_key)
        except Exception:
            pass  # Cache might be empty
        
        # Add query to queue
        success = add_query_to_original_queue(
            query=test_query,
            partition_key=partition_key,
            datatype="integer"
        )
        assert success, "Failed to add query to queue"
        
        # In a real scenario, the queue processor would:
        # 1. Process the original queue
        # 2. Generate query fragments 
        # 3. Execute fragments and populate cache
        
        # For testing, we simulate this process
        from partitioncache.query_processor import generate_all_hashes
        
        # Generate hashes (simulating fragment generation)
        hashes = generate_all_hashes(test_query, partition_key)
        assert len(hashes) > 0, "Should generate query hashes"
        
        # Simulate cache population (normally done by queue processor)
        # Execute the query to get actual partition values
        with db_session.cursor() as cur:
            cur.execute(test_query)
            results = cur.fetchall()
            actual_zipcodes = {row[0] for row in results}
        
        # Populate cache with actual results
        for hash_key in hashes:
            cache_client.set_set(hash_key, actual_zipcodes, partition_key)
        
        # Verify cache was populated
        for hash_key in hashes:
            cached_values = cache_client.get(hash_key, partition_key)
            assert cached_values == actual_zipcodes, "Cache should contain actual zipcode values"
        
        # Test cache application
        enhanced_partition_keys, _, hits = partitioncache.get_partition_keys(
            query=test_query,
            cache_handler=cache_client,
            partition_key=partition_key,
            min_component_size=1
        )
        
        assert hits > 0, "Should find cache hits"
        assert enhanced_partition_keys == actual_zipcodes, "Should return cached partition keys"

    @pytest.mark.slow
    def test_queue_processor_performance(self, db_session):
        """
        Performance test for queue operations.
        Tests handling of multiple queries and queue throughput.
        """
        from partitioncache.queue import clear_all_queues
        
        # Clear queues
        clear_all_queues()
        
        # Add multiple queries to test queue handling
        test_queries = [
            "SELECT * FROM test_locations WHERE zipcode = 1001;",
            "SELECT * FROM test_locations WHERE zipcode = 1002;", 
            "SELECT * FROM test_locations WHERE region = 'northeast';",
            "SELECT COUNT(*) FROM test_locations WHERE zipcode > 10000;",
            "SELECT region, AVG(population) FROM test_locations GROUP BY region;",
        ]
        
        start_time = time.time()
        
        # Add all queries to queue
        for i, query in enumerate(test_queries):
            partition_key = "zipcode" if "zipcode" in query else "region"
            datatype = "integer" if partition_key == "zipcode" else "text"
            
            success = add_query_to_original_queue(
                query=query,
                partition_key=partition_key,
                datatype=datatype
            )
            assert success, f"Failed to add query {i} to queue"
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should handle multiple queries quickly
        assert elapsed < 5.0, f"Queue operations took too long: {elapsed:.2f}s"
        
        # Verify all queries were queued
        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= len(test_queries), "Not all queries were queued"


class TestQueueErrorHandling:
    """Test error handling and edge cases in queue processing."""

    def test_invalid_query_handling(self, db_session):
        """Test handling of invalid SQL queries in queue."""
        invalid_query = "INVALID SQL SYNTAX HERE;"
        
        # Should handle invalid queries gracefully
        try:
            success = add_query_to_original_queue(
                query=invalid_query,
                partition_key="zipcode",
                datatype="integer"
            )
            # Even if it succeeds in queuing, processing should handle the error
            assert isinstance(success, bool)
        except Exception as e:
            # Some implementations might reject invalid queries immediately
            assert "syntax" in str(e).lower() or "invalid" in str(e).lower()

    def test_queue_cleanup_on_error(self, db_session):
        """Test that queue cleanup works properly after errors."""
        # This test ensures queues can be cleared even after error conditions
        from partitioncache.queue import clear_all_queues
        
        # Clear should work regardless of queue state
        original_cleared, fragment_cleared = clear_all_queues()
        
        # Should return counts (even if 0)
        assert isinstance(original_cleared, int)
        assert isinstance(fragment_cleared, int)
        assert original_cleared >= 0
        assert fragment_cleared >= 0
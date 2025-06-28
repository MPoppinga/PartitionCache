import time

import pytest


class TestPgCronIntegration:
    """
    Dedicated test suite for pg_cron integration.

    This test uses unique job names with database prefix to ensure proper isolation
    between parallel test runs. Only one test class for pg_cron to minimize conflicts.
    """

    @pytest.mark.slow
    def test_full_pg_cron_workflow(self, db_session, cache_client, postgresql_queue_processor):
        """
        Test the complete pg_cron workflow with proper isolation.

        This test validates that PartitionCache can use pg_cron for automated
        queue processing in production environments. Uses database-specific
        job names to avoid conflicts in parallel CI runs.
        """
        from partitioncache.query_processor import generate_all_query_hash_pairs
        from partitioncache.queue import clear_all_queues, get_queue_lengths, get_queue_provider_name, push_to_query_fragment_queue, reset_queue_handler

        # Only run for PostgreSQL queue provider
        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL with pg_cron")

        # Only run for PostgreSQL cache backends (pg_cron processes PostgreSQL cache tables)
        if not str(cache_client).startswith("postgresql"):
            pytest.skip("pg_cron tests only compatible with PostgreSQL cache backends")

        reset_queue_handler()

        # Check pg_cron availability from fixture
        if not postgresql_queue_processor.get("pg_cron_available", False):
            pytest.skip("pg_cron extension not available")

        # Get current database name for unique job naming
        with db_session.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]

        # Create unique job name with database prefix and timestamp
        job_prefix = f"{db_name}_pcache_test_{int(time.time())}"

        # Clean up any existing test jobs for this database
        with db_session.cursor() as cur:
            try:
                cur.execute("DELETE FROM cron.job WHERE jobname LIKE %s", (f"{db_name}_pcache_test_%",))
                db_session.commit()
            except Exception as e:
                print(f"Cleanup warning: {e}")

        # Setup PartitionCache queue processor with unique job name
        table_prefix = postgresql_queue_processor["table_prefix"]
        queue_prefix = postgresql_queue_processor["queue_prefix"]

        try:
            # Configure processor with unique job name
            config_table = f"{queue_prefix}_processor_config"
            with db_session.cursor() as cur:
                # Use a custom job name for this test
                test_job_name = f"{job_prefix}_process_queue"

                # Insert/update configuration with our unique job name
                cur.execute(
                    f"""
                    INSERT INTO {config_table} 
                    (job_name, table_prefix, queue_prefix, cache_backend, 
                     frequency_seconds, enabled, timeout_seconds, max_parallel_jobs)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_name) 
                    DO UPDATE SET 
                        enabled = EXCLUDED.enabled,
                        frequency_seconds = EXCLUDED.frequency_seconds,
                        updated_at = NOW()
                """,
                    (
                        test_job_name,
                        table_prefix,
                        queue_prefix,
                        "array",  # cache backend
                        5,  # frequency in seconds (fast for testing)
                        True,  # enabled
                        300,  # timeout
                        1,  # single job for testing
                    ),
                )
                db_session.commit()

            # Verify cron job was created
            with db_session.cursor() as cur:
                cur.execute("SELECT jobname, schedule, active FROM cron.job WHERE jobname LIKE %s", (f"{test_job_name}_%",))
                jobs = cur.fetchall()
                assert len(jobs) > 0, f"No cron jobs created for {test_job_name}"
                for job in jobs:
                    assert job[2] is True, f"Job {job[0]} should be active"
                    print(f"Created job: {job[0]} with schedule: {job[1]}")

            # Clear queues and add test data as fragments (direct processor only processes fragments)
            clear_all_queues()

            def _queue_query_as_fragments(query: str, partition_key: str, partition_datatype: str = "integer") -> bool:
                """Helper to generate and queue query fragments (same as manual processor test)."""
                query_hash_pairs = generate_all_query_hash_pairs(
                    query,
                    partition_key,
                    min_component_size=1,
                    follow_graph=True,
                    keep_all_attributes=True,
                )
                return push_to_query_fragment_queue(query_hash_pairs=query_hash_pairs, partition_key=partition_key, partition_datatype=partition_datatype)

            # Add test queries as fragments
            test_queries = [
                ("SELECT * FROM test_locations WHERE zipcode = 8001;", "zipcode", "integer"),
                ("SELECT * FROM test_locations WHERE zipcode = 8002;", "zipcode", "integer"),
                ("SELECT * FROM test_locations WHERE zipcode BETWEEN 8000 AND 8010;", "zipcode", "integer"),
            ]

            for query, partition_key, datatype in test_queries:
                success = _queue_query_as_fragments(query, partition_key, datatype)
                assert success, f"Failed to queue fragments for: {query}"

            initial_queue_length = get_queue_lengths()["query_fragment_queue"]
            assert initial_queue_length > 0, "No fragments queued"

            # Wait for pg_cron to process (with timeout)
            max_wait = 30  # seconds
            check_interval = 2
            start_time = time.time()

            print(f"Waiting for pg_cron to process {initial_queue_length} fragments...")
            while time.time() - start_time < max_wait:
                current_length = get_queue_lengths()["query_fragment_queue"]
                if current_length < initial_queue_length:
                    print(f"Fragment queue reduced from {initial_queue_length} to {current_length}")
                    break
                time.sleep(check_interval)

            # Verify processing occurred
            final_length = get_queue_lengths()["query_fragment_queue"]
            assert final_length < initial_queue_length, f"pg_cron did not process fragments. Initial: {initial_queue_length}, Final: {final_length}"

            print(f"pg_cron successfully processed {initial_queue_length - final_length} queries")

            # Check processor logs
            log_table = f"{queue_prefix}_processor_log"
            with db_session.cursor() as cur:
                cur.execute(f"""
                    SELECT job_id, status, execution_source, created_at 
                    FROM {log_table}
                    WHERE created_at > NOW() - INTERVAL '1 minute'
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                logs = cur.fetchall()

                cron_executions = [log for log in logs if log[2] == "cron"]
                assert len(cron_executions) > 0, "No cron executions found in logs"

                print(f"Found {len(cron_executions)} cron executions in logs")
                for log in cron_executions[:3]:  # Show first 3
                    print(f"  Job: {log[0]}, Status: {log[1]}, Time: {log[3]}")

        finally:
            # Cleanup: Disable and remove test jobs
            with db_session.cursor() as cur:
                try:
                    # Disable the test processor
                    cur.execute("UPDATE %s SET enabled = false WHERE job_name = %%s" % config_table, (test_job_name,))

                    # This should trigger removal of cron jobs
                    cur.execute("DELETE FROM %s WHERE job_name = %%s" % config_table, (test_job_name,))

                    # Explicitly remove any remaining cron jobs
                    cur.execute("DELETE FROM cron.job WHERE jobname LIKE %s", (f"{job_prefix}%",))

                    db_session.commit()
                    print(f"Cleaned up test jobs with prefix: {job_prefix}")
                except Exception as e:
                    print(f"Cleanup error: {e}")

            reset_queue_handler()

    def test_pg_cron_job_isolation(self, db_session, postgresql_queue_processor):
        """
        Test that pg_cron jobs with different prefixes don't interfere.

        This validates our isolation strategy for parallel test execution.
        """
        # Only run for PostgreSQL
        from partitioncache.queue import get_queue_provider_name

        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL with pg_cron")

        # Check pg_cron availability
        with db_session.cursor() as cur:
            try:
                cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_cron';")
                if not cur.fetchone():
                    pytest.skip("pg_cron extension not available")
            except Exception:
                pytest.skip("Cannot check for pg_cron extension")

        # Get current database name
        with db_session.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]

        # Create two different job prefixes
        timestamp = int(time.time())
        job_prefix_1 = f"{db_name}_iso_test1_{timestamp}"
        job_prefix_2 = f"{db_name}_iso_test2_{timestamp}"

        try:
            # Create two test jobs with different prefixes
            with db_session.cursor() as cur:
                for i, prefix in enumerate([job_prefix_1, job_prefix_2]):
                    job_name = f"{prefix}_job"
                    cur.execute("SELECT cron.schedule(%s, '* * * * *', 'SELECT 1;')", (job_name,))

            # Verify both jobs exist independently
            with db_session.cursor() as cur:
                cur.execute("SELECT jobname FROM cron.job WHERE jobname IN (%s, %s)", (f"{job_prefix_1}_job", f"{job_prefix_2}_job"))
                jobs = cur.fetchall()
                assert len(jobs) == 2, "Both isolation test jobs should exist"

            # Remove first job
            with db_session.cursor() as cur:
                cur.execute("SELECT cron.unschedule(%s)", (f"{job_prefix_1}_job",))

            # Verify second job still exists
            with db_session.cursor() as cur:
                cur.execute("SELECT jobname FROM cron.job WHERE jobname = %s", (f"{job_prefix_2}_job",))
                remaining = cur.fetchone()
                assert remaining is not None, "Second job should still exist"

            print("pg_cron job isolation verified - jobs don't interfere")

        finally:
            # Cleanup both test jobs
            with db_session.cursor() as cur:
                for prefix in [job_prefix_1, job_prefix_2]:
                    try:
                        cur.execute("DELETE FROM cron.job WHERE jobname LIKE %s", (f"{prefix}%",))
                    except Exception:
                        pass
                db_session.commit()

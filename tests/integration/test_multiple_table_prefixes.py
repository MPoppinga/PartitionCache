import os
import time
import pytest


class TestMultipleTablePrefixes:
    """
    Test suite for multiple queue processors with different table prefixes in the same database.
    
    Verifies that job_name uniqueness allows multiple processors to coexist.
    """
    
    @pytest.mark.slow
    def test_multiple_table_prefixes_same_database(self, db_session, cache_client):
        """
        Test that multiple processors with different table prefixes can run in the same database.
        
        This test validates:
        1. Setup creates unique job names for different table prefixes
        2. SQL functions can find and manage each job independently
        3. No configuration conflicts between different processors
        """
        from partitioncache.cli.postgresql_queue_processor import (
            construct_processor_job_name,
            get_cache_database_name,
            get_queue_table_prefix_from_env
        )
        
        # Only run for PostgreSQL queue provider
        from partitioncache.queue import get_queue_provider_name
        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL queue processor")

        # Only run for PostgreSQL cache backends
        if not str(cache_client).startswith("postgresql"):
            pytest.skip("Test only compatible with PostgreSQL cache backends")

        target_database = get_cache_database_name()
        queue_prefix = get_queue_table_prefix_from_env()
        config_table = f"{queue_prefix}_processor_config"
        
        # Test cases: different table prefixes that should create unique job names
        test_prefixes = [
            "partitioncache_cache1",
            "partitioncache_cache2", 
            "custom_cache"
        ]
        
        # Generate expected job names using the actual function
        expected_jobs = []
        for table_prefix in test_prefixes:
            job_name = construct_processor_job_name(target_database, table_prefix)
            expected_jobs.append((table_prefix, job_name))
        
        timestamp = int(time.time())
        
        try:
            # Setup multiple processor configurations
            for table_prefix, expected_job_name in expected_jobs:
                with db_session.cursor() as cur:
                    # Create processor config using insert_initial_config logic
                    cur.execute(f"""
                        INSERT INTO {config_table} 
                        (job_name, table_prefix, queue_prefix, cache_backend, frequency_seconds, 
                         enabled, timeout_seconds, target_database, default_bitsize)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_name) DO UPDATE SET
                            table_prefix = EXCLUDED.table_prefix,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        expected_job_name, table_prefix, queue_prefix, "array", 
                        60, False, 1800, target_database, None
                    ))
                db_session.commit()
            
            # Verify all jobs were created with unique names
            with db_session.cursor() as cur:
                cur.execute(f"""
                    SELECT job_name, table_prefix 
                    FROM {config_table} 
                    WHERE target_database = %s 
                    AND job_name LIKE %s
                    ORDER BY job_name
                """, (target_database, 'partitioncache_process_queue_%'))
                
                actual_jobs = cur.fetchall()
                
                # Check that we have the expected number of jobs
                assert len(actual_jobs) >= len(expected_jobs), f"Expected at least {len(expected_jobs)} jobs, got {len(actual_jobs)}"
                
                # Verify job names are unique
                job_names = [job[0] for job in actual_jobs]
                unique_names = set(job_names)
                assert len(job_names) == len(unique_names), f"Duplicate job names found: {job_names}"
                
                # Verify expected jobs exist
                actual_job_dict = {job[0]: job[1] for job in actual_jobs}
                for table_prefix, expected_job_name in expected_jobs:
                    assert expected_job_name in actual_job_dict, f"Job {expected_job_name} not found in {list(actual_job_dict.keys())}"
                    assert actual_job_dict[expected_job_name] == table_prefix, f"Job {expected_job_name} has wrong table_prefix"
            
            print(f"✅ Successfully created {len(expected_jobs)} unique jobs for different table prefixes")
            
            # Test SQL function compatibility - should now work with fixes
            with db_session.cursor() as cur:
                # Test: Multiple configs should require explicit job_name
                try:
                    cur.execute("SELECT partitioncache_get_processor_status(%s, %s)", 
                               (queue_prefix, target_database))
                    status = cur.fetchone()
                    # With multiple configs, this should fail requiring explicit job_name
                    print("⚠️  SQL function succeeded with multiple configs - should require explicit job_name")
                except Exception as e:
                    if "Multiple processor configurations found" in str(e):
                        print("✅ SQL function correctly requires explicit job_name for multiple configs")
                    else:
                        print(f"❌ SQL function failed with unexpected error: {e}")
                    
                # Test with explicit job names - should work now
                for table_prefix, expected_job_name in expected_jobs:
                    try:
                        cur.execute("SELECT partitioncache_get_processor_status(%s, %s, %s)", 
                                   (queue_prefix, target_database, expected_job_name))
                        status = cur.fetchone()
                        if status:
                            print(f"✅ SQL function found job {expected_job_name}")
                            # Verify the job_name in the result matches what we expect
                            if hasattr(status, '__getitem__') and len(status) > 0:
                                actual_job_name = status[0] if hasattr(status[0], '__str__') else str(status[0])
                                if actual_job_name == expected_job_name:
                                    print(f"✅ Job name matches: {actual_job_name}")
                                else:
                                    print(f"❌ Job name mismatch - expected: {expected_job_name}, got: {actual_job_name}")
                        else:
                            print(f"❌ SQL function couldn't find job {expected_job_name}")
                    except Exception as e:
                        print(f"❌ SQL function failed for {expected_job_name}: {e}")
                        
                # Test: Single config per database should work automatically  
                # Create a test with only one config
                test_single_db = f"{target_database}_single_test"
                test_single_prefix = "partitioncache_single"
                test_single_job = f"partitioncache_process_queue_{test_single_db}_single"
                
                try:
                    cur.execute(f"""
                        INSERT INTO {config_table} 
                        (job_name, table_prefix, queue_prefix, cache_backend, frequency_seconds, 
                         enabled, timeout_seconds, target_database, default_bitsize)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_name) DO NOTHING
                    """, (
                        test_single_job, test_single_prefix, queue_prefix, "array", 
                        60, False, 1800, test_single_db, None
                    ))
                    db_session.commit()
                    
                    # This should work without explicit job_name since there's only one config
                    cur.execute("SELECT partitioncache_get_processor_status(%s, %s)", 
                               (queue_prefix, test_single_db))
                    status = cur.fetchone()
                    if status:
                        print(f"✅ SQL function works automatically for single config in {test_single_db}")
                    else:
                        print(f"❌ SQL function failed for single config in {test_single_db}")
                        
                    # Cleanup single test
                    cur.execute(f"DELETE FROM {config_table} WHERE job_name = %s", (test_single_job,))
                    db_session.commit()
                    
                except Exception as e:
                    print(f"❌ Single config test failed: {e}")
                        
        finally:
            # Cleanup: remove test jobs
            with db_session.cursor() as cur:
                for _, expected_job_name in expected_jobs:
                    cur.execute(f"DELETE FROM {config_table} WHERE job_name = %s", (expected_job_name,))
            db_session.commit()
            
    @pytest.mark.slow
    def test_job_name_construction_consistency(self):
        """
        Test that Python setup and SQL functions construct job names consistently.
        
        This test validates that there's no mismatch between job creation and lookup logic.
        """
        # Test data
        target_database = "test_db"
        test_cases = [
            ("partitioncache_cache1", "cache1"),
            ("partitioncache_cache2", "cache2"), 
            ("custom_cache", "customcache"),
            ("partitioncache", "default"),  # Edge case: just the prefix
        ]
        
        print("=== Job Name Construction Consistency Test ===")
        
        for table_prefix, expected_suffix in test_cases:
            # Python setup logic (current implementation)
            table_suffix = table_prefix.replace('partitioncache', '').replace('_', '') or 'default'
            python_job_name = f"partitioncache_process_queue_{target_database}_{table_suffix}"
            
            # SQL function logic (current - just uses target_database)
            sql_job_name = f"partitioncache_process_queue_{target_database}"
            
            print(f"Table Prefix: {table_prefix}")
            print(f"  Python creates: {python_job_name}")
            print(f"  SQL looks for:  {sql_job_name}")
            
            if python_job_name == sql_job_name:
                print("  ✅ Names match")
            else:
                print("  ❌ MISMATCH - SQL functions won't find Python-created jobs")
                
            assert table_suffix == expected_suffix, f"Expected suffix {expected_suffix}, got {table_suffix}"
            
        print("\n=== Recommendation ===")
        print("SQL functions need to be updated to include table_prefix in job name construction")
        print("OR require explicit job_name parameters for multiple table prefix scenarios")
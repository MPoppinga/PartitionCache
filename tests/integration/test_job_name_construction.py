import pytest


class TestJobNameConstruction:
    """
    Test suite for verifying job name construction consistency between Python and SQL.
    
    This ensures that the SQL helper function matches Python logic exactly.
    """
    
    def test_sql_helper_function_consistency(self, db_session):
        """
        Test that SQL helper function produces same results as Python logic.
        """
        # Test cases covering various table_prefix patterns
        test_cases = [
            ("partitioncache_cache1", "test_db", "partitioncache_process_queue_test_db_cache1"),
            ("partitioncache_cache2", "test_db", "partitioncache_process_queue_test_db_cache2"),
            ("custom_cache", "test_db", "partitioncache_process_queue_test_db_customcache"),
            ("partitioncache", "test_db", "partitioncache_process_queue_test_db_default"),
            ("partitioncache_", "test_db", "partitioncache_process_queue_test_db_default"),
            ("partitioncache_test_long_name", "myapp", "partitioncache_process_queue_myapp_testlongname"),
        ]
        
        for table_prefix, target_database, expected_job_name in test_cases:
            with db_session.cursor() as cur:
                # Test SQL helper function
                cur.execute("SELECT partitioncache_construct_job_name(%s, %s)", 
                           (target_database, table_prefix))
                sql_result = cur.fetchone()[0]
                
                # Test Python logic (same as in handle_setup)
                table_suffix = table_prefix.replace('partitioncache', '').replace('_', '') or 'default'
                python_result = f"partitioncache_process_queue_{target_database}_{table_suffix}"
                
                print(f"Table Prefix: {table_prefix}")
                print(f"  SQL result:    {sql_result}")
                print(f"  Python result: {python_result}")
                print(f"  Expected:      {expected_job_name}")
                
                # Verify SQL matches Python
                assert sql_result == python_result, f"SQL/Python mismatch for {table_prefix}: SQL={sql_result}, Python={python_result}"
                
                # Verify both match expected
                assert sql_result == expected_job_name, f"SQL result mismatch for {table_prefix}: got {sql_result}, expected {expected_job_name}"
                assert python_result == expected_job_name, f"Python result mismatch for {table_prefix}: got {python_result}, expected {expected_job_name}"
                
    def test_sql_helper_function_edge_cases(self, db_session):
        """
        Test edge cases for job name construction.
        """
        edge_cases = [
            # (table_prefix, target_database, description)
            (None, "test_db", "NULL table_prefix should use simple naming"),
            ("", "test_db", "Empty table_prefix should use simple naming"),
            ("partitioncache_cache1", "db_with_underscores", "Database names with underscores"),
            ("partitioncache_cache1", "db-with-hyphens", "Database names with hyphens"),
        ]
        
        for table_prefix, target_database, description in edge_cases:
            with db_session.cursor() as cur:
                print(f"Testing: {description}")
                print(f"  Input: table_prefix={table_prefix}, target_database={target_database}")
                
                try:
                    cur.execute("SELECT partitioncache_construct_job_name(%s, %s)", 
                               (target_database, table_prefix))
                    result = cur.fetchone()[0]
                    print(f"  Result: {result}")
                    
                    # Basic validation
                    assert result is not None, "Result should not be NULL"
                    assert result.startswith("partitioncache_process_queue_"), "Result should start with expected prefix"
                    assert target_database in result, "Result should contain target database name"
                    
                    # For NULL/empty table_prefix, should use simple naming (no table suffix)
                    if table_prefix is None or table_prefix == "":
                        expected_simple = f"partitioncache_process_queue_{target_database}"
                        assert result == expected_simple, f"Expected simple naming for NULL/empty prefix: {expected_simple}, got {result}"
                    
                except Exception as e:
                    pytest.fail(f"SQL helper function failed for {description}: {e}")
                    
    def test_job_name_uniqueness_across_scenarios(self, db_session):
        """
        Test that different table prefixes always produce unique job names.
        """
        target_database = "test_uniqueness"
        
        # Various table prefixes that should all produce unique job names
        table_prefixes = [
            "partitioncache_cache1",
            "partitioncache_cache2",
            "partitioncache_test",
            "partitioncache_prod", 
            "partitioncache_staging",
            "custom_cache",
            "another_prefix",
            "partitioncache",  # Edge case - just the base prefix
        ]
        
        job_names = set()
        
        for table_prefix in table_prefixes:
            with db_session.cursor() as cur:
                cur.execute("SELECT partitioncache_construct_job_name(%s, %s)", 
                           (target_database, table_prefix))
                job_name = cur.fetchone()[0]
                
                print(f"Table Prefix: {table_prefix:25} -> Job Name: {job_name}")
                
                # Check for duplicates
                assert job_name not in job_names, f"Duplicate job name detected: {job_name} (from prefix: {table_prefix})"
                job_names.add(job_name)
                
        print(f"✅ Generated {len(job_names)} unique job names from {len(table_prefixes)} table prefixes")
        assert len(job_names) == len(table_prefixes), "All job names should be unique"
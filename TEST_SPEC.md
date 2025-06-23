# Integration Test Specification

This document provides a comprehensive overview of the integration test suite for PartitionCache. Each test validates specific functionality and expected behavior.

## Test Structure Overview

The test suite is organized into two main categories:

### Unit Tests (`tests/pytest/`)
Traditional unit tests covering individual components and functions.

### Integration Tests (`tests/integration/`)
Comprehensive integration tests organized into six main test modules:

- **test_cache_backends.py** - Tests all cache backend implementations and core functionality
- **test_queue_processor.py** - Tests asynchronous queue processing and pg_cron functionality  
- **test_cli.py** - Tests command-line interface functionality
- **test_cache_performance.py** - Performance, concurrency, and stress tests
- **test_error_recovery.py** - Error handling and fault tolerance tests
- **test_end_to_end_workflows.py** - Complete workflow tests simulating real-world usage

## Cache Backend Tests (test_cache_backends.py)

### TestCacheLifecycle

**Test: test_set_and_get_integer_partition**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that integer partition values can be stored in and retrieved from cache backends.
Actions:
1. Store a set of integer values (zipcodes) in cache using set_set()
2. Retrieve the same values using get()
3. Verify retrieved values match stored values exactly
Expected Outcome: Cache operations succeed and retrieved values equal stored values.

**Test: test_set_and_get_text_partition**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that text partition values can be stored in and retrieved from cache backends.
Actions:
1. Store a set of text values (region names) in cache using set_set()
2. Retrieve the same values using get()
3. Verify retrieved values match stored values exactly
Expected Outcome: Cache operations succeed and retrieved text values equal stored values.

**Test: test_get_miss**
File: tests/integration/test_cache_backends.py
Purpose: Verifies cache miss behavior when attempting to retrieve non-existent keys.
Actions:
1. Attempt to retrieve a key that doesn't exist in cache
2. Verify the return value is None
Expected Outcome: Cache returns None for non-existent keys.

**Test: test_exists_check**
File: tests/integration/test_cache_backends.py
Purpose: Verifies the key existence checking functionality works correctly.
Actions:
1. Check that a key doesn't exist before storing
2. Store values in cache for that key
3. Verify the key now exists
Expected Outcome: exists() returns False before storing, True after storing.

**Test: test_update_value**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that existing cache entries can be updated with new values.
Actions:
1. Store initial set of values in cache
2. Update the same key with an expanded set of values
3. Verify the updated values are retrieved correctly
Expected Outcome: Cache entry is successfully updated with new values.

**Test: test_delete_value**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that cache entries can be completely removed from the cache.
Actions:
1. Store values in cache and verify existence
2. Delete the cache entry using delete()
3. Verify the key no longer exists and returns None
Expected Outcome: Cache entry is completely removed and no longer accessible.

**Test: test_null_value_handling**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that null values can be stored and properly identified in cache.
Actions:
1. Store a null value using set_null()
2. Check null status using is_null()
3. Verify the key exists but get() returns None
Expected Outcome: Null values are properly stored and identified, with get() returning None.

**Test: test_intersect_multiple_keys**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that the cache can efficiently intersect values from multiple cache entries.
Actions:
1. Store overlapping sets of values in multiple cache entries
2. Request intersection of two keys
3. Request intersection of all three keys
4. Verify intersection results and hit counts
Expected Outcome: Intersections return only common values across requested keys with correct hit counts.

**Test: test_filter_existing_keys**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that the cache can filter a set of keys to identify which ones exist.
Actions:
1. Store values for some keys but not others
2. Request filtering of a mixed set of existing and non-existing keys
3. Verify only existing keys are returned
Expected Outcome: Only keys that actually exist in cache are returned in the filtered set.

**Test: test_get_all_keys**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that all keys for a partition can be retrieved.
Actions:
1. Store multiple cache entries for a partition
2. Request all keys for that partition
3. Verify all stored keys are present in the result
Expected Outcome: All stored keys are returned in the key list.

**Test: test_empty_set_handling**
File: tests/integration/test_cache_backends.py
Purpose: Verifies that cache backends handle empty sets appropriately.
Actions:
1. Attempt to store an empty set in cache
2. If successful, verify retrieval behavior
3. Handle backends that don't support empty sets gracefully
Expected Outcome: Empty sets are handled consistently (stored and retrieved, or operation skipped).

### TestCacheIntegration

**Test: test_partition_cache_workflow**
File: tests/integration/test_cache_backends.py
Purpose: Verifies the complete PartitionCache workflow from query processing to cache application.
Actions:
1. Generate query hashes using generate_all_hashes()
2. Populate cache with test partition values
3. Use get_partition_keys() API to retrieve cached values
4. Verify the complete workflow returns expected results
Expected Outcome: Complete workflow successfully retrieves cached partition values with proper hit counts.

**Test: test_apply_cache_lazy_integration**
File: tests/integration/test_cache_backends.py
Purpose: Verifies lazy cache application with actual database queries.
Actions:
1. Generate and populate cache with query hashes
2. Use apply_cache_lazy() to enhance the original query
3. Verify the enhanced query contains cache restrictions
4. Check that statistics show cache hits and enhancement
Expected Outcome: Lazy cache application successfully enhances queries with cache-based restrictions.

## Queue Processor Tests (test_queue_processor.py)

### TestQueueProcessor

**Test: test_queue_basic_operations**
File: tests/integration/test_queue_processor.py
Purpose: Verifies basic queue operations including adding queries and checking queue lengths.
Actions:
1. Clear all queues to start fresh
2. Add a test query to the original queue
3. Check queue lengths to verify the query was added
Expected Outcome: Query is successfully added to the original queue and queue length reflects the addition.

**Test: test_queue_processor_setup**
File: tests/integration/test_queue_processor.py
Purpose: Verifies that PostgreSQL queue processor setup and configuration works correctly.
Actions:
1. Import and test queue processor setup functionality
2. Verify that required queue tables exist or can be created
3. Check that setup logic executes without errors
Expected Outcome: Queue processor setup completes successfully and required database objects are available.

**Test: test_schedule_and_execute_task**
File: tests/integration/test_queue_processor.py
Purpose: Verifies end-to-end pg_cron job scheduling and execution functionality.
Actions:
1. Clear any existing test cron jobs
2. Schedule a test job to run every minute using cron.schedule()
3. Wait 65 seconds for the job to execute
4. Verify job execution in cron.job_run_details with 'succeeded' status
5. Verify side effect by checking that test record was inserted
6. Clean up by unscheduling the test job
Expected Outcome: The cron job is successfully scheduled, executes within the expected timeframe, shows 'succeeded' status, and produces the expected side effect (test record insertion).

**Test: test_queue_processing_integration**
File: tests/integration/test_queue_processor.py
Purpose: Verifies integration between queue processing and cache population workflows.
Actions:
1. Clear queues and cache to start fresh
2. Add a test query to the original queue
3. Simulate query fragment generation using generate_all_hashes()
4. Execute the query against the database to get actual results
5. Populate cache with the actual results (simulating queue processor)
6. Test cache application using get_partition_keys()
Expected Outcome: Complete integration workflow successfully processes queued queries, populates cache with actual data, and enables successful cache application.

**Test: test_queue_processor_performance**
File: tests/integration/test_queue_processor.py
Purpose: Verifies queue performance with multiple queries and measures throughput.
Actions:
1. Clear queues and add multiple test queries with different partition keys
2. Measure time taken for all queue operations
3. Verify all queries were successfully queued
4. Check that operations complete within reasonable time limits
Expected Outcome: Multiple queries are processed quickly (under 5 seconds) and all queries are successfully added to the queue.

### TestQueueErrorHandling

**Test: test_invalid_query_handling**
File: tests/integration/test_queue_processor.py
Purpose: Verifies that invalid SQL queries are handled gracefully by the queue system.
Actions:
1. Attempt to add a query with invalid SQL syntax to the queue
2. Verify the system handles the error appropriately
3. Check that error messages are meaningful
Expected Outcome: Invalid queries are either rejected with appropriate error messages or queued for later error handling during processing.

**Test: test_queue_cleanup_on_error**
File: tests/integration/test_queue_processor.py
Purpose: Verifies that queue cleanup operations work properly even after error conditions.
Actions:
1. Perform queue clear operation regardless of current queue state
2. Verify that clear operations return valid counts
3. Ensure cleanup works even after errors
Expected Outcome: Queue cleanup operations succeed and return non-negative integer counts for cleared items.

## CLI Tests (test_cli.py)

### TestCLICommands

**Test: test_pcache_manage_help**
File: tests/integration/test_cli.py
Purpose: Verifies that the main pcache-manage command displays help information correctly.
Actions:
1. Execute pcache-manage --help as a subprocess
2. Verify the command returns success (exit code 0)
3. Check that help output contains expected keywords
Expected Outcome: Help command succeeds and displays information about PartitionCache Management Tool with setup and cache options.

**Test: test_pcache_manage_status**
File: tests/integration/test_cli.py
Purpose: Verifies that the status command provides useful environment and configuration information.
Actions:
1. Set required environment variables for database connection
2. Execute pcache-manage status command
3. Verify output contains environment or configuration information
Expected Outcome: Status command succeeds and provides information about environment validation or configuration status.

**Test: test_pcache_manage_setup_cache**
File: tests/integration/test_cli.py
Purpose: Verifies that cache metadata setup can be performed via CLI.
Actions:
1. Set required database environment variables
2. Execute pcache-manage setup cache command
3. Verify setup completes successfully
Expected Outcome: Cache setup command succeeds and reports successful completion.

**Test: test_pcache_manage_cache_count**
File: tests/integration/test_cli.py
Purpose: Verifies that cache counting functionality works through the CLI.
Actions:
1. Set required environment variables
2. Execute pcache-manage cache count command
3. Verify output contains numerical information
Expected Outcome: Cache count command succeeds and displays numerical statistics about cache contents.

**Test: test_pcache_add_help**
File: tests/integration/test_cli.py
Purpose: Verifies that pcache-add command displays appropriate help information.
Actions:
1. Execute pcache-add --help as subprocess
2. Verify command succeeds and contains query and partition-related help
Expected Outcome: Help displays information about query and partition parameters.

**Test: test_pcache_add_direct_mode**
File: tests/integration/test_cli.py
Purpose: Verifies that queries can be added directly to cache via CLI.
Actions:
1. Create temporary SQL file with test query
2. Execute pcache-add with direct mode, query file, and partition parameters
3. Verify command succeeds or provides meaningful configuration errors
4. Clean up temporary file
Expected Outcome: Query addition succeeds or fails with clear configuration-related error messages.

**Test: test_pcache_read_help**
File: tests/integration/test_cli.py
Purpose: Verifies that pcache-read command provides appropriate help information.
Actions:
1. Execute pcache-read --help
2. Verify help contains partition and key-related information
Expected Outcome: Help command succeeds and shows information about partition and key parameters.

**Test: test_pcache_monitor_help**
File: tests/integration/test_cli.py
Purpose: Verifies that pcache-monitor command displays help information.
Actions:
1. Execute pcache-monitor --help
2. Verify output contains monitor or queue-related information
Expected Outcome: Help command succeeds and displays monitoring or queue-related help text.

**Test: test_pcache_postgresql_queue_processor_help**
File: tests/integration/test_cli.py
Purpose: Verifies that PostgreSQL queue processor command shows help information.
Actions:
1. Execute pcache-postgresql-queue-processor --help
2. Verify output contains PostgreSQL or queue-related information
Expected Outcome: Help command succeeds and displays PostgreSQL queue processor help text.

**Test: test_pcache_postgresql_eviction_manager_help**
File: tests/integration/test_cli.py
Purpose: Verifies that PostgreSQL eviction manager command displays help information.
Actions:
1. Execute pcache-postgresql-eviction-manager --help
2. Verify output contains eviction or cache-related information
Expected Outcome: Help command succeeds and displays eviction management help text.

### TestCLIIntegration

**Test: test_complete_cache_workflow**
File: tests/integration/test_cli.py
Purpose: Verifies complete cache workflow using multiple CLI commands in sequence.
Actions:
1. Set database environment variables
2. Execute cache setup command
3. Execute cache count command to verify setup
4. Verify both commands succeed or provide meaningful errors
Expected Outcome: Complete workflow succeeds with cache setup and subsequent operations working correctly.

**Test: test_queue_workflow**
File: tests/integration/test_cli.py
Purpose: Verifies queue-related CLI operations work in sequence.
Actions:
1. Set database environment variables for queue operations
2. Execute queue setup command
3. Execute queue count command
4. Verify operations succeed or provide meaningful errors
Expected Outcome: Queue setup and counting operations succeed and provide useful information.

**Test: test_cache_export_import_workflow**
File: tests/integration/test_cli.py
Purpose: Verifies cache export and import functionality works correctly.
Actions:
1. Create temporary export file
2. Execute cache export command
3. If export succeeds, verify file exists and execute import command
4. Clean up temporary file
5. Verify operations succeed or provide configuration errors
Expected Outcome: Export creates a file and import processes it successfully, or both operations provide clear configuration-related errors.

### TestCLIErrorHandling

**Test: test_invalid_command_arguments**
File: tests/integration/test_cli.py
Purpose: Verifies CLI handles invalid command arguments gracefully.
Actions:
1. Execute CLI command with invalid subcommand
2. Verify command fails with appropriate error message
Expected Outcome: Command fails with error message indicating invalid or unrecognized arguments.

**Test: test_missing_required_args**
File: tests/integration/test_cli.py
Purpose: Verifies CLI properly reports missing required arguments.
Actions:
1. Execute pcache-add command without required arguments
2. Verify command fails with error about missing arguments
Expected Outcome: Command fails and error message indicates missing required arguments.

**Test: test_invalid_file_paths**
File: tests/integration/test_cli.py
Purpose: Verifies CLI handles invalid file paths appropriately.
Actions:
1. Execute command with non-existent file path
2. Verify command fails with file-related error message
Expected Outcome: Command fails with error message about file not found or invalid path.

**Test: test_cli_with_missing_env_vars**
File: tests/integration/test_cli.py
Purpose: Verifies CLI behavior when required environment variables are missing.
Actions:
1. Execute CLI command with minimal environment (no database configs)
2. Verify command fails with configuration-related error
Expected Outcome: Command fails gracefully with error message about configuration, environment, or connection issues.

### TestCLIPerformance

**Test: test_cli_response_time**
File: tests/integration/test_cli.py
Purpose: Verifies that CLI commands respond within reasonable time limits.
Actions:
1. Measure time for CLI help command execution
2. Verify command succeeds and completes quickly
Expected Outcome: CLI help command succeeds and completes in under 5 seconds.

**Test: test_multiple_cli_calls**
File: tests/integration/test_cli.py
Purpose: Verifies consistency across multiple CLI command executions.
Actions:
1. Execute multiple different CLI help commands
2. Verify all commands succeed and produce output
3. Check for consistency across commands
Expected Outcome: All CLI commands succeed consistently and produce non-empty output.

## Test Environment and Setup

### Database Schema
The tests use a simplified schema based on the OSM example:
- `test_locations` table with zipcode (integer) and region (text) partition keys
- Sample data covering multiple zipcodes and regions
- `test_cron_results` table for pg_cron job validation

### Cache Backends Tested
- postgresql_array (primary backend)
- postgresql_bit (bit-based storage)
- postgresql_roaringbit (roaring bitmap storage)
- redis_set (if Redis available)
- redis_bit (if Redis available)

### Test Data
- Integer partition keys: 1001, 1002, 90210, 10001
- Text partition keys: "northeast", "west", "southeast"
- Sample queries covering simple filters, ranges, and complex joins

## Performance Tests (test_cache_performance.py)

### TestCachePerformance

**Test: test_large_set_operations**
File: tests/integration/test_cache_performance.py
Purpose: Validates performance with large sets of partition values (10,000+ items).
Actions:
1. Generate a large set of 10,000 partition values
2. Measure time for set_set() operation
3. Measure time for get() operation
4. Verify data integrity and performance thresholds
Expected Outcome: Large set operations complete within 10s for set, 5s for get, with correct data retrieval.

**Test: test_many_small_operations**
File: tests/integration/test_cache_performance.py
Purpose: Tests throughput with many small cache operations.
Actions:
1. Perform 1,000 small set operations with 2-element sets
2. Measure total execution time and calculate operations per second
3. Verify minimum throughput requirements
Expected Outcome: Achieves at least 50 operations per second for small cache operations.

**Test: test_intersection_performance**
File: tests/integration/test_cache_performance.py
Purpose: Validates performance of set intersection operations with multiple overlapping sets.
Actions:
1. Create 50 cache entries with 1,000 elements each and 80% overlap
2. Measure time for get_intersected() operation across all sets
3. Verify intersection correctness and performance
Expected Outcome: Intersection of 50 sets completes within 5 seconds with correct results.

### TestCacheConcurrency

**Test: test_concurrent_writes**
File: tests/integration/test_cache_performance.py
Purpose: Validates thread safety with concurrent write operations from multiple threads.
Actions:
1. Launch 10 threads, each performing 50 write operations
2. Monitor success rates across all concurrent operations
3. Verify data consistency after concurrent writes
Expected Outcome: At least 95% of concurrent write operations succeed without data corruption.

**Test: test_concurrent_read_write**
File: tests/integration/test_cache_performance.py
Purpose: Tests mixed concurrent read and write operations for race conditions.
Actions:
1. Pre-populate shared cache entries
2. Launch 4 reader threads and 4 writer threads simultaneously
3. Monitor success rates for both read and write operations
Expected Outcome: Read operations achieve >87.5% success rate, write operations >90% success rate.

## Error Recovery Tests (test_error_recovery.py)

### TestErrorRecovery

**Test: test_database_connection_recovery**
File: tests/integration/test_error_recovery.py
Purpose: Verifies graceful handling of database connection failures.
Actions:
1. Modify connection parameters to simulate connection failure
2. Attempt to create cache handler with invalid parameters
3. Verify informative error messages are provided
4. Restore valid connection parameters
Expected Outcome: Connection failures produce meaningful error messages containing "connection" or "host" keywords.

**Test: test_malformed_query_handling**
File: tests/integration/test_error_recovery.py
Purpose: Tests handling of malformed SQL queries in cache operations.
Actions:
1. Test various problematic inputs (empty strings, syntax errors, incomplete SQL)
2. Attempt hash generation and cache operations with invalid queries
3. Verify error messages are meaningful when operations fail
Expected Outcome: Malformed queries either process gracefully or fail with meaningful error messages.

**Test: test_cache_corruption_recovery**
File: tests/integration/test_error_recovery.py
Purpose: Validates recovery from simulated cache corruption scenarios.
Actions:
1. Store valid cache data
2. Attempt operations with corrupt keys (null, empty, very long)
3. Verify valid data remains accessible after corruption attempts
Expected Outcome: Valid cache data remains accessible despite corruption attempts on other entries.

## End-to-End Workflow Tests (test_end_to_end_workflows.py)

### TestEndToEndWorkflows

**Test: test_complete_osm_style_workflow**
File: tests/integration/test_end_to_end_workflows.py
Purpose: Validates complete workflow similar to the OSM example with realistic queries.
Actions:
1. Execute baseline queries without cache
2. Generate cache fragments and populate cache with actual query results
3. Test cache application and query enhancement
4. Compare baseline vs. enhanced query results
5. Measure performance improvements
Expected Outcome: Complete workflow processes all queries with cache hits and performance improvements.

**Test: test_multi_partition_workflow**
File: tests/integration/test_end_to_end_workflows.py
Purpose: Tests workflows using multiple partition keys (zipcode and region).
Actions:
1. Register multiple partition keys with different datatypes
2. Process queries specific to each partition type
3. Verify partition isolation and correct cache behavior
4. Test cross-partition query scenarios
Expected Outcome: Each partition key maintains isolation with successful cache hits for partition-specific queries.

**Test: test_queue_to_cache_workflow**
File: tests/integration/test_end_to_end_workflows.py
Purpose: Validates complete workflow from queue processing to cache application.
Actions:
1. Clear queues and add test queries to original queue
2. Simulate queue processor by generating fragments and executing queries
3. Populate cache with actual query results
4. Test cache application with new queries using populated cache
Expected Outcome: Queued queries result in populated cache that provides hits for subsequent similar queries.

**Test: test_cli_integration_workflow**
File: tests/integration/test_end_to_end_workflows.py
Purpose: Tests complete workflow using CLI commands for setup, add, and management.
Actions:
1. Use CLI to setup all PartitionCache tables and configuration
2. Add queries to cache using CLI add command with temporary files
3. Check cache status and counts using CLI management commands
4. Verify workflow success through CLI return codes and output
Expected Outcome: Complete CLI workflow succeeds with proper setup, query addition, and status reporting.

**Test: test_performance_monitoring_workflow**
File: tests/integration/test_end_to_end_workflows.py
Purpose: Validates performance monitoring across different query types and complexity levels.
Actions:
1. Execute various query types (simple, range, IN list, complex joins)
2. Measure baseline performance without cache
3. Populate cache and measure cache lookup performance
4. Test enhanced query performance where supported
5. Analyze performance characteristics and verify reasonable response times
Expected Outcome: Performance monitoring shows cache hits across query types with reasonable lookup times (<1s average).

## Running the Tests

The integration tests are designed to run in a CI environment with:
- PostgreSQL 16 with pg_cron extension
- Redis (optional, for Redis backend tests)
- All PartitionCache dependencies installed

### Running All Tests
```bash
# Run unit tests
python -m pytest tests/pytest/ -v

# Run all integration tests
python -m pytest tests/integration/ -v

# Run integration tests excluding slow tests
python -m pytest tests/integration/ -v -m "not slow"
```

### Running by Category
```bash
# Core functionality tests
python -m pytest tests/integration/test_cache_backends.py tests/integration/test_cli.py -v

# Queue and workflow tests
python -m pytest tests/integration/test_queue_processor.py tests/integration/test_end_to_end_workflows.py -v

# Performance and stress tests
python -m pytest tests/integration/test_cache_performance.py tests/integration/test_error_recovery.py -v

# End-to-end workflows only
python -m pytest tests/integration/test_end_to_end_workflows.py -v
```

### Running with Specific Markers
```bash
# Performance tests only
python -m pytest tests/integration/ -v -m "performance"

# Stress tests only  
python -m pytest tests/integration/ -v -m "stress"

# Concurrent tests only
python -m pytest tests/integration/ -v -m "concurrent"
```
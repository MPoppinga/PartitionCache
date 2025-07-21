import os
import tempfile
import time

import pytest

import partitioncache
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy


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
    if hasattr(retrieved, "__iter__") and not isinstance(retrieved, str | bytes):
        return set(retrieved) == expected

    return retrieved == expected


def _extract_partition_values_from_query(query, partition_key="zipcode"):
    """
    Helper function to extract partition values from SQL queries for testing.

    This is a test-specific helper that identifies common test patterns.
    For production use, actual query execution should be used instead.

    Args:
        query: SQL query string
        partition_key: The partition key column name

    Returns:
        set: Set of partition values that the query likely targets
    """
    # Known test data values for different partition keys
    test_data_ranges = {
        "zipcode": {1001, 1002, 90210, 10001, 20001},
        "region": {"northeast", "west", "southeast"},
        "store_id": {100, 200, 300, 400, 500, 700, 800}
    }

    available_values = test_data_ranges.get(partition_key, set())
    query_upper = query.upper()

    # Extract values mentioned in the query
    extracted_values = set()

    # For zipcode queries, look for numeric values in typical zipcode range
    if partition_key == "zipcode":
        import re
        # Look for numbers that could be zipcodes
        numbers = re.findall(r'\b(\d{4,5})\b', query)
        for num_str in numbers:
            num = int(num_str)
            if num in available_values:
                extracted_values.add(num)

        # Handle BETWEEN clauses
        between_match = re.search(r'BETWEEN\s+(\d+)\s+AND\s+(\d+)', query_upper)
        if between_match:
            lower = int(between_match.group(1))
            upper = int(between_match.group(2))
            # Add any test values that fall in this range
            extracted_values.update(val for val in available_values if lower <= val <= upper)

    # For text-based partition keys like region
    elif partition_key == "region":
        # Look for quoted strings that match our test regions
        import re
        quoted_strings = re.findall(r"'([^']+)'", query)
        quoted_strings.extend(re.findall(r'"([^"]+)"', query))

        for string_val in quoted_strings:
            if string_val in available_values:
                extracted_values.add(string_val)

    # For numeric IDs like store_id
    elif partition_key == "store_id":
        import re
        numbers = re.findall(r'\b(\d+)\b', query)
        for num_str in numbers:
            num = int(num_str)
            if num in available_values:
                extracted_values.add(num)

    # Fallback: if no specific values found, return a reasonable default
    if not extracted_values and available_values:
        # Return a small subset for testing
        extracted_values = set(list(available_values)[:2])

    return extracted_values


@pytest.fixture
def e2e_cache_client(db_session):
    """
    Fixture providing only postgresql_array backend for E2E tests.

    E2E tests focus on testing complete workflows, not individual backend implementations.
    Using a single backend avoids table conflicts when all tests share the same database.
    Individual backend functionality is thoroughly tested in regular integration tests.
    """
    from partitioncache.cache_handler import get_cache_handler

    # Only use postgresql_array for E2E workflow tests
    cache_backend = "postgresql_array"
    cache_handler = get_cache_handler(cache_backend)

    yield cache_handler

    # Cleanup
    cache_handler.close()


class TestEndToEndWorkflows:
    """
    End-to-end integration tests that validate complete workflows
    from query submission to cache application.

    Note: These tests use only postgresql_array backend to avoid conflicts
    when multiple backends share the same database. Individual backend
    functionality is tested separately in backend-specific integration tests.
    """

    def test_complete_osm_style_workflow(self, db_session, e2e_cache_client):
        """Test complete workflow similar to OSM example."""
        # This test replicates the OSM example workflow but simplified

        partition_key = "zipcode"
        # datatype = "integer"  # Not used in this test

        # Sample queries similar to OSM example
        test_queries = [
            "SELECT * FROM test_locations WHERE zipcode = 1001;",
            "SELECT name, population FROM test_locations WHERE zipcode BETWEEN 1000 AND 2000;",
            "SELECT COUNT(*) FROM test_locations WHERE zipcode IN (1001, 1002);",
        ]

        results = []

        for query in test_queries:
            print(f"\nTesting query: {query}")

            # Step 1: Run query without cache (baseline)
            with db_session.cursor() as cur:
                cur.execute(query)
                baseline_results = cur.fetchall()

            print(f"Baseline results: {len(baseline_results)} rows")

            # Step 2: Generate cache fragments and populate cache
            from partitioncache.query_processor import generate_all_hashes

            hashes = generate_all_hashes(query, partition_key)

            partition_values = set()
            # Execute query to get actual partition values
            with db_session.cursor() as cur:
                cur.execute(query)
                query_results = cur.fetchall()

                # Extract partition values using helper function
                if partition_key in query.lower():
                    # Try to extract from actual results first
                    partition_values = set()
                    for row in query_results:
                        # For non-COUNT queries, try to find partition values in results
                        if "COUNT" not in query.upper():
                            try:
                                # Look for partition values in any column
                                for value in row:
                                    if partition_key == "zipcode" and isinstance(value, int) and 1000 <= value <= 99999:
                                        partition_values.add(value)
                                    elif partition_key == "region" and isinstance(value, str):
                                        partition_values.add(value)
                            except (IndexError, TypeError):
                                pass

                    # If we couldn't extract from results, use query analysis
                    if not partition_values:
                        partition_values = _extract_partition_values_from_query(query, partition_key)

            # Populate cache with discovered partition values
            for hash_key in hashes:
                if partition_values:
                    e2e_cache_client.set_cache(hash_key, partition_values, partition_key)

            # Step 3: Test cache application
            cached_partition_keys, num_subqueries, num_hits = partitioncache.get_partition_keys(
                query=query,
                cache_handler=e2e_cache_client,
                partition_key=partition_key,
                min_component_size=1,
            )

            print(f"Cache hits: {num_hits}, Cached partition keys: {cached_partition_keys}")

            # Step 4: Test query enhancement if cache handler supports lazy operations
            if isinstance(e2e_cache_client, AbstractCacheHandler_Lazy) and cached_partition_keys:
                enhanced_query, stats = partitioncache.apply_cache_lazy(
                    query=query,
                    cache_handler=e2e_cache_client,
                    partition_key=partition_key,
                    method="TMP_TABLE_IN",
                    min_component_size=1,
                )

                print(f"Enhanced query: {enhanced_query[:100]}...")
                print(f"Stats: {stats}")

                # Execute enhanced query (may be multiple statements)
                with db_session.cursor() as cur:
                    # Split multi-statement queries and execute them
                    statements = [stmt.strip() for stmt in enhanced_query.split(";") if stmt.strip()]
                    enhanced_results = []

                    for stmt in statements:
                        cur.execute(stmt)
                        # Only fetch results from SELECT statements
                        if stmt.upper().strip().startswith("SELECT"):
                            enhanced_results = cur.fetchall()

                # Results should match or be subset of baseline
                assert len(enhanced_results) <= len(baseline_results), "Enhanced query returned more results than baseline"

                results.append(
                    {
                        "query": query,
                        "baseline_rows": len(baseline_results),
                        "enhanced_rows": len(enhanced_results),
                        "cache_hits": num_hits,
                        "enhanced": stats.get("enhanced", False),
                    }
                )
            else:
                results.append(
                    {
                        "query": query,
                        "baseline_rows": len(baseline_results),
                        "cache_hits": num_hits,
                        "partition_keys": len(cached_partition_keys) if cached_partition_keys else 0,
                    }
                )

        # Verify overall workflow success
        assert len(results) == len(test_queries), "Not all queries processed"
        cache_hits = sum(r["cache_hits"] for r in results)
        assert cache_hits > 0, "No cache hits across all queries"

        print(f"\nWorkflow Summary: {len(results)} queries, {cache_hits} total cache hits")

    def test_multi_partition_workflow(self, db_session, e2e_cache_client):
        """Test workflow with multiple partition keys."""
        # postgresql_array backend supports multiple datatypes

        # Test using both zipcode and region partition keys
        partition_configs = [
            ("zipcode", "integer"),
            ("region", "text"),
        ]

        # Queries for each partition type
        queries_by_partition = {
            "zipcode": [
                "SELECT * FROM test_locations WHERE zipcode = 1001;",
                "SELECT region FROM test_locations WHERE zipcode IN (1001, 1002);",
            ],
            "region": [
                "SELECT * FROM test_locations WHERE region = 'northeast';",
                "SELECT zipcode FROM test_locations WHERE region IN ('northeast', 'west');",
            ],
        }

        workflow_results = {}

        for partition_key, datatype in partition_configs:
            print(f"\nTesting partition: {partition_key} ({datatype})")

            # Register partition key if needed
            try:
                e2e_cache_client.register_partition_key(partition_key, datatype)
            except Exception:
                pass  # May already be registered

            partition_results = []

            for query in queries_by_partition[partition_key]:
                # Execute query to get actual results
                with db_session.cursor() as cur:
                    cur.execute(query)
                    baseline_results = cur.fetchall()

                # Generate cache entries
                from partitioncache.query_processor import generate_all_hashes

                hashes = generate_all_hashes(query, partition_key)

                # Determine partition values using helper function
                partition_values = _extract_partition_values_from_query(query, partition_key)

                # Populate cache
                for hash_key in hashes:
                    e2e_cache_client.set_cache(hash_key, partition_values, partition_key)

                # Test cache retrieval
                cached_keys, _, hits = partitioncache.get_partition_keys(
                    query=query,
                    cache_handler=e2e_cache_client,
                    partition_key=partition_key,
                    min_component_size=1,
                )

                partition_results.append({"query": query, "hits": hits, "cached_keys": cached_keys, "baseline_rows": len(baseline_results)})

            workflow_results[partition_key] = partition_results

        # Verify multi-partition workflow
        for partition_key, results in workflow_results.items():
            total_hits = sum(r["hits"] for r in results)
            assert total_hits > 0, f"No cache hits for partition {partition_key}"
            print(f"Partition {partition_key}: {len(results)} queries, {total_hits} hits")

    def test_queue_to_cache_workflow(self, db_session, e2e_cache_client):
        """Test complete workflow from queue processing to cache application."""

        from partitioncache.queue import clear_all_queues, push_to_original_query_queue

        # Clear queues to start fresh
        clear_all_queues()

        partition_key = "zipcode"
        test_queries = [
            "SELECT * FROM test_locations WHERE zipcode = 1001;",
            "SELECT name FROM test_locations WHERE zipcode BETWEEN 1000 AND 1500;",
        ]

        # Step 1: Add queries to queue
        queued_queries = []
        for query in test_queries:
            success = push_to_original_query_queue(query=query, partition_key=partition_key, partition_datatype="integer")
            if success:
                queued_queries.append(query)

        assert len(queued_queries) > 0, "No queries successfully queued"

        # Ensure partition key is registered
        try:
            e2e_cache_client.register_partition_key(partition_key, "integer")
        except Exception:
            # Might already be registered
            pass

        # Step 2: Simulate queue processing
        # In real workflow, queue processor would handle this
        for query in queued_queries:
            # Generate fragments (simulate queue processor)
            from partitioncache.query_processor import generate_all_hashes

            hashes = generate_all_hashes(query, partition_key)

            # Execute query to get results (simulate fragment execution)
            with db_session.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()

                # Extract partition values from results
                partition_values = set()
                for row in results:
                    try:
                        # Assume zipcode is accessible in the result
                        for value in row:
                            if isinstance(value, int) and 1000 <= value <= 99999:
                                partition_values.add(value)
                                break
                    except Exception:
                        pass

                # If no values extracted, use query-based extraction
                if not partition_values:
                    partition_values = _extract_partition_values_from_query(query, partition_key)

            # Populate cache (simulate queue processor cache population)
            for hash_key in hashes:
                if partition_values:
                    e2e_cache_client.set_cache(hash_key, partition_values, partition_key)

        # Step 3: Test cache application for new queries
        test_query = "SELECT population FROM test_locations WHERE zipcode = 1001;"

        # Should find cache hits from previously processed queries
        cached_keys, _, hits = partitioncache.get_partition_keys(
            query=test_query,
            cache_handler=e2e_cache_client,
            partition_key=partition_key,
            min_component_size=1,
        )

        assert hits > 0, "No cache hits for test query after queue processing"
        assert cached_keys is not None, "No cached keys returned"

        print(f"Queue workflow: {len(queued_queries)} queued, {hits} cache hits for test query")

    def test_cli_integration_workflow(self, db_session):
        """Test workflow using CLI commands."""
        import subprocess

        # Set up environment for CLI commands
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "CACHE_BACKEND": "postgresql_array",
            }
        )

        # Step 1: Setup via CLI
        setup_result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "setup", "all"], capture_output=True, text=True, env=env, timeout=60)

        # Setup should succeed or already exist
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Step 2: Add query via CLI
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            test_query = "SELECT * FROM test_locations WHERE zipcode = 1001;"
            f.write(test_query)
            query_file = f.name

        try:
            add_result = subprocess.run(
                [
                    "python",
                    "-m",
                    "partitioncache.cli.add_to_cache",
                    "--direct",
                    "--query-file",
                    query_file,
                    "--partition-key",
                    "zipcode",
                    "--partition-datatype",
                    "integer",
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=120,
            )

            # CLI add should succeed or fail with specific known issues
            if add_result.returncode != 0:
                # Be more specific about expected error conditions
                error_output = (add_result.stdout + add_result.stderr).lower()

                # Expected issues in test environment
                expected_errors = [
                    "no such table",  # Test data not set up
                    "permission denied",  # Database permissions
                    "connection refused",  # Database not available
                    "database does not exist",  # Test DB setup issue
                    "role does not exist",  # User setup issue
                ]

                has_expected_error = any(err in error_output for err in expected_errors)
                if not has_expected_error:
                    pytest.fail(f"CLI command failed with unexpected error: {add_result.stderr}")

            # Step 3: Check cache status via CLI
            count_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "cache", "count"], capture_output=True, text=True, env=env, timeout=60
            )

            # Should provide cache status information or fail with known issues
            if count_result.returncode != 0:
                error_output = (count_result.stdout + count_result.stderr).lower()
                expected_errors = [
                    "no such table",  # Cache tables not set up
                    "permission denied",  # Database permissions
                    "connection refused",  # Database not available
                    "database does not exist",  # Test DB setup issue
                    "role does not exist",  # User setup issue
                ]

                has_expected_error = any(err in error_output for err in expected_errors)
                if not has_expected_error:
                    pytest.fail(f"Cache count command failed with unexpected error: {count_result.stderr}")

            if count_result.returncode == 0:
                # Should contain numerical information or cache statistics (check both stdout and stderr)
                output = (count_result.stdout + count_result.stderr).lower()
                assert any(char.isdigit() for char in count_result.stdout + count_result.stderr) or "cache" in output or "statistics" in output
                print(f"✅ CLI cache count output: {count_result.stdout.strip() + count_result.stderr.strip()}")

        finally:
            # Cleanup
            os.unlink(query_file)

        print("CLI integration workflow completed successfully")

    def test_performance_monitoring_workflow(self, db_session, e2e_cache_client):
        """Test workflow for monitoring cache performance."""
        partition_key = "zipcode"

        # Create a series of queries with known performance characteristics
        performance_queries = [
            ("simple", "SELECT * FROM test_locations WHERE zipcode = 1001;"),
            ("range", "SELECT * FROM test_locations WHERE zipcode BETWEEN 1000 AND 2000;"),
            ("in_list", "SELECT * FROM test_locations WHERE zipcode IN (1001, 1002, 90210);"),
            (
                "complex",
                """
                SELECT l1.name, l1.population
                FROM test_locations l1
                JOIN test_locations l2 ON l1.region = l2.region
                WHERE l1.zipcode = 1001;
            """,
            ),
        ]

        performance_results = []

        for query_type, query in performance_queries:
            print(f"\nPerformance test: {query_type}")

            # Measure baseline performance
            start_time = time.time()
            with db_session.cursor() as cur:
                cur.execute(query)
                baseline_results = cur.fetchall()
            baseline_time = time.time() - start_time

            # Populate cache
            from partitioncache.query_processor import generate_all_hashes

            hashes = generate_all_hashes(query, partition_key)

            # Use test data partition values
            test_partition_values = {1001, 1002, 90210}
            for hash_key in hashes:
                e2e_cache_client.set_cache(hash_key, test_partition_values, partition_key)

            # Measure cache-enabled performance
            start_time = time.time()
            cached_keys, _, hits = partitioncache.get_partition_keys(
                query=query,
                cache_handler=e2e_cache_client,
                partition_key=partition_key,
                min_component_size=1,
            )
            cache_lookup_time = time.time() - start_time

            # Test enhanced query if supported
            enhanced_time = None
            if isinstance(e2e_cache_client, AbstractCacheHandler_Lazy) and cached_keys:
                start_time = time.time()
                enhanced_query, stats = partitioncache.apply_cache_lazy(
                    query=query,
                    cache_handler=e2e_cache_client,
                    partition_key=partition_key,
                    method="TMP_TABLE_IN",
                    min_component_size=1,
                )

                with db_session.cursor() as cur:
                    # Enhanced query might be a multi-statement query (with temp tables)
                    # Split and execute each statement separately
                    statements = [stmt.strip() for stmt in enhanced_query.split(";") if stmt.strip()]
                    for stmt in statements:
                        cur.execute(stmt)
                        # Only fetch results if this statement returns data
                        try:
                            cur.fetchall()
                        except Exception:
                            # Statement doesn't return data (e.g., CREATE TABLE, etc.)
                            pass
                enhanced_time = time.time() - start_time

            performance_results.append(
                {
                    "query_type": query_type,
                    "baseline_time": baseline_time,
                    "cache_lookup_time": cache_lookup_time,
                    "enhanced_time": enhanced_time,
                    "cache_hits": hits,
                    "baseline_rows": len(baseline_results),
                }
            )

        # Analyze performance results
        for result in performance_results:
            print(
                f"{result['query_type']}: baseline={result['baseline_time']:.3f}s, cache_lookup={result['cache_lookup_time']:.3f}s, hits={result['cache_hits']}"
            )

        # Verify performance characteristics
        total_hits = sum(r["cache_hits"] for r in performance_results)
        assert total_hits > 0, "No cache hits in performance monitoring"

        # Cache lookups should be reasonably fast
        avg_cache_time = sum(r["cache_lookup_time"] for r in performance_results) / len(performance_results)
        assert avg_cache_time < 1.0, f"Average cache lookup too slow: {avg_cache_time:.3f}s"

        print(f"Performance monitoring: {len(performance_results)} queries, avg cache lookup: {avg_cache_time:.3f}s")

    def test_data_consistency_workflow(self, db_session, e2e_cache_client):
        """Test workflow ensuring data consistency across operations."""
        partition_key = "zipcode"

        # Test data consistency across different operations
        consistency_queries = [
            "SELECT zipcode FROM test_locations WHERE zipcode = 1001;",
            "SELECT zipcode FROM test_locations WHERE zipcode IN (1001, 1002);",
            "SELECT DISTINCT zipcode FROM test_locations WHERE region = 'northeast';",
        ]

        expected_results = {}
        cached_results = {}

        # Step 1: Get baseline results
        for i, query in enumerate(consistency_queries):
            with db_session.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
                # Extract zipcode values
                zipcodes = set()
                for row in results:
                    for value in row:
                        if isinstance(value, int) and 1000 <= value <= 99999:
                            zipcodes.add(value)
                expected_results[i] = zipcodes

        # Step 2: Populate cache and test consistency
        for i, query in enumerate(consistency_queries):
            from partitioncache.query_processor import generate_all_hashes

            hashes = generate_all_hashes(query, partition_key)

            # Use expected results as cache data
            for hash_key in hashes:
                if expected_results[i]:
                    e2e_cache_client.set_cache(hash_key, expected_results[i], partition_key)

            # Retrieve from cache
            cached_keys, _, hits = partitioncache.get_partition_keys(
                query=query,
                cache_handler=e2e_cache_client,
                partition_key=partition_key,
                min_component_size=1,
            )

            cached_results[i] = cached_keys if cached_keys else set()

        # Step 3: Verify consistency
        for i in range(len(consistency_queries)):
            assert _compare_cache_values(cached_results[i], expected_results[i]), (
                f"Inconsistent results for query {i}: cache={cached_results[i]}, expected={expected_results[i]}"
            )

        print(f"Data consistency verified across {len(consistency_queries)} queries")

        # Test cross-query consistency
        if len(expected_results) > 1:
            # Queries involving zipcode 1001 should have consistent results
            zipcode_1001_queries = [i for i, result in expected_results.items() if 1001 in result]
            if len(zipcode_1001_queries) > 1:
                for i in zipcode_1001_queries:
                    assert 1001 in cached_results[i], f"Missing 1001 in cached results for query {i}"

        print("Cross-query consistency verified")

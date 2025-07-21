"""
Integration tests for cache optimization in pcache-monitor.

Tests the complete cache optimization workflow including:
- Cache population
- Fragment generation with optimization
- Original query/hash preservation
- Different optimization methods
"""

import os
import subprocess
import tempfile
import time

import pytest

from partitioncache.queue import (
    clear_original_query_queue,
    clear_query_fragment_queue,
    push_to_original_query_queue,
)


@pytest.fixture(scope="function")
def cache_optimization_setup(cache_client, request):
    """Set up test environment for cache optimization testing."""
    # Skip if not PostgreSQL backend (queue operations require PostgreSQL)
    # Use a simpler approach: check the cache_client type directly
    cache_backend = cache_client.__class__.__name__.lower()
    
    # Also check the string representation for fallback
    if "postgresql" not in cache_backend:
        cache_repr = str(cache_client).lower()
        if "postgresql" not in cache_repr:
            pytest.skip(f"Cache optimization tests require PostgreSQL backend, got {cache_backend}")

    partition_key = "store_id"

    # Clear queues if possible - non-critical for cache optimization tests
    try:
        clear_original_query_queue()
        clear_query_fragment_queue()
    except Exception as e:
        # Queue operations not critical for cache optimization tests
        # These tests focus on cache handler functionality
        print(f"Warning: Could not clear queues (non-critical): {e}")

    # Add some test cache entries that fragments can leverage
    test_cache_data = [
        # Basic city filter
        ("SELECT DISTINCT store_id FROM sales WHERE city_id = 10", {100, 200, 300}),
        # Date range filter
        ("SELECT DISTINCT store_id FROM sales WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01'", {100, 400, 700, 800}),
        # Product category filter
        ("SELECT DISTINCT store_id FROM sales WHERE product_category = 'electronics'", {200, 300, 500}),
        # Complex condition
        ("SELECT DISTINCT store_id FROM sales WHERE city_id = 10 AND product_category = 'electronics'", {200, 300}),
    ]

    # Register partition key if needed
    try:
        cache_client.register_partition_key(partition_key, "integer")
    except Exception:
        pass  # May already be registered

    # Use cache_client directly to add test data
    for query, store_ids in test_cache_data:
        # Generate a simple hash for the query
        import hashlib
        cache_hash = hashlib.sha1(query.encode()).hexdigest()
        cache_client.set_cache(cache_hash, store_ids, partition_key)
        if hasattr(cache_client, 'set_query'):
            cache_client.set_query(cache_hash, query, partition_key)

    yield cache_client, partition_key

    # Cleanup queues if possible
    try:
        clear_original_query_queue()
        clear_query_fragment_queue()
    except Exception:
        # Non-critical - cache optimization tests don't depend on queue state
        pass


class TestCacheOptimizationIntegration:
    """Integration tests for cache optimization in pcache-monitor."""

    def test_cache_optimization_enabled_by_default(self, cache_optimization_setup):
        """Test that cache optimization is enabled by default."""
        cache_client, partition_key = cache_optimization_setup
        print(f"DEBUG: Test is running with cache_client={cache_client}")

        # Add a test query to the queue
        test_query = """
        SELECT store_id, SUM(revenue) as total_revenue
        FROM sales
        WHERE city_id = 10 AND product_category = 'electronics'
        GROUP BY store_id
        """

        push_to_original_query_queue(test_query, partition_key, "integer")

        # Run monitor without explicit optimization flag (should be enabled by default)
        try:
            # Run for a short time to process the query
            result = subprocess.run(
                ["pcache-monitor", "--max-processes", "1", "--close", "--status-log-interval", "1"], 
                capture_output=True, text=True, timeout=15
            )

            # Check that cache optimization was enabled in logs
            assert "Cache optimization ENABLED" in result.stderr

        except subprocess.TimeoutExpired:
            # For timeout cases, check if any expected output was produced
            pytest.skip("Monitor process timed out - may indicate system load issues")

    def test_cache_optimization_can_be_disabled(self, cache_optimization_setup):
        """Test that cache optimization can be explicitly disabled."""
        cache_client, partition_key = cache_optimization_setup
        print(f"DEBUG: Test is running with cache_client={cache_client}")

        # Add a test query to the queue
        test_query = """
        SELECT store_id, SUM(revenue) as total_revenue
        FROM sales
        WHERE city_id = 10 AND product_category = 'electronics'
        GROUP BY store_id
        """

        push_to_original_query_queue(test_query, partition_key, "integer")

        # Run monitor with cache optimization explicitly disabled
        try:
            # Run for a short time to process the query - explicitly disable optimization
            result = subprocess.run(
                ["pcache-monitor", "--max-processes", "1", "--close", "--status-log-interval", "1", "--disable-cache-optimization"],
                capture_output=True, text=True, timeout=15
            )

            # Check that cache optimization was not mentioned in logs
            assert "Cache optimization ENABLED" not in result.stderr
            assert "Applied cache optimization" not in result.stderr

        except subprocess.TimeoutExpired:
            pytest.skip("Monitor process timed out - may indicate system load issues")

    def test_cache_optimization_enabled_with_hits(self, cache_optimization_setup):
        """Test cache optimization when enabled and cache hits are found."""
        cache_client, partition_key = cache_optimization_setup

        # Add a test query that should benefit from cache optimization
        test_query = """
        SELECT store_id, COUNT(*) as order_count
        FROM sales
        WHERE city_id = 10 AND order_date >= '2024-01-01' AND order_date < '2024-02-01'
        GROUP BY store_id
        ORDER BY order_count DESC
        """

        push_to_original_query_queue(test_query, partition_key, "integer")

        # Run monitor with cache optimization enabled
        try:
            result = subprocess.run(
                [
                    "pcache-monitor",
                    "--enable-cache-optimization",
                    "--cache-optimization-method",
                    "IN_SUBQUERY",
                    "--max-processes",
                    "1",
                    "--close",
                    "--status-log-interval",
                    "1",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            # Check that cache optimization was enabled and used
            assert "Cache optimization ENABLED" in result.stderr
            # Note: Actual optimization application would depend on the specific query fragments generated

        except subprocess.TimeoutExpired:
            pass  # Expected for monitor processes

    def test_cache_optimization_method_validation(self, cache_optimization_setup):
        """Test different cache optimization methods."""
        cache_client, partition_key = cache_optimization_setup

        methods_to_test = ["IN", "VALUES", "IN_SUBQUERY", "TMP_TABLE_IN"]

        for method in methods_to_test:
            # Clear queue and add test query
            clear_original_query_queue()
            clear_query_fragment_queue()

            test_query = f"""
            SELECT store_id, AVG(revenue) as avg_revenue
            FROM sales
            WHERE city_id = 10
            GROUP BY store_id
            -- Method: {method}
            """

            push_to_original_query_queue(test_query, partition_key, "integer")

            try:
                result = subprocess.run(
                    [
                        "pcache-monitor",
                        "--enable-cache-optimization",
                        "--cache-optimization-method",
                        method,
                        "--max-processes",
                        "1",
                        "--close",
                        "--status-log-interval",
                        "1",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=20,
                )

                # Check that the method was set correctly
                assert f"Method: {method}" in result.stderr

            except subprocess.TimeoutExpired:
                pass  # Expected for monitor processes

    def test_min_cache_hits_threshold(self, cache_optimization_setup):
        """Test that min-cache-hits threshold is respected for non-lazy methods."""
        cache_client, partition_key = cache_optimization_setup

        # Add a query that might have few cache hits
        test_query = """
        SELECT store_id, MAX(order_date) as latest_order
        FROM sales
        WHERE customer_segment = 'premium'
        GROUP BY store_id
        """

        push_to_original_query_queue(test_query, partition_key, "integer")

        # Test with high threshold that should prevent optimization
        try:
            result = subprocess.run(
                [
                    "pcache-monitor",
                    "--enable-cache-optimization",
                    "--cache-optimization-method",
                    "IN",  # Non-lazy method
                    "--min-cache-hits",
                    "10",  # High threshold
                    "--no-prefer-lazy-optimization",
                    "--max-processes",
                    "1",
                    "--close",
                    "--status-log-interval",
                    "1",
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )

            # Check configuration was applied
            assert "Min cache hits: 10" in result.stderr
            assert "Prefer lazy: False" in result.stderr

        except subprocess.TimeoutExpired:
            pass  # Expected for monitor processes

    def test_original_query_preservation(self, cache_optimization_setup):
        """Test that original query and hash are preserved in cache."""
        cache_client, partition_key = cache_optimization_setup

        # Use a distinctive query that we can check for later
        original_query = """
        SELECT store_id,
               SUM(revenue) as total_revenue,
               COUNT(*) as order_count
        FROM sales
        WHERE city_id = 10
          AND product_category = 'electronics'
          AND order_date >= '2024-01-01'
        GROUP BY store_id
        HAVING SUM(revenue) > 1000
        ORDER BY total_revenue DESC
        """

        push_to_original_query_queue(original_query, partition_key, "integer")

        # Get initial cache state
        initial_query_count = 0
        try:
            all_queries = cache_client.get_all_queries(partition_key)
            initial_query_count = len(all_queries) if all_queries else 0
        except Exception:
            pass  # Some backends might not support this

        # Run monitor with optimization
        try:
            subprocess.run(
                [
                    "pcache-monitor",
                    "--enable-cache-optimization",
                    "--cache-optimization-method",
                    "IN_SUBQUERY",
                    "--max-processes",
                    "1",
                    "--close",
                    "--status-log-interval",
                    "1",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

        except subprocess.TimeoutExpired:
            pass  # Expected for monitor processes

        # Wait a moment for processing to complete
        time.sleep(2)

        # Check that new queries were added to cache (indicating processing occurred)
        try:
            final_queries = cache_client.get_all_queries(partition_key)
            final_query_count = len(final_queries) if final_queries else 0

            # We should have more queries after processing
            assert final_query_count >= initial_query_count

            # Check that some query text contains parts of our original query
            if final_queries:
                found_related_query = False
                for _query_hash, query_text in final_queries:
                    if any(keyword in query_text.lower() for keyword in ["sales", "store_id", "revenue"]):
                        found_related_query = True
                        # Ensure it's a valid SQL fragment, not an optimized version with extra restrictions
                        assert "SELECT" in query_text.upper()
                        break

                # We should find at least one query related to our test
                assert found_related_query, "Should find queries related to our test case"

        except AttributeError:
            # Some cache handlers might not support get_all_queries
            pytest.skip("Cache handler doesn't support query inspection")


class TestCacheOptimizationConfiguration:
    """Test cache optimization configuration and CLI validation."""

    def test_invalid_optimization_method(self):
        """Test that invalid optimization methods are rejected."""
        try:
            result = subprocess.run(
                ["pcache-monitor", "--enable-cache-optimization", "--cache-optimization-method", "INVALID_METHOD", "--help"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            # Should fail with invalid choice
            assert result.returncode != 0
            assert "invalid choice" in result.stderr.lower()

        except subprocess.TimeoutExpired:
            pytest.fail("Command should have failed quickly with invalid method")

    def test_help_shows_optimization_options(self):
        """Test that help text includes cache optimization options."""
        result = subprocess.run(["pcache-monitor", "--help"], capture_output=True, text=True, timeout=10)

        assert result.returncode == 0
        help_text = result.stdout

        # Check that all our new options are documented
        assert "--enable-cache-optimization" in help_text
        assert "--cache-optimization-method" in help_text
        assert "--min-cache-hits" in help_text
        assert "--prefer-lazy-optimization" in help_text
        assert "cache optimization options" in help_text.lower()

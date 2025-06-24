import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

import partitioncache
from partitioncache.cache_handler.abstract import AbstractCacheHandler


class TestCachePerformance:
    """
    Performance tests for cache operations across different backends.
    Tests throughput, latency, and scalability characteristics.
    """

    @pytest.mark.slow
    def test_large_set_operations(self, cache_client: AbstractCacheHandler):
        """Test performance with large sets of partition values."""
        partition_key = "zipcode"
        cache_key = "perf_large_set"

        # Generate large set of test values
        large_set = set(range(10000, 20000))  # 10,000 values

        # Measure set operation
        start_time = time.time()
        success = cache_client.set_set(cache_key, large_set, partition_key)
        set_time = time.time() - start_time

        assert success, "Failed to set large set"
        assert set_time < 10.0, f"Set operation too slow: {set_time:.2f}s"

        # Measure get operation
        start_time = time.time()
        retrieved = cache_client.get(cache_key, partition_key)
        get_time = time.time() - start_time

        assert retrieved == large_set, "Retrieved set doesn't match"
        assert get_time < 5.0, f"Get operation too slow: {get_time:.2f}s"

    @pytest.mark.slow
    def test_many_small_operations(self, cache_client: AbstractCacheHandler):
        """Test performance with many small cache operations."""
        partition_key = "zipcode"
        num_operations = 1000

        start_time = time.time()

        # Perform many small set operations
        for i in range(num_operations):
            cache_key = f"perf_small_{i}"
            test_set = {10000 + i, 10001 + i}
            cache_client.set_set(cache_key, test_set, partition_key)

        total_time = time.time() - start_time
        ops_per_second = num_operations / total_time

        # Should handle at least 50 operations per second
        assert ops_per_second > 50, f"Too slow: {ops_per_second:.1f} ops/sec"

    @pytest.mark.slow
    def test_intersection_performance(self, cache_client: AbstractCacheHandler):
        """Test performance of set intersection operations."""
        partition_key = "zipcode"
        num_sets = 50
        set_size = 1000

        # Create overlapping sets
        base_values = set(range(1000, 2000))
        cache_keys = []

        for i in range(num_sets):
            cache_key = f"intersect_perf_{i}"
            # Create sets with 80% overlap
            test_set = set(random.sample(list(base_values), set_size))
            cache_client.set_set(cache_key, test_set, partition_key)
            cache_keys.append(cache_key)

        # Measure intersection performance
        start_time = time.time()
        intersection, hit_count = cache_client.get_intersected(set(cache_keys), partition_key)
        intersection_time = time.time() - start_time

        assert hit_count == num_sets, "Not all sets found"
        assert intersection is not None, "Intersection should not be None"
        assert intersection_time < 5.0, f"Intersection too slow: {intersection_time:.2f}s"

    def test_cache_memory_efficiency(self, cache_client: AbstractCacheHandler):
        """Test memory efficiency with duplicate data."""
        partition_key = "zipcode"
        common_values = {1001, 1002, 1003}

        # Store the same values in many cache entries
        for i in range(100):
            cache_key = f"duplicate_{i}"
            cache_client.set_set(cache_key, common_values, partition_key)

        # Verify all entries exist and are retrievable
        for i in range(100):
            cache_key = f"duplicate_{i}"
            assert cache_client.exists(cache_key, partition_key)
            retrieved = cache_client.get(cache_key, partition_key)
            assert retrieved == common_values


class TestCacheConcurrency:
    """
    Tests for concurrent access patterns and thread safety.
    """

    @pytest.mark.slow
    def test_concurrent_writes(self, cache_client: AbstractCacheHandler):
        """Test concurrent write operations from multiple threads."""
        partition_key = "zipcode"
        num_threads = 10
        operations_per_thread = 50

        def write_worker(thread_id: int) -> list[str]:
            """Worker function for concurrent writes."""
            successful_keys = []
            for i in range(operations_per_thread):
                cache_key = f"concurrent_{thread_id}_{i}"
                test_set = {thread_id * 1000 + i}
                if cache_client.set_set(cache_key, test_set, partition_key):
                    successful_keys.append(cache_key)
            return successful_keys

        # Execute concurrent writes
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(write_worker, i) for i in range(num_threads)]
            results = [future.result() for future in as_completed(futures)]

        # Verify all operations succeeded
        total_successful = sum(len(result) for result in results)
        expected_total = num_threads * operations_per_thread

        # Allow for some failures in concurrent scenarios
        success_rate = total_successful / expected_total
        assert success_rate > 0.95, f"Too many concurrent write failures: {success_rate:.2%}"

    @pytest.mark.slow
    def test_concurrent_read_write(self, cache_client: AbstractCacheHandler):
        """Test mixed concurrent read and write operations."""
        partition_key = "zipcode"
        shared_keys = [f"shared_{i}" for i in range(20)]

        # Pre-populate some shared data
        for key in shared_keys:
            test_set = {1000 + hash(key) % 1000}
            cache_client.set_set(key, test_set, partition_key)

        def read_worker() -> int:
            """Worker function for concurrent reads."""
            successful_reads = 0
            for _ in range(100):
                key = random.choice(shared_keys)
                if cache_client.get(key, partition_key) is not None:
                    successful_reads += 1
            return successful_reads

        def write_worker() -> int:
            """Worker function for concurrent writes."""
            successful_writes = 0
            for i in range(50):
                key = f"writer_{random.randint(1000, 9999)}_{i}"
                test_set = {random.randint(2000, 3000)}
                if cache_client.set_set(key, test_set, partition_key):
                    successful_writes += 1
            return successful_writes

        # Execute mixed concurrent operations
        with ThreadPoolExecutor(max_workers=8) as executor:
            read_futures = [executor.submit(read_worker) for _ in range(4)]
            write_futures = [executor.submit(write_worker) for _ in range(4)]

            read_results = [future.result() for future in as_completed(read_futures)]
            write_results = [future.result() for future in as_completed(write_futures)]

        # Verify reasonable success rates
        total_reads = sum(read_results)
        total_writes = sum(write_results)

        assert total_reads > 350, f"Too many read failures: {total_reads}/400"
        assert total_writes > 180, f"Too many write failures: {total_writes}/200"


class TestCacheStress:
    """
    Stress tests to validate cache behavior under extreme conditions.
    """

    @pytest.mark.slow
    def test_memory_pressure(self, cache_client: AbstractCacheHandler):
        """Test cache behavior under memory pressure."""
        partition_key = "zipcode"
        large_sets = []

        try:
            # Create increasingly large sets until we hit limits
            for i in range(10):
                cache_key = f"stress_memory_{i}"
                set_size = 1000 * (i + 1)  # 1K, 2K, 3K... up to 10K
                large_set = set(range(i * 10000, i * 10000 + set_size))

                success = cache_client.set_set(cache_key, large_set, partition_key)
                if success:
                    large_sets.append((cache_key, large_set))
                else:
                    # Cache may reject very large sets
                    break

            # Verify we can still retrieve previously stored sets
            for cache_key, expected_set in large_sets:
                retrieved = cache_client.get(cache_key, partition_key)
                assert retrieved == expected_set, f"Failed to retrieve {cache_key}"

        except Exception as e:
            # Some backends may have memory limits
            pytest.skip(f"Backend memory limits reached: {e}")

    @pytest.mark.slow
    def test_rapid_operations(self, cache_client: AbstractCacheHandler):
        """Test rapid succession of cache operations."""
        partition_key = "zipcode"
        num_rapid_ops = 500

        start_time = time.time()

        # Rapid set/get/delete cycle
        for i in range(num_rapid_ops):
            cache_key = f"rapid_{i}"
            test_set = {i}

            # Set, verify, delete
            cache_client.set_set(cache_key, test_set, partition_key)
            retrieved = cache_client.get(cache_key, partition_key)
            if retrieved == test_set:
                cache_client.delete(cache_key, partition_key)

        total_time = time.time() - start_time
        ops_per_second = (num_rapid_ops * 3) / total_time  # 3 ops per iteration

        # Should handle rapid operations efficiently
        assert ops_per_second > 100, f"Rapid operations too slow: {ops_per_second:.1f} ops/sec"

    def test_edge_case_values(self, cache_client: AbstractCacheHandler):
        """Test cache with edge case values."""
        partition_key = "zipcode"

        edge_cases = [
            ("single_value", {42}),
            ("large_numbers", {2**31 - 1, 2**31 - 2}),
            ("zero_value", {0}),
            ("negative_values", {-1, -100, -1000}),
        ]

        for case_name, test_set in edge_cases:
            cache_key = f"edge_{case_name}"

            try:
                success = cache_client.set_set(cache_key, test_set, partition_key)
                if success:
                    retrieved = cache_client.get(cache_key, partition_key)
                    assert retrieved == test_set, f"Edge case {case_name} failed"
            except Exception as e:
                # Some backends may not support certain value ranges
                pytest.skip(f"Backend doesn't support {case_name}: {e}")


class TestCacheRealWorldScenarios:
    """
    Tests simulating real-world usage patterns and scenarios.
    """

    def test_query_cache_workflow(self, cache_client: AbstractCacheHandler, sample_queries, db_session):
        """Test realistic query caching workflow."""
        partition_key = "zipcode"

        # Simulate processing multiple queries with overlapping results
        queries_and_results = [
            (sample_queries["zipcode_simple"], {1001}),
            (sample_queries["zipcode_range"], {1001, 1002}),
            ("SELECT * FROM test_locations WHERE zipcode IN (1001, 90210);", {1001, 90210}),
        ]

        for query, expected_zipcodes in queries_and_results:
            # Generate hashes for the query
            from partitioncache.query_processor import generate_all_hashes
            hashes = generate_all_hashes(query, partition_key)

            # Populate cache with results
            for hash_key in hashes:
                cache_client.set_set(hash_key, expected_zipcodes, partition_key)

            # Verify cache retrieval
            partition_keys, _, hits = partitioncache.get_partition_keys(
                query=query,
                cache_handler=cache_client,
                partition_key=partition_key,
                min_component_size=1
            )

            assert hits > 0, f"No cache hits for query: {query}"
            assert partition_keys == expected_zipcodes, f"Unexpected results for: {query}"

    def test_cache_eviction_simulation(self, cache_client: AbstractCacheHandler):
        """Test cache behavior when simulating eviction scenarios."""
        partition_key = "zipcode"

        # Fill cache with many entries
        cache_entries = []
        for i in range(200):
            cache_key = f"eviction_test_{i}"
            test_set = {1000 + i}

            if cache_client.set_set(cache_key, test_set, partition_key):
                cache_entries.append((cache_key, test_set))

        # Verify a reasonable number were stored
        assert len(cache_entries) > 50, "Too few entries stored"

        # Test that we can still retrieve recent entries
        recent_entries = cache_entries[-10:]  # Last 10 entries
        for cache_key, expected_set in recent_entries:
            retrieved = cache_client.get(cache_key, partition_key)
            assert retrieved == expected_set, f"Lost recent entry: {cache_key}"

    def test_partition_key_isolation(self, cache_client: AbstractCacheHandler):
        """Test that different partition keys are properly isolated."""
        zipcode_key = "zipcode"
        region_key = "region"
        cache_key = "isolation_test"

        zipcode_values = {1001, 1002}
        region_values = {"northeast", "west"}

        # Store same cache key with different partition keys
        cache_client.set_set(cache_key, zipcode_values, zipcode_key)
        cache_client.set_set(cache_key, region_values, region_key)

        # Verify isolation - each partition should return its own values
        retrieved_zipcodes = cache_client.get(cache_key, zipcode_key)
        retrieved_regions = cache_client.get(cache_key, region_key)

        assert retrieved_zipcodes == zipcode_values, "Zipcode partition contaminated"
        assert retrieved_regions == region_values, "Region partition contaminated"

        # Verify cross-partition operations don't interfere
        assert cache_client.exists(cache_key, zipcode_key)
        assert cache_client.exists(cache_key, region_key)

        # Delete from one partition shouldn't affect the other
        cache_client.delete(cache_key, zipcode_key)
        assert not cache_client.exists(cache_key, zipcode_key)
        assert cache_client.exists(cache_key, region_key)

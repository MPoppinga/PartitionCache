import pytest
from pyroaring import BitMap

import partitioncache
from partitioncache.cache_handler.abstract import AbstractCacheHandler


class TestCacheLifecycle:
    """
    Test suite for cache backend lifecycle operations.

    This test class uses the parameterized cache_client fixture,
    so each test method runs automatically against all cache backends:
    - postgresql_array, postgresql_bit, postgresql_roaringbit
    - redis_set, redis_bit (if Redis is available)
    """

    def test_set_and_get_integer_partition(self, cache_client: AbstractCacheHandler):
        """Test basic set and get operations with integer partition keys."""
        partition_key = "zipcode"
        cache_key = "test_query_hash_001"
        test_values = {1001, 1002, 90210}

        # Set values in cache
        success = cache_client.set_set(cache_key, test_values, partition_key)
        assert success, "Failed to set values in cache"

        # Retrieve values from cache
        retrieved = cache_client.get(cache_key, partition_key)

        # Convert BitMap to set for comparison if necessary
        if isinstance(retrieved, BitMap):
            retrieved = set(retrieved)

        assert retrieved == test_values, f"Retrieved {retrieved}, expected {test_values}"

    def test_set_and_get_text_partition(self, cache_client: AbstractCacheHandler):
        """Test basic set and get operations with text partition keys."""
        # Skip test for backends that only support integer values
        if hasattr(cache_client, "get_supported_datatypes"):
            supported_datatypes = cache_client.get_supported_datatypes()
            if "text" not in supported_datatypes:
                pytest.skip(f"Backend {cache_client.__class__.__name__} does not support text datatypes. Supported: {supported_datatypes}")

        partition_key = "region"
        cache_key = "test_query_hash_002"
        test_values = {"northeast", "west", "southeast"}

        # Set values in cache
        success = cache_client.set_set(cache_key, test_values, partition_key)
        assert success, "Failed to set values in cache"

        # Retrieve values from cache
        retrieved = cache_client.get(cache_key, partition_key)

        # Convert BitMap to set for comparison if necessary
        if isinstance(retrieved, BitMap):
            retrieved = set(retrieved)

        assert retrieved == test_values, f"Retrieved {retrieved}, expected {test_values}"

    def test_get_miss(self, cache_client: AbstractCacheHandler):
        """Test cache miss behavior."""
        partition_key = "zipcode"
        non_existent_key = "non_existent_query_hash"

        # Attempt to retrieve non-existent key
        result = cache_client.get(non_existent_key, partition_key)
        assert result is None, "Expected None for cache miss"

    def test_exists_check(self, cache_client: AbstractCacheHandler):
        """Test key existence checking."""
        partition_key = "zipcode"
        cache_key = "test_exists_hash"
        test_values = {1001, 1002}

        # Key should not exist initially
        assert not cache_client.exists(cache_key, partition_key)

        # Set values and check existence
        cache_client.set_set(cache_key, test_values, partition_key)
        assert cache_client.exists(cache_key, partition_key)

    def test_update_value(self, cache_client: AbstractCacheHandler):
        """Test updating existing cache entries."""
        partition_key = "zipcode"
        cache_key = "test_update_hash"
        initial_values = {1001, 1002}
        updated_values = {1001, 1002, 90210, 10001}

        # Set initial values
        cache_client.set_set(cache_key, initial_values, partition_key)
        retrieved_initial = cache_client.get(cache_key, partition_key)

        # Convert BitMap to set for comparison if necessary
        if isinstance(retrieved_initial, BitMap):
            retrieved_initial = set(retrieved_initial)

        assert retrieved_initial == initial_values

        # Update with new values
        cache_client.set_set(cache_key, updated_values, partition_key)
        retrieved_updated = cache_client.get(cache_key, partition_key)

        # Convert BitMap to set for comparison if necessary
        if isinstance(retrieved_updated, BitMap):
            retrieved_updated = set(retrieved_updated)

        assert retrieved_updated == updated_values

    def test_delete_value(self, cache_client: AbstractCacheHandler):
        """Test deleting cache entries."""
        partition_key = "zipcode"
        cache_key = "test_delete_hash"
        test_values = {1001, 1002}

        # Set and verify values exist
        cache_client.set_set(cache_key, test_values, partition_key)
        assert cache_client.exists(cache_key, partition_key)

        # Delete and verify removal
        success = cache_client.delete(cache_key, partition_key)
        assert success, "Failed to delete cache entry"
        assert not cache_client.exists(cache_key, partition_key)
        assert cache_client.get(cache_key, partition_key) is None

    def test_null_value_handling(self, cache_client: AbstractCacheHandler):
        """Test null value storage and checking."""
        partition_key = "zipcode"
        cache_key = "test_null_hash"

        # Set null value
        success = cache_client.set_null(cache_key, partition_key)
        assert success, "Failed to set null value"

        # Check null status
        assert cache_client.is_null(cache_key, partition_key)
        assert cache_client.exists(cache_key, partition_key)

        # Get should return None for null entries
        result = cache_client.get(cache_key, partition_key)
        assert result is None

    def test_intersect_multiple_keys(self, cache_client: AbstractCacheHandler):
        """Test intersection of multiple cache entries."""
        partition_key = "zipcode"

        # Set up multiple cache entries with overlapping values
        cache_client.set_set("hash_1", {1001, 1002, 90210}, partition_key)
        cache_client.set_set("hash_2", {1001, 90210, 10001}, partition_key)
        cache_client.set_set("hash_3", {90210, 10001, 20001}, partition_key)

        # Test intersection of two keys
        keys_to_intersect = {"hash_1", "hash_2"}
        intersection, hit_count = cache_client.get_intersected(keys_to_intersect, partition_key)
        expected_intersection = {1001, 90210}  # Common values between hash_1 and hash_2

        # Convert BitMap to set for comparison if necessary
        if isinstance(intersection, BitMap):
            intersection = set(intersection)

        assert intersection == expected_intersection
        assert hit_count == 2  # Both keys found

        # Test intersection of all three keys
        all_keys = {"hash_1", "hash_2", "hash_3"}
        intersection_all, hit_count_all = cache_client.get_intersected(all_keys, partition_key)
        expected_all = {90210}  # Only value common to all three

        # Convert BitMap to set for comparison if necessary
        if isinstance(intersection_all, BitMap):
            intersection_all = set(intersection_all)

        assert intersection_all == expected_all
        assert hit_count_all == 3

    def test_filter_existing_keys(self, cache_client: AbstractCacheHandler):
        """Test filtering to find which keys exist in cache."""
        partition_key = "zipcode"

        # Set up some cache entries
        cache_client.set_set("existing_1", {1001}, partition_key)
        cache_client.set_set("existing_2", {1002}, partition_key)

        # Test filtering
        candidate_keys = {"existing_1", "existing_2", "non_existent_1", "non_existent_2"}
        existing_keys = cache_client.filter_existing_keys(candidate_keys, partition_key)

        expected_existing = {"existing_1", "existing_2"}
        assert existing_keys == expected_existing

    def test_get_all_keys(self, cache_client: AbstractCacheHandler):
        """Test retrieving all keys for a partition."""
        partition_key = "zipcode"

        # Set up multiple cache entries
        test_keys = ["all_keys_1", "all_keys_2", "all_keys_3"]
        for key in test_keys:
            cache_client.set_set(key, {1001}, partition_key)

        # Get all keys
        all_keys = cache_client.get_all_keys(partition_key)

        # All our test keys should be present
        for key in test_keys:
            assert key in all_keys

    def test_empty_set_handling(self, cache_client: AbstractCacheHandler):
        """Test handling of empty sets."""
        partition_key = "zipcode"
        cache_key = "empty_set_hash"
        empty_set: set[int] = set()

        # Some backends might handle empty sets differently
        try:
            success = cache_client.set_set(cache_key, empty_set, partition_key)
            if success:
                retrieved = cache_client.get(cache_key, partition_key)

                # Convert BitMap to set for comparison if necessary
                if isinstance(retrieved, BitMap):
                    retrieved = set(retrieved)

                assert retrieved == empty_set or retrieved is None
        except Exception:
            # Some backends might not support empty sets
            pytest.skip("Backend does not support empty sets")


class TestCacheIntegration:
    """
    Integration tests using PartitionCache API methods.
    Tests the full workflow from query processing to cache application.
    """

    def test_partition_cache_workflow(self, cache_client: AbstractCacheHandler, sample_queries, db_session):
        """Test complete workflow: generate hashes, populate cache, apply cache."""
        partition_key = "zipcode"
        query = sample_queries["zipcode_simple"]

        # Generate query hashes
        from partitioncache.query_processor import generate_all_hashes

        hashes = generate_all_hashes(query, partition_key)

        assert len(hashes) > 0, "Should generate at least one hash"

        # Populate cache with test data
        test_partition_values = {1001, 1002}
        for hash_key in hashes:
            cache_client.set_set(hash_key, test_partition_values, partition_key)

        # Use PartitionCache API to get partition keys
        partition_keys, num_subqueries, num_hits = partitioncache.get_partition_keys(
            query=query, cache_handler=cache_client, partition_key=partition_key, min_component_size=1
        )

        # Convert BitMap to set for comparison if necessary
        if isinstance(partition_keys, BitMap):
            partition_keys = set(partition_keys)

        assert partition_keys == test_partition_values
        assert num_hits > 0
        assert partition_keys is not None

    def test_apply_cache_lazy_integration(self, cache_client: AbstractCacheHandler, sample_queries, db_session):
        """Test apply_cache_lazy integration with actual database."""
        # Skip if cache handler doesn't support lazy operations
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        if not isinstance(cache_client, AbstractCacheHandler_Lazy):
            pytest.skip("Cache handler does not support lazy operations")

        partition_key = "zipcode"
        query = sample_queries["zipcode_range"]

        # Generate and populate cache
        from partitioncache.query_processor import generate_all_hashes

        hashes = generate_all_hashes(query, partition_key)
        test_values = {1001, 1002}

        for hash_key in hashes:
            cache_client.set_set(hash_key, test_values, partition_key)

        # Apply cache using lazy method
        enhanced_query, stats = partitioncache.apply_cache_lazy(
            query=query, cache_handler=cache_client, partition_key=partition_key, method="TMP_TABLE_IN", min_component_size=1
        )

        assert enhanced_query is not None
        assert stats["cache_hits"] > 0
        assert stats["enhanced"] == 1

        # Verify enhanced query contains cache restrictions
        assert "tmp_cache_keys" in enhanced_query.lower() or partition_key in enhanced_query.lower()

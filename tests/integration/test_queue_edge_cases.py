"""
Integration tests for queue edge cases and monitor resilience:
- Queue operations with empty queues
- Duplicate fragment handling
- Queue roundtrip integrity
- Large queue payloads
- Queue clear operations
- Multiple partition key queueing
"""

import pytest

from partitioncache.query_processor import generate_all_query_hash_pairs
from partitioncache.queue import (
    clear_all_queues,
    get_queue_lengths,
    get_queue_provider_name,
    pop_from_original_query_queue,
    pop_from_query_fragment_queue,
    push_to_original_query_queue,
    push_to_query_fragment_queue,
    reset_queue_handler,
)


@pytest.fixture(autouse=True)
def clean_queues(db_session):
    """Ensure clean queue state before and after each test."""
    reset_queue_handler()
    try:
        clear_all_queues()
    except Exception:
        pass
    yield
    try:
        clear_all_queues()
    except Exception:
        pass
    reset_queue_handler()


@pytest.fixture
def require_postgresql_queue():
    """Skip test if not using PostgreSQL queue provider."""
    if get_queue_provider_name() != "postgresql":
        pytest.skip("Test requires PostgreSQL queue provider")


class TestQueueEmptyOperations:
    """Test queue operations on empty queues."""

    def test_pop_from_empty_original_queue(self):
        """Popping from empty original queue should return None."""
        result = pop_from_original_query_queue()
        assert result is None, "Empty original queue should return None"

    def test_pop_from_empty_fragment_queue(self):
        """Popping from empty fragment queue should return None."""
        result = pop_from_query_fragment_queue()
        assert result is None, "Empty fragment queue should return None"

    def test_queue_lengths_when_empty(self):
        """Queue lengths should be 0 when empty."""
        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] == 0, "Original queue should be empty"
        assert lengths["query_fragment_queue"] == 0, "Fragment queue should be empty"

    def test_clear_empty_queues(self):
        """Clearing empty queues should succeed without error."""
        orig_cleared, frag_cleared = clear_all_queues()
        assert orig_cleared == 0
        assert frag_cleared == 0


class TestOriginalQueryQueueRoundtrip:
    """Test push/pop roundtrip for original query queue."""

    def test_push_and_pop_original_query(self):
        """Push a query and pop it back - verify integrity."""
        test_query = "SELECT * FROM test_locations WHERE zipcode = 1001"
        test_partition_key = "zipcode"
        test_datatype = "integer"

        success = push_to_original_query_queue(
            test_query,
            partition_key=test_partition_key,
            partition_datatype=test_datatype,
        )
        assert success is True, "Push should succeed"

        # Verify queue length
        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 1

        # Pop and verify full roundtrip integrity
        result = pop_from_original_query_queue()
        assert result is not None, "Should pop a query"
        query, partition_key, datatype = result
        assert query == test_query, f"Query text should survive roundtrip, got: {query}"
        assert partition_key == test_partition_key
        assert datatype == test_datatype

    def test_push_multiple_pop_fifo_order(self):
        """Multiple pushes should be popped in FIFO order."""
        queries = [
            ("SELECT 1 FROM t WHERE zipcode = 1", "zipcode", "integer"),
            ("SELECT 2 FROM t WHERE region = 'a'", "region", "text"),
            ("SELECT 3 FROM t WHERE zipcode = 2", "zipcode", "integer"),
        ]

        for query, pk, dt in queries:
            success = push_to_original_query_queue(query, partition_key=pk, partition_datatype=dt)
            assert success

        # Pop all and verify order
        popped = []
        for _ in range(3):
            result = pop_from_original_query_queue()
            if result:
                popped.append(result)

        assert len(popped) == 3, f"Should pop 3 queries, got {len(popped)}"

        # Verify partition keys match in order
        expected_pks = [pk for _, pk, _ in queries]
        actual_pks = [pk for _, pk, _ in popped]
        assert actual_pks == expected_pks, f"Expected {expected_pks}, got {actual_pks}"


class TestFragmentQueueRoundtrip:
    """Test push/pop roundtrip for fragment queue."""

    def test_push_and_pop_fragments(self):
        """Push fragments and verify they can be popped."""
        query = "SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001"
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) > 0

        success = push_to_query_fragment_queue(
            pairs,
            partition_key="zipcode",
            partition_datatype="integer",
        )
        assert success is True

        # Verify queue length
        lengths = get_queue_lengths()
        assert lengths["query_fragment_queue"] >= len(pairs)

        # Pop all fragments
        popped_count = 0
        while True:
            result = pop_from_query_fragment_queue()
            if result is None:
                break
            popped_count += 1
            query_text, query_hash, partition_key, datatype, _ = result
            assert len(query_hash) > 0, "Hash should not be empty"
            assert partition_key == "zipcode"
            assert datatype == "integer"

        assert popped_count >= len(pairs), f"Should pop at least {len(pairs)} fragments, got {popped_count}"

    def test_push_empty_fragment_list(self):
        """Pushing empty fragment list should handle gracefully."""
        success = push_to_query_fragment_queue(
            [],
            partition_key="zipcode",
            partition_datatype="integer",
        )
        # Should return a boolean without crashing
        assert isinstance(success, bool), "Should return a boolean"

        lengths = get_queue_lengths()
        assert lengths["query_fragment_queue"] == 0


class TestDuplicateFragmentHandling:
    """Test handling of duplicate fragments in the queue."""

    def test_duplicate_fragments_upsert(self, require_postgresql_queue):
        """PostgreSQL queue should handle duplicate fragments via upsert."""
        query = "SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001"
        pairs = generate_all_query_hash_pairs(query, "zipcode")

        # Push same fragments twice
        push_to_query_fragment_queue(pairs, "zipcode", "integer")
        push_to_query_fragment_queue(pairs, "zipcode", "integer")

        lengths = get_queue_lengths()
        # PostgreSQL uses upsert, so duplicates should not double the count
        assert lengths["query_fragment_queue"] == len(pairs), "Upsert should prevent duplicate fragments"

    def test_same_query_different_partition_keys(self):
        """Same query for different partition keys should create separate entries."""
        query = "SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001 AND t1.region = 'northeast'"

        pairs_zip = generate_all_query_hash_pairs(query, "zipcode")
        pairs_region = generate_all_query_hash_pairs(query, "region")

        push_to_query_fragment_queue(pairs_zip, "zipcode", "integer")
        push_to_query_fragment_queue(pairs_region, "region", "text")

        lengths = get_queue_lengths()
        total_expected = len(pairs_zip) + len(pairs_region)
        assert lengths["query_fragment_queue"] >= total_expected


class TestLargeQueuePayloads:
    """Test queue behavior with large payloads."""

    def test_large_query_text(self):
        """Queue should handle queries with very long text."""
        # Create a query with many conditions
        conditions = " AND ".join([f"t1.field_{i} = {i}" for i in range(50)])
        query = f"SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001 AND {conditions}"

        success = push_to_original_query_queue(query, "zipcode", "integer")
        assert success is True

        result = pop_from_original_query_queue()
        assert result is not None, "Should pop the large query"
        query_roundtrip, _partition_key, _datatype = result
        assert "field_49" in query_roundtrip, "Large query text should survive roundtrip"
        assert "field_0" in query_roundtrip, "Large query text should preserve all conditions"

    def test_many_fragments_at_once(self):
        """Push a large number of fragments in a single call."""
        # Generate many unique hash pairs
        pairs = [(f"SELECT {i} FROM t WHERE z = {i}", f"hash_{i:04d}") for i in range(100)]

        success = push_to_query_fragment_queue(pairs, "zipcode", "integer")
        assert success is True

        lengths = get_queue_lengths()
        assert lengths["query_fragment_queue"] >= 100


class TestQueueClearOperations:
    """Test selective and full queue clearing."""

    def test_clear_only_original_queue(self):
        """Clear original queue should not affect fragment queue."""
        from partitioncache.queue import clear_original_query_queue

        push_to_original_query_queue("SELECT 1", "zipcode", "integer")
        push_to_query_fragment_queue([("SELECT 2", "hash2")], "zipcode", "integer")

        orig_cleared = clear_original_query_queue()
        assert orig_cleared >= 1

        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] == 0
        assert lengths["query_fragment_queue"] >= 1

    def test_clear_only_fragment_queue(self):
        """Clear fragment queue should not affect original queue."""
        from partitioncache.queue import clear_query_fragment_queue

        push_to_original_query_queue("SELECT 1", "zipcode", "integer")
        push_to_query_fragment_queue([("SELECT 2", "hash2")], "zipcode", "integer")

        frag_cleared = clear_query_fragment_queue()
        assert frag_cleared >= 1

        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 1
        assert lengths["query_fragment_queue"] == 0

    def test_clear_all_queues(self):
        """Clear all should empty both queues."""
        push_to_original_query_queue("SELECT 1", "zipcode", "integer")
        push_to_query_fragment_queue([("SELECT 2", "hash2")], "zipcode", "integer")

        orig_cleared, frag_cleared = clear_all_queues()
        assert orig_cleared >= 1
        assert frag_cleared >= 1

        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] == 0
        assert lengths["query_fragment_queue"] == 0


class TestQueueMultiplePartitions:
    """Test queue operations with multiple partition keys and datatypes."""

    def test_mixed_partition_datatypes(self):
        """Queue should handle different partition key datatypes correctly."""
        # Integer partition
        push_to_original_query_queue(
            "SELECT * FROM t WHERE zipcode = 1",
            partition_key="zipcode",
            partition_datatype="integer",
        )

        # Text partition
        push_to_original_query_queue(
            "SELECT * FROM t WHERE region = 'a'",
            partition_key="region",
            partition_datatype="text",
        )

        # Float partition
        push_to_original_query_queue(
            "SELECT * FROM t WHERE score = 1.5",
            partition_key="score",
            partition_datatype="float",
        )

        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 3

        # Pop all and verify datatypes preserved
        results = []
        for _ in range(3):
            r = pop_from_original_query_queue()
            if r:
                results.append(r)

        assert len(results) == 3
        datatypes = {r[2] for r in results}
        assert "integer" in datatypes
        assert "text" in datatypes
        assert "float" in datatypes

    def test_fragment_queue_preserves_partition_info(self):
        """Fragment queue should preserve partition key and datatype through roundtrip."""
        pairs = [("SELECT 1 FROM t WHERE zipcode = 1001", "test_hash_001")]

        push_to_query_fragment_queue(pairs, "zipcode", "integer")

        result = pop_from_query_fragment_queue()
        assert result is not None
        _, _, partition_key, datatype, _ = result
        assert partition_key == "zipcode"
        assert datatype == "integer"

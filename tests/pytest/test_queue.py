"""
Unit tests for the queue module.

Tests the queue.py wrapper functions that delegate to queue handler implementations.
These tests mock the handler to verify the wrapper's argument forwarding, error handling,
and return value pass-through behavior.

Queue Tuple Structures (defined in AbstractQueueHandler):
    Original queue pop: (query, partition_key, partition_datatype)
    Fragment queue pop: (query, hash, partition_key, partition_datatype, cache_backend)

Push/Pop Asymmetry:
    Push operations accept structured arguments (e.g., list of (query, hash) pairs
    with partition_key as a separate parameter). Pop operations return a flat tuple
    containing all stored fields for a single queue entry, including fields that were
    optional or had defaults during push (e.g., partition_datatype, cache_backend).
"""

import os
from unittest.mock import Mock, patch

import pytest

from partitioncache.queue import (
    clear_all_queues,
    clear_original_query_queue,
    clear_query_fragment_queue,
    get_queue_lengths,
    pop_from_original_query_queue,
    pop_from_query_fragment_queue,
    push_to_original_query_queue,
    push_to_query_fragment_queue,
)


@pytest.fixture
def mock_redis_env():
    """Mock environment variables for Redis configuration."""
    env_vars = {
        "QUERY_QUEUE_PROVIDER": "redis",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "QUERY_QUEUE_REDIS_DB": "1",
        "QUERY_QUEUE_REDIS_QUEUE_KEY": "test_queue",
    }
    with patch.dict(os.environ, env_vars):
        yield


@pytest.fixture
def mock_postgresql_env():
    """Mock environment variables for PostgreSQL configuration."""
    env_vars = {
        "QUERY_QUEUE_PROVIDER": "postgresql",
        "PG_QUEUE_HOST": "localhost",
        "PG_QUEUE_PORT": "5432",
        "PG_QUEUE_USER": "test_user",
        "PG_QUEUE_PASSWORD": "test_password",
        "PG_QUEUE_DB": "test_db",
    }
    with patch.dict(os.environ, env_vars):
        yield


class TestQueueInterface:
    """Test the queue interface with different handlers."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_original_query_queue_success(self, mock_get_handler):
        """Test successful push to original query queue."""
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_original_query_queue("SELECT * FROM table", "test_partition_key")

        assert result is True
        # Wrapper forwards (query, partition_key, partition_datatype=None) to handler
        mock_handler.push_to_original_query_queue.assert_called_once_with("SELECT * FROM table", "test_partition_key", None)

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_original_query_queue_default_partition_key(self, mock_get_handler):
        """Test push to original query queue with default partition key.

        The default partition_key is the literal string "partition_key"
        (see push_to_original_query_queue signature: partition_key="partition_key").
        """
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_original_query_queue("SELECT * FROM table")

        assert result is True
        # partition_key defaults to "partition_key", partition_datatype defaults to None
        mock_handler.push_to_original_query_queue.assert_called_once_with("SELECT * FROM table", "partition_key", None)

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_original_query_queue_error(self, mock_get_handler):
        """Test push to original query queue with error.

        When the handler raises an exception, the wrapper catches it and returns False.
        """
        mock_get_handler.side_effect = Exception("Handler error")

        result = push_to_original_query_queue("SELECT * FROM table")
        assert result is False

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_original_query_queue_success(self, mock_get_handler):
        """Test successful pop from original query queue.

        Pop returns a 3-tuple: (query, partition_key, partition_datatype).
        The partition_datatype field ("integer" here) is stored alongside the query
        in the queue and returned on pop. Its value depends on what was passed during
        push (or the handler's default if None was stored).
        """
        mock_handler = Mock()
        # Pop tuple: (query, partition_key, partition_datatype)
        mock_handler.pop_from_original_query_queue.return_value = ("SELECT * FROM table", "test_partition_key", "integer")
        mock_get_handler.return_value = mock_handler

        result = pop_from_original_query_queue()
        assert result is not None
        query, partition_key, partition_datatype = result
        assert query == "SELECT * FROM table"
        assert partition_key == "test_partition_key"
        assert partition_datatype == "integer"
        mock_handler.pop_from_original_query_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_original_query_queue_empty(self, mock_get_handler):
        """Test pop from empty original query queue returns None."""
        mock_handler = Mock()
        mock_handler.pop_from_original_query_queue.return_value = None
        mock_get_handler.return_value = mock_handler

        result = pop_from_original_query_queue()
        assert result is None


class TestQueryFragmentQueue:
    """Test query fragment queue operations."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_success(self, mock_get_handler):
        """Test successful push to query fragment queue.

        Push accepts a list of (query, hash) pairs. The wrapper forwards these
        along with partition_key, partition_datatype (default "integer"), and
        cache_backend (default None) to the handler.
        """
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM table WHERE id=1", "hash1"), ("SELECT * FROM table WHERE id=2", "hash2")]

        result = push_to_query_fragment_queue(query_hash_pairs, "test_partition_key")
        assert result is True
        # Wrapper forwards: (query_hash_pairs, partition_key, partition_datatype="integer", cache_backend=None)
        mock_handler.push_to_query_fragment_queue.assert_called_once_with(query_hash_pairs, "test_partition_key", "integer", None)

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_default_partition_key(self, mock_get_handler):
        """Test push to query fragment queue with default partition key.

        The default partition_key is the literal string "partition_key"
        (see push_to_query_fragment_queue signature: partition_key="partition_key").
        """
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM table WHERE id=1", "hash1")]

        result = push_to_query_fragment_queue(query_hash_pairs)
        assert result is True
        # partition_key defaults to "partition_key", partition_datatype defaults to "integer"
        mock_handler.push_to_query_fragment_queue.assert_called_once_with(query_hash_pairs, "partition_key", "integer", None)

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_empty_list(self, mock_get_handler):
        """Test push empty list to query fragment queue.

        Pushing an empty list is a valid no-op. The PostgreSQL handler returns True
        immediately (early return for empty lists). The wrapper forwards the empty
        list to the handler without special-casing it.
        """
        mock_handler = Mock()
        # Handler returns True for empty list (PostgreSQL handler has early return for this)
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_query_fragment_queue([], "test_partition_key")
        assert result is True
        mock_handler.push_to_query_fragment_queue.assert_called_once_with([], "test_partition_key", "integer", None)

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_with_cache_backend(self, mock_get_handler):
        """Test push to query fragment queue with explicit cache_backend.

        The cache_backend parameter allows specifying which cache backend should
        process this fragment (e.g., "postgis_h3" for spatial caching).
        """
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM table WHERE id=1", "hash1")]

        result = push_to_query_fragment_queue(query_hash_pairs, "test_partition_key", "geometry", cache_backend="postgis_h3")
        assert result is True
        # All params forwarded: (pairs, partition_key, partition_datatype, cache_backend)
        mock_handler.push_to_query_fragment_queue.assert_called_once_with(query_hash_pairs, "test_partition_key", "geometry", "postgis_h3")

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_query_fragment_queue_with_cache_backend(self, mock_get_handler):
        """Test pop from query fragment queue returns cache_backend.

        Pop returns a 5-tuple: (query, hash, partition_key, partition_datatype, cache_backend).
        The cache_backend field (last element) is non-None when a specific backend was
        specified during push.
        """
        mock_handler = Mock()
        # Pop tuple: (query, hash, partition_key, partition_datatype, cache_backend)
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM table", "test_hash", "test_partition_key", "geometry", "postgis_h3")
        mock_get_handler.return_value = mock_handler

        result = pop_from_query_fragment_queue()
        assert result is not None
        query, hash_val, partition_key, partition_datatype, cache_backend = result
        assert query == "SELECT * FROM table"
        assert hash_val == "test_hash"
        assert partition_key == "test_partition_key"
        assert partition_datatype == "geometry"
        assert cache_backend == "postgis_h3"

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_query_fragment_queue_success(self, mock_get_handler):
        """Test successful pop from query fragment queue.

        Pop returns a 5-tuple: (query, hash, partition_key, partition_datatype, cache_backend).
        When no explicit cache_backend was specified during push, it is None.
        """
        mock_handler = Mock()
        # Pop tuple: (query, hash, partition_key, partition_datatype, cache_backend)
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM table", "test_hash", "test_partition_key", "integer", None)
        mock_get_handler.return_value = mock_handler

        result = pop_from_query_fragment_queue()
        assert result is not None
        query, hash_val, partition_key, partition_datatype, cache_backend = result
        assert query == "SELECT * FROM table"
        assert hash_val == "test_hash"
        assert partition_key == "test_partition_key"
        assert partition_datatype == "integer"
        assert cache_backend is None
        mock_handler.pop_from_query_fragment_queue.assert_called_once()


class TestQueueLengths:
    """Test queue length functionality."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_get_queue_lengths_success(self, mock_get_handler):
        """Test successful queue length retrieval.

        Returns a dict with counts for both queue types. The values 5 and 3 here
        are arbitrary mock return values to verify the wrapper passes through
        whatever the handler returns.
        """
        mock_handler = Mock()
        # Arbitrary mock counts: 5 items in original queue, 3 in fragment queue
        mock_handler.get_queue_lengths.return_value = {"original_query_queue": 5, "query_fragment_queue": 3}
        mock_get_handler.return_value = mock_handler

        result = get_queue_lengths()

        assert result == {"original_query_queue": 5, "query_fragment_queue": 3}
        mock_handler.get_queue_lengths.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_get_queue_lengths_error(self, mock_get_handler):
        """Test queue length retrieval with error.

        On handler error, the wrapper returns zero counts for both queues
        (graceful degradation rather than propagating the exception).
        """
        mock_get_handler.side_effect = Exception("Handler error")

        result = get_queue_lengths()
        assert result == {"original_query_queue": 0, "query_fragment_queue": 0}


class TestQueueClearOperations:
    """Test queue clearing operations.

    Clear operations return the count of entries removed. The mock values (5, 3)
    are arbitrary and verify that the wrapper passes through the handler's return value.
    """

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_original_query_queue(self, mock_get_handler):
        """Test clearing original query queue returns the count of entries removed."""
        mock_handler = Mock()
        # Arbitrary mock: 5 entries were cleared from the original queue
        mock_handler.clear_original_query_queue.return_value = 5
        mock_get_handler.return_value = mock_handler

        result = clear_original_query_queue()
        assert result == 5
        mock_handler.clear_original_query_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_query_fragment_queue(self, mock_get_handler):
        """Test clearing query fragment queue returns the count of entries removed."""
        mock_handler = Mock()
        # Arbitrary mock: 3 entries were cleared from the fragment queue
        mock_handler.clear_query_fragment_queue.return_value = 3
        mock_get_handler.return_value = mock_handler

        result = clear_query_fragment_queue()
        assert result == 3
        mock_handler.clear_query_fragment_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_all_queues(self, mock_get_handler):
        """Test clearing all queues.

        Returns a 2-tuple: (original_queue_cleared, fragment_queue_cleared).
        """
        mock_handler = Mock()
        # Arbitrary mock: (original_queue_cleared=5, fragment_queue_cleared=3)
        mock_handler.clear_all_queues.return_value = (5, 3)
        mock_get_handler.return_value = mock_handler

        result = clear_all_queues()
        original_cleared, fragment_cleared = result
        assert original_cleared == 5
        assert fragment_cleared == 3
        mock_handler.clear_all_queues.assert_called_once()


class TestIntegration:
    """Integration-style tests for queue operations using mocked handlers.

    These tests verify push-then-pop roundtrips. Note the structural asymmetry:
    - Push: accepts structured arguments (query string, partition_key as separate params)
    - Pop: returns a flat tuple with all stored fields for a single entry

    For the original queue:
        Push args: (query, partition_key, partition_datatype=None)
        Pop result: (query, partition_key, partition_datatype)

    For the fragment queue:
        Push args: ([(query, hash), ...], partition_key, partition_datatype, cache_backend)
        Pop result: (query, hash, partition_key, partition_datatype, cache_backend)
        - Pop returns a single item from the list, with hash included in the tuple
        - The list structure from push is flattened: each item is popped individually
    """

    @patch("partitioncache.queue._get_queue_handler")
    def test_queue_roundtrip_original_query_queue(self, mock_get_handler):
        """Test pushing and popping from original query queue.

        Push takes (query, partition_key) while pop returns (query, partition_key, partition_datatype).
        The extra partition_datatype field in pop reflects metadata stored alongside
        the query, even when it was passed as None during push (handlers may apply
        a default, e.g., Redis defaults to "integer" on pop).
        """
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        # Pop returns: (query, partition_key, partition_datatype)
        mock_handler.pop_from_original_query_queue.return_value = ("SELECT * FROM test_table", "test_partition_key", "integer")
        mock_get_handler.return_value = mock_handler

        test_query = "SELECT * FROM test_table"
        test_partition_key = "test_partition_key"

        # Push to queue
        push_result = push_to_original_query_queue(test_query, test_partition_key)
        assert push_result is True

        # Pop from queue - returns additional partition_datatype field
        pop_result = pop_from_original_query_queue()
        assert pop_result is not None
        query, partition_key, partition_datatype = pop_result
        assert query == test_query
        assert partition_key == test_partition_key
        assert partition_datatype == "integer"

    @patch("partitioncache.queue._get_queue_handler")
    def test_queue_roundtrip_query_fragment_queue(self, mock_get_handler):
        """Test pushing and popping from query fragment queue.

        Push takes a list of (query, hash) pairs with partition_key as a separate param.
        Pop returns a single flattened 5-tuple: (query, hash, partition_key, partition_datatype, cache_backend).
        Each list item pushed is popped individually with all metadata included.
        """
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        # Pop returns a single item: (query, hash, partition_key, partition_datatype, cache_backend)
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM test", "test_hash", "test_partition_key", "integer", None)
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM test", "test_hash")]
        test_partition_key = "test_partition_key"

        # Push to queue - accepts list of (query, hash) pairs
        push_result = push_to_query_fragment_queue(query_hash_pairs, test_partition_key)
        assert push_result is True

        # Pop from queue - returns single flattened tuple with all stored fields
        pop_result = pop_from_query_fragment_queue()
        assert pop_result is not None
        query, hash_val, partition_key, partition_datatype, cache_backend = pop_result
        assert query == "SELECT * FROM test"
        assert hash_val == "test_hash"
        assert partition_key == test_partition_key
        assert partition_datatype == "integer"
        assert cache_backend is None

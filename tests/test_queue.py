"""
Unit tests for the queue module.
"""

import os
import pytest
from unittest.mock import Mock, patch
from partitioncache.queue import (
    push_to_original_query_queue,
    push_to_query_fragment_queue,
    pop_from_original_query_queue,
    pop_from_query_fragment_queue,
    push_to_queue,
    get_queue_lengths,
    clear_original_query_queue,
    clear_query_fragment_queue,
    clear_all_queues,
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
        mock_handler.push_to_original_query_queue.assert_called_once_with("SELECT * FROM table", "test_partition_key", "integer")

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_original_query_queue_default_partition_key(self, mock_get_handler):
        """Test push to original query queue with default partition key."""
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_original_query_queue("SELECT * FROM table")

        assert result is True
        mock_handler.push_to_original_query_queue.assert_called_once_with("SELECT * FROM table", "partition_key", "integer")

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_original_query_queue_error(self, mock_get_handler):
        """Test push to original query queue with error."""
        mock_get_handler.side_effect = Exception("Handler error")

        result = push_to_original_query_queue("SELECT * FROM table")
        assert result is False

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_original_query_queue_success(self, mock_get_handler):
        """Test successful pop from original query queue."""
        mock_handler = Mock()
        mock_handler.pop_from_original_query_queue.return_value = ("SELECT * FROM table", "test_partition_key", "integer")
        mock_get_handler.return_value = mock_handler

        result = pop_from_original_query_queue()
        assert result == ("SELECT * FROM table", "test_partition_key", "integer")
        mock_handler.pop_from_original_query_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_original_query_queue_empty(self, mock_get_handler):
        """Test pop from empty original query queue."""
        mock_handler = Mock()
        mock_handler.pop_from_original_query_queue.return_value = None
        mock_get_handler.return_value = mock_handler

        result = pop_from_original_query_queue()
        assert result is None


class TestQueryFragmentQueue:
    """Test query fragment queue operations."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_success(self, mock_get_handler):
        """Test successful push to query fragment queue."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM table WHERE id=1", "hash1"), ("SELECT * FROM table WHERE id=2", "hash2")]

        result = push_to_query_fragment_queue(query_hash_pairs, "test_partition_key")
        assert result is True
        mock_handler.push_to_query_fragment_queue.assert_called_once_with(query_hash_pairs, "test_partition_key", "integer")

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_default_partition_key(self, mock_get_handler):
        """Test push to query fragment queue with default partition key."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM table WHERE id=1", "hash1")]

        result = push_to_query_fragment_queue(query_hash_pairs)
        assert result is True
        mock_handler.push_to_query_fragment_queue.assert_called_once_with(query_hash_pairs, "partition_key", "integer")

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_query_fragment_queue_empty_list(self, mock_get_handler):
        """Test push empty list to query fragment queue."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_query_fragment_queue([], "test_partition_key")
        assert result is True
        mock_handler.push_to_query_fragment_queue.assert_called_once_with([], "test_partition_key", "integer")

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_query_fragment_queue_success(self, mock_get_handler):
        """Test successful pop from query fragment queue."""
        mock_handler = Mock()
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM table", "test_hash", "test_partition_key", "integer")
        mock_get_handler.return_value = mock_handler

        result = pop_from_query_fragment_queue()
        assert result == ("SELECT * FROM table", "test_hash", "test_partition_key", "integer")
        mock_handler.pop_from_query_fragment_queue.assert_called_once()


class TestLegacyFunctions:
    """Test legacy functions and compatibility."""

    @patch("partitioncache.queue.push_to_original_query_queue")
    def test_push_to_queue_calls_original_query_queue(self, mock_push_original):
        """Test that push_to_queue calls push_to_original_query_queue."""
        mock_push_original.return_value = True

        result = push_to_queue("SELECT * FROM table", "test_partition_key")

        assert result is True
        mock_push_original.assert_called_once_with("SELECT * FROM table", "test_partition_key", "integer", None)

    @patch("partitioncache.queue.push_to_original_query_queue")
    def test_push_to_queue_default_partition_key(self, mock_push_original):
        """Test that push_to_queue uses default partition key."""
        mock_push_original.return_value = True

        result = push_to_queue("SELECT * FROM table")

        assert result is True
        mock_push_original.assert_called_once_with("SELECT * FROM table", "partition_key", "integer", None)


class TestQueueLengths:
    """Test queue length functionality."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_get_queue_lengths_success(self, mock_get_handler):
        """Test successful queue length retrieval."""
        mock_handler = Mock()
        mock_handler.get_queue_lengths.return_value = {"original_query_queue": 5, "query_fragment_queue": 3}
        mock_get_handler.return_value = mock_handler

        result = get_queue_lengths()

        assert result == {"original_query_queue": 5, "query_fragment_queue": 3}
        mock_handler.get_queue_lengths.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_get_queue_lengths_error(self, mock_get_handler):
        """Test queue length retrieval with error."""
        mock_get_handler.side_effect = Exception("Handler error")

        result = get_queue_lengths()
        assert result == {"original_query_queue": 0, "query_fragment_queue": 0}


class TestQueueClearOperations:
    """Test queue clearing operations."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_original_query_queue(self, mock_get_handler):
        """Test clearing original query queue."""
        mock_handler = Mock()
        mock_handler.clear_original_query_queue.return_value = 5
        mock_get_handler.return_value = mock_handler

        result = clear_original_query_queue()
        assert result == 5
        mock_handler.clear_original_query_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_query_fragment_queue(self, mock_get_handler):
        """Test clearing query fragment queue."""
        mock_handler = Mock()
        mock_handler.clear_query_fragment_queue.return_value = 3
        mock_get_handler.return_value = mock_handler

        result = clear_query_fragment_queue()
        assert result == 3
        mock_handler.clear_query_fragment_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_all_queues(self, mock_get_handler):
        """Test clearing all queues."""
        mock_handler = Mock()
        mock_handler.clear_all_queues.return_value = (5, 3)
        mock_get_handler.return_value = mock_handler

        result = clear_all_queues()
        assert result == (5, 3)
        mock_handler.clear_all_queues.assert_called_once()


class TestIntegration:
    """Integration tests for queue operations."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_queue_roundtrip_original_query_queue(self, mock_get_handler):
        """Test pushing and popping from original query queue."""
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        mock_handler.pop_from_original_query_queue.return_value = ("SELECT * FROM test_table", "test_partition_key", "integer")
        mock_get_handler.return_value = mock_handler

        test_query = "SELECT * FROM test_table"
        test_partition_key = "test_partition_key"

        # Push to queue
        push_result = push_to_original_query_queue(test_query, test_partition_key)
        assert push_result is True

        # Pop from queue
        pop_result = pop_from_original_query_queue()
        assert pop_result == (test_query, test_partition_key, "integer")

    @patch("partitioncache.queue._get_queue_handler")
    def test_queue_roundtrip_query_fragment_queue(self, mock_get_handler):
        """Test pushing and popping from query fragment queue."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM test", "test_hash", "test_partition_key", "integer")
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM test", "test_hash")]
        test_partition_key = "test_partition_key"

        # Push to queue
        push_result = push_to_query_fragment_queue(query_hash_pairs, test_partition_key)
        assert push_result is True

        # Pop from queue
        pop_result = pop_from_query_fragment_queue()
        assert pop_result == ("SELECT * FROM test", "test_hash", test_partition_key, "integer")

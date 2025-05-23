"""
Unit tests for the queue module.
"""

import os
import pytest
from unittest.mock import Mock, patch
from partitioncache.queue import (
    push_to_incoming_queue,
    push_to_outgoing_queue,
    pop_from_incoming_queue,
    pop_from_outgoing_queue,
    push_to_queue,
    get_queue_lengths,
    clear_incoming_queue,
    clear_outgoing_queue,
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
    def test_push_to_incoming_queue_success(self, mock_get_handler):
        """Test successful push to incoming queue."""
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_incoming_queue("SELECT * FROM table")

        assert result is True
        mock_handler.push_to_original_query_queue.assert_called_once_with("SELECT * FROM table")

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_incoming_queue_error(self, mock_get_handler):
        """Test push to incoming queue with error."""
        mock_get_handler.side_effect = Exception("Handler error")

        result = push_to_incoming_queue("SELECT * FROM table")
        assert result is False

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_incoming_queue_success(self, mock_get_handler):
        """Test successful pop from incoming queue."""
        mock_handler = Mock()
        mock_handler.pop_from_original_query_queue.return_value = "SELECT * FROM table"
        mock_get_handler.return_value = mock_handler

        result = pop_from_incoming_queue()
        assert result == "SELECT * FROM table"
        mock_handler.pop_from_original_query_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_incoming_queue_empty(self, mock_get_handler):
        """Test pop from empty incoming queue."""
        mock_handler = Mock()
        mock_handler.pop_from_original_query_queue.return_value = None
        mock_get_handler.return_value = mock_handler

        result = pop_from_incoming_queue()
        assert result is None


class TestOutgoingQueue:
    """Test outgoing queue operations."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_outgoing_queue_success(self, mock_get_handler):
        """Test successful push to outgoing queue."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM table WHERE id=1", "hash1"), ("SELECT * FROM table WHERE id=2", "hash2")]

        result = push_to_outgoing_queue(query_hash_pairs)
        assert result is True
        mock_handler.push_to_query_fragment_queue.assert_called_once_with(query_hash_pairs)

    @patch("partitioncache.queue._get_queue_handler")
    def test_push_to_outgoing_queue_empty_list(self, mock_get_handler):
        """Test push empty list to outgoing queue."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_get_handler.return_value = mock_handler

        result = push_to_outgoing_queue([])
        assert result is True
        mock_handler.push_to_query_fragment_queue.assert_called_once_with([])

    @patch("partitioncache.queue._get_queue_handler")
    def test_pop_from_outgoing_queue_success(self, mock_get_handler):
        """Test successful pop from outgoing queue."""
        mock_handler = Mock()
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM table", "test_hash")
        mock_get_handler.return_value = mock_handler

        result = pop_from_outgoing_queue()
        assert result == ("SELECT * FROM table", "test_hash")
        mock_handler.pop_from_query_fragment_queue.assert_called_once()


class TestLegacyFunctions:
    """Test legacy functions and compatibility."""

    @patch("partitioncache.queue.push_to_incoming_queue")
    def test_push_to_queue_calls_incoming(self, mock_push_incoming):
        """Test that push_to_queue calls push_to_incoming_queue."""
        mock_push_incoming.return_value = True

        result = push_to_queue("SELECT * FROM table")

        assert result is True
        mock_push_incoming.assert_called_once_with("SELECT * FROM table")


class TestQueueLengths:
    """Test queue length functionality."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_get_queue_lengths_success(self, mock_get_handler):
        """Test successful queue length retrieval."""
        mock_handler = Mock()
        mock_handler.get_queue_lengths.return_value = {"original_query": 5, "query_fragment": 3}
        mock_get_handler.return_value = mock_handler

        result = get_queue_lengths()

        assert result == {"incoming": 5, "outgoing": 3}
        mock_handler.get_queue_lengths.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_get_queue_lengths_error(self, mock_get_handler):
        """Test queue length retrieval with error."""
        mock_get_handler.side_effect = Exception("Handler error")

        result = get_queue_lengths()
        assert result == {"incoming": 0, "outgoing": 0}


class TestQueueClearOperations:
    """Test queue clearing operations."""

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_incoming_queue(self, mock_get_handler):
        """Test clearing incoming queue."""
        mock_handler = Mock()
        mock_handler.clear_original_query_queue.return_value = 5
        mock_get_handler.return_value = mock_handler

        result = clear_incoming_queue()
        assert result == 5
        mock_handler.clear_original_query_queue.assert_called_once()

    @patch("partitioncache.queue._get_queue_handler")
    def test_clear_outgoing_queue(self, mock_get_handler):
        """Test clearing outgoing queue."""
        mock_handler = Mock()
        mock_handler.clear_query_fragment_queue.return_value = 3
        mock_get_handler.return_value = mock_handler

        result = clear_outgoing_queue()
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
    def test_queue_roundtrip_incoming(self, mock_get_handler):
        """Test pushing and popping from incoming queue."""
        mock_handler = Mock()
        mock_handler.push_to_original_query_queue.return_value = True
        mock_handler.pop_from_original_query_queue.return_value = "SELECT * FROM test_table"
        mock_get_handler.return_value = mock_handler

        test_query = "SELECT * FROM test_table"

        # Push to queue
        push_result = push_to_incoming_queue(test_query)
        assert push_result is True

        # Pop from queue
        pop_result = pop_from_incoming_queue()
        assert pop_result == test_query

    @patch("partitioncache.queue._get_queue_handler")
    def test_queue_roundtrip_outgoing(self, mock_get_handler):
        """Test pushing and popping from outgoing queue."""
        mock_handler = Mock()
        mock_handler.push_to_query_fragment_queue.return_value = True
        mock_handler.pop_from_query_fragment_queue.return_value = ("SELECT * FROM test", "test_hash")
        mock_get_handler.return_value = mock_handler

        query_hash_pairs = [("SELECT * FROM test", "test_hash")]

        # Push to queue
        push_result = push_to_outgoing_queue(query_hash_pairs)
        assert push_result is True

        # Pop from queue
        pop_result = pop_from_outgoing_queue()
        assert pop_result == ("SELECT * FROM test", "test_hash")

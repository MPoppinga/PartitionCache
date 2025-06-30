"""
Helper functions to push queries to the queue to be cached asynchronously by an observer process.

This module provides a clean interface for queue operations while delegating the actual
implementation to specific queue handlers via the factory pattern.
"""

import os
from logging import getLogger

from partitioncache.queue_handler import get_queue_handler

logger = getLogger("PartitionCache")

# Global queue handler instance
_queue_handler = None


def _get_queue_handler(queue_provider: str | None = None):
    """Get or create the queue handler instance."""
    global _queue_handler
    if _queue_handler is None:
        _queue_handler = get_queue_handler(queue_provider)
    return _queue_handler


def push_to_original_query_queue(
    query: str, partition_key: str = "partition_key", partition_datatype: str | None = None, queue_provider: str | None = None
) -> bool:
    """
    Push an original query to the original query queue to be processed into fragments.

    Requires the following environment variables to be set, based on the QUERY_QUEUE_PROVIDER:

    QUERY_QUEUE_PROVIDER: "postgresql" (default):
    - PG_QUEUE_HOST
    - PG_QUEUE_PORT
    - PG_QUEUE_USER
    - PG_QUEUE_PASSWORD
    - PG_QUEUE_DB

    QUERY_QUEUE_PROVIDER: "redis":
    - REDIS_HOST
    - REDIS_PORT
    - QUERY_QUEUE_REDIS_DB
    - QUERY_QUEUE_REDIS_QUEUE_KEY (base key, will have _original_query appended)

    Args:
        query (str): The original query to be pushed to the original query queue.
        partition_key (str): The partition key for this query (default: "partition_key").
        partition_datatype (str): The datatype of the partition key (default: None).
        queue_provider (str): The queue provider to use (default: None, which uses the environment variable QUERY_QUEUE_PROVIDER).

    Returns:
        bool: True if the query was pushed to the queue, False otherwise.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.push_to_original_query_queue(query, partition_key, partition_datatype)  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to push query to original query queue: {e}")
        return False


def push_to_query_fragment_queue(
    query_hash_pairs: list[tuple[str, str]], partition_key: str = "partition_key", partition_datatype: str = "integer", queue_provider: str | None = None
) -> bool:
    """
    Push query fragments (as query-hash pairs) directly to the query fragment queue.

    Args:
        query_hash_pairs (list[tuple[str, str]]): List of (query, hash) tuples to push to query fragment queue.
        partition_key (str): The partition key for these query fragments (default: "partition_key").
        partition_datatype (str): The datatype of the partition key (default: "integer").
        queue_provider (str): The queue provider to use (default: None, which uses the environment variable QUERY_QUEUE_PROVIDER).
    Returns:
        bool: True if all fragments were pushed successfully, False otherwise.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.push_to_query_fragment_queue(query_hash_pairs, partition_key, partition_datatype)  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to push fragments to query fragment queue: {e}")
        return False


def pop_from_original_query_queue(queue_provider: str | None = None) -> tuple[str, str, str] | None:
    """
    Pop an original query from the original query queue.

    Returns:
        Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if queue is empty or error occurred.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.pop_from_original_query_queue()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to pop from original query queue: {e}")
        return None


def pop_from_original_query_queue_blocking(timeout: int = 60, queue_provider: str | None = None) -> tuple[str, str, str] | None:
    """
    Pop an original query from the original query queue with blocking wait.
    Uses provider-specific blocking mechanisms when available.

    Args:
        timeout (int): Maximum time to wait in seconds (default: 60)

    Returns:
        Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if timeout or error occurred.
    """
    try:
        handler = _get_queue_handler(queue_provider)

        # Check if handler supports blocking operations
        if hasattr(handler, "pop_from_original_query_queue_blocking"):
            return handler.pop_from_original_query_queue_blocking(timeout)  # type: ignore
        else:
            # Fallback to regular pop
            return handler.pop_from_original_query_queue()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to pop from original query queue with blocking: {e}")
        return None


def pop_from_query_fragment_queue(queue_provider: str | None = None) -> tuple[str, str, str, str] | None:
    """
    Pop a query fragment from the query fragment queue.

    Returns:
        Tuple[str, str, str, str] or None: (query, hash, partition_key, partition_datatype) tuple if available, None if queue is empty or error occurred.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.pop_from_query_fragment_queue()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to pop from query fragment queue: {e}")
        return None


def pop_from_query_fragment_queue_blocking(timeout: int = 60, queue_provider: str | None = None) -> tuple[str, str, str, str] | None:
    """
    Pop a query fragment from the query fragment queue with blocking wait.
    Uses provider-specific blocking mechanisms when available.

    Args:
        timeout (int): Maximum time to wait in seconds (default: 60)

    Returns:
        Tuple[str, str, str, str] or None: (query, hash, partition_key, partition_datatype) tuple if available, None if timeout or error occurred.
    """
    try:
        handler = _get_queue_handler(queue_provider)

        # Check if handler supports blocking operations
        if hasattr(handler, "pop_from_query_fragment_queue_blocking"):
            return handler.pop_from_query_fragment_queue_blocking(timeout)  # type: ignore
        else:
            # Fallback to regular pop
            return handler.pop_from_query_fragment_queue()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to pop from query fragment queue with blocking: {e}")
        return None


def get_queue_lengths(queue_provider: str | None = None) -> dict:
    """
    Get the current lengths of both original query and query fragment queues.

    Returns:
        dict: Dictionary with 'original_query_queue' and 'query_fragment_queue' queue lengths.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.get_queue_lengths()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to get queue lengths: {e}")
        return {"original_query_queue": 0, "query_fragment_queue": 0}


def clear_original_query_queue(queue_provider: str | None = None) -> int:
    """
    Clear the original query queue and return the number of entries cleared.

    Returns:
        int: Number of entries cleared from the original query queue.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.clear_original_query_queue()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to clear original query queue: {e}")
        return 0


def clear_query_fragment_queue(queue_provider: str | None = None) -> int:
    """
    Clear the query fragment queue and return the number of entries cleared.

    Returns:
        int: Number of entries cleared from the query fragment queue.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.clear_query_fragment_queue()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to clear query fragment queue: {e}")
        return 0


def clear_all_queues(queue_provider: str | None = None) -> tuple[int, int]:
    """
    Clear both original query and query fragment queues.

    Returns:
        Tuple[int, int]: (original_query_cleared, query_fragment_cleared) number of entries cleared.
    """
    try:
        handler = _get_queue_handler(queue_provider)
        return handler.clear_all_queues()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Failed to clear all queues: {e}")
        return (0, 0)




def close_queue_handler() -> None:
    """
    Close the queue handler and release any resources.
    Useful for cleanup in applications that use the queue functionality.
    """
    global _queue_handler
    if _queue_handler is not None:
        try:
            _queue_handler.close()
        except Exception as e:
            logger.error(f"Error closing queue handler: {e}")
        finally:
            _queue_handler = None


def reset_queue_handler() -> None:
    """
    Reset the queue handler singleton for testing.
    Forces creation of a new handler on next access.
    """
    global _queue_handler
    if _queue_handler is not None:
        try:
            _queue_handler.close()
        except Exception:
            pass  # Ignore errors during cleanup
        _queue_handler = None


def get_queue_provider_name() -> str:
    """
    Get the current queue provider type.

    Returns:
        str: The queue provider type ('postgresql' or 'redis')
    """

    return os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")

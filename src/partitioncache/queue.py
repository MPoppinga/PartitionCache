"""
Helper functions to push queries to the queue to be cached asynchronously by an observer process.

This module provides a clean interface for queue operations while delegating the actual
implementation to specific queue handlers via the factory pattern.
"""

from typing import List, Tuple, Optional
from logging import getLogger

from partitioncache.queue_handler import get_queue_handler

logger = getLogger("PartitionCache")

# Global queue handler instance
_queue_handler = None


def _get_queue_handler():
    """Get or create the queue handler instance."""
    global _queue_handler
    if _queue_handler is None:
        _queue_handler = get_queue_handler()
    return _queue_handler


def push_to_incoming_queue(query: str) -> bool:
    """
    Push an original query to the incoming queue to be processed into fragments.

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
        query (str): The original query to be pushed to the incoming queue.

    Returns:
        bool: True if the query was pushed to the queue, False otherwise.
    """
    try:
        handler = _get_queue_handler()
        return handler.push_to_original_query_queue(query)
    except Exception as e:
        logger.error(f"Failed to push query to incoming queue: {e}")
        return False


def push_to_outgoing_queue(query_hash_pairs: List[Tuple[str, str]]) -> bool:
    """
    Push query fragments (as query-hash pairs) directly to the outgoing queue.

    Args:
        query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to outgoing queue.

    Returns:
        bool: True if all fragments were pushed successfully, False otherwise.
    """
    try:
        handler = _get_queue_handler()
        return handler.push_to_query_fragment_queue(query_hash_pairs)
    except Exception as e:
        logger.error(f"Failed to push fragments to outgoing queue: {e}")
        return False


def pop_from_incoming_queue() -> Optional[str]:
    """
    Pop an original query from the incoming queue.

    Returns:
        str or None: The query string if available, None if queue is empty or error occurred.
    """
    try:
        handler = _get_queue_handler()
        return handler.pop_from_original_query_queue()
    except Exception as e:
        logger.error(f"Failed to pop from incoming queue: {e}")
        return None


def pop_from_outgoing_queue() -> Optional[Tuple[str, str]]:
    """
    Pop a query fragment from the outgoing queue.

    Returns:
        Tuple[str, str] or None: (query, hash) tuple if available, None if queue is empty or error occurred.
    """
    try:
        handler = _get_queue_handler()
        return handler.pop_from_query_fragment_queue()
    except Exception as e:
        logger.error(f"Failed to pop from outgoing queue: {e}")
        return None


def get_queue_lengths() -> dict:
    """
    Get the current lengths of both incoming and outgoing queues.

    Returns:
        dict: Dictionary with 'incoming' and 'outgoing' queue lengths for backward compatibility.
    """
    try:
        handler = _get_queue_handler()
        lengths = handler.get_queue_lengths()
        # Convert to old key names for backward compatibility
        return {"incoming": lengths.get("original_query", 0), "outgoing": lengths.get("query_fragment", 0)}
    except Exception as e:
        logger.error(f"Failed to get queue lengths: {e}")
        return {"incoming": 0, "outgoing": 0}


def clear_incoming_queue() -> int:
    """
    Clear the incoming queue and return the number of entries cleared.

    Returns:
        int: Number of entries cleared from the incoming queue.
    """
    try:
        handler = _get_queue_handler()
        return handler.clear_original_query_queue()
    except Exception as e:
        logger.error(f"Failed to clear incoming queue: {e}")
        return 0


def clear_outgoing_queue() -> int:
    """
    Clear the outgoing queue and return the number of entries cleared.

    Returns:
        int: Number of entries cleared from the outgoing queue.
    """
    try:
        handler = _get_queue_handler()
        return handler.clear_query_fragment_queue()
    except Exception as e:
        logger.error(f"Failed to clear outgoing queue: {e}")
        return 0


def clear_all_queues() -> Tuple[int, int]:
    """
    Clear both incoming and outgoing queues.

    Returns:
        Tuple[int, int]: (incoming_cleared, outgoing_cleared) number of entries cleared.
    """
    try:
        handler = _get_queue_handler()
        return handler.clear_all_queues()
    except Exception as e:
        logger.error(f"Failed to clear all queues: {e}")
        return (0, 0)


def push_to_queue(query: str) -> bool:
    """
    Legacy function for backward compatibility. Pushes to incoming queue.

    Args:
        query (str): The query to be pushed to the queue.

    Returns:
        bool: True if the query was pushed to the queue, False otherwise.
    """
    return push_to_incoming_queue(query)


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


# Legacy function to support existing imports
def push_fragments_to_outgoing_queue(query_hash_pairs: List[Tuple[str, str]]) -> bool:
    """
    Legacy function name for backward compatibility with tests and existing code.

    Args:
        query_hash_pairs: List of (query, hash) tuples to push to outgoing queue.

    Returns:
        bool: True if all fragments were pushed successfully, False otherwise.
    """
    return push_to_outgoing_queue(query_hash_pairs)

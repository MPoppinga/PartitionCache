"""
Redis queue handler implementation.
"""

import json
from typing import List, Tuple, Optional, cast
from logging import getLogger

from partitioncache.queue_handler.abstract import AbstractQueueHandler

logger = getLogger("PartitionCache")


class RedisQueueHandler(AbstractQueueHandler):
    """
    Redis implementation of the queue handler.
    Uses Redis lists for original query and query fragment queues.
    """

    def __init__(self, host: str, port: int, db: int, password: Optional[str] = None, queue_key: str = "query_queue"):
        """
        Initialize the Redis queue handler.

        Args:
            host (str): Redis host
            port (int): Redis port
            db (int): Redis database number
            password (Optional[str]): Redis password
            queue_key (str): Base key for queue naming
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.queue_key = queue_key
        self._redis_client = None

    def _get_redis_connection(self):
        """Get Redis connection with proper configuration."""
        if self._redis_client is None:
            import redis

            connection_params = {
                "host": self.host,
                "port": self.port,
                "db": self.db,
            }

            if self.password:
                connection_params["password"] = self.password

            self._redis_client = redis.Redis(**connection_params)

        return self._redis_client

    def _get_queue_key(self, suffix: str) -> str:
        """Get queue key with appropriate suffix."""
        return f"{self.queue_key}_{suffix}"

    def push_to_original_query_queue(self, query: str) -> bool:
        """
        Push an original query to the original query queue to be processed into fragments.

        Args:
            query (str): The original query to be pushed to the original query queue.

        Returns:
            bool: True if the query was pushed successfully, False otherwise.
        """
        try:
            r = self._get_redis_connection()
            queue_key = self._get_queue_key("original_query")
            r.rpush(queue_key, query)
            logger.debug(f"Pushed query to Redis original query queue: {queue_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to push query to Redis original query queue: {e}")
            return False

    def push_to_query_fragment_queue(self, query_hash_pairs: List[Tuple[str, str]]) -> bool:
        """
        Push query fragments (as query-hash pairs) directly to the query fragment queue.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to fragment queue.

        Returns:
            bool: True if all fragments were pushed successfully, False otherwise.
        """
        try:
            r = self._get_redis_connection()
            queue_key = self._get_queue_key("query_fragment")

            # Push each query-hash pair as a JSON-encoded string
            for query, hash_value in query_hash_pairs:
                fragment_data = json.dumps({"query": query, "hash": hash_value})
                r.rpush(queue_key, fragment_data)

            logger.debug(f"Pushed {len(query_hash_pairs)} fragments to Redis query fragment queue: {queue_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to push fragments to Redis query fragment queue: {e}")
            return False

    def pop_from_original_query_queue(self) -> Optional[str]:
        """
        Pop an original query from the original query queue.

        Returns:
            str or None: The query string if available, None if queue is empty or error occurred.
        """
        try:
            r = self._get_redis_connection()
            queue_key = self._get_queue_key("original_query")
            result = r.blpop([queue_key], timeout=1)  # 1 second timeout
            if result is not None and isinstance(result, (list, tuple)) and len(result) >= 2:
                return result[1].decode("utf-8")
            return None
        except Exception as e:
            logger.error(f"Failed to pop from Redis original query queue: {e}")
            return None

    def pop_from_query_fragment_queue(self) -> Optional[Tuple[str, str]]:
        """
        Pop a query fragment from the query fragment queue.

        Returns:
            Tuple[str, str] or None: (query, hash) tuple if available, None if queue is empty or error occurred.
        """
        try:
            r = self._get_redis_connection()
            queue_key = self._get_queue_key("query_fragment")
            result = r.blpop([queue_key], timeout=1)  # 1 second timeout
            if result is not None and isinstance(result, (list, tuple)) and len(result) >= 2:
                fragment_data = json.loads(result[1].decode("utf-8"))
                return fragment_data["query"], fragment_data["hash"]
            return None
        except Exception as e:
            logger.error(f"Failed to pop from Redis query fragment queue: {e}")
            return None

    def get_queue_lengths(self) -> dict:
        """
        Get the current lengths of both original query and query fragment queues.

        Returns:
            dict: Dictionary with 'original_query' and 'query_fragment' queue lengths.
        """
        try:
            r = self._get_redis_connection()
            original_query_key = self._get_queue_key("original_query")
            query_fragment_key = self._get_queue_key("query_fragment")

            original_query_len = r.llen(original_query_key)
            query_fragment_len = r.llen(query_fragment_key)

            # Cast to int since we know this is sync Redis
            original_query_count = int(cast(int, original_query_len)) if original_query_len is not None else 0
            query_fragment_count = int(cast(int, query_fragment_len)) if query_fragment_len is not None else 0

            return {"original_query": original_query_count, "query_fragment": query_fragment_count}
        except Exception as e:
            logger.error(f"Failed to get Redis queue lengths: {e}")
            return {"original_query": 0, "query_fragment": 0}

    def clear_original_query_queue(self) -> int:
        """
        Clear the original query queue and return the number of entries cleared.

        Returns:
            int: Number of entries cleared from the original query queue.
        """
        try:
            r = self._get_redis_connection()
            original_query_key = self._get_queue_key("original_query")
            length_result = r.llen(original_query_key)
            length = int(cast(int, length_result)) if length_result is not None else 0
            deleted = r.delete(original_query_key)
            return length if deleted else 0
        except Exception as e:
            logger.error(f"Failed to clear Redis original query queue: {e}")
            return 0

    def clear_query_fragment_queue(self) -> int:
        """
        Clear the query fragment queue and return the number of entries cleared.

        Returns:
            int: Number of entries cleared from the query fragment queue.
        """
        try:
            r = self._get_redis_connection()
            query_fragment_key = self._get_queue_key("query_fragment")
            length_result = r.llen(query_fragment_key)
            length = int(cast(int, length_result)) if length_result is not None else 0
            deleted = r.delete(query_fragment_key)
            return length if deleted else 0
        except Exception as e:
            logger.error(f"Failed to clear Redis query fragment queue: {e}")
            return 0

    def clear_all_queues(self) -> Tuple[int, int]:
        """
        Clear both original query and query fragment queues.

        Returns:
            Tuple[int, int]: (original_query_cleared, query_fragment_cleared) number of entries cleared.
        """
        try:
            r = self._get_redis_connection()

            # Clear both queues
            original_query_key = self._get_queue_key("original_query")
            query_fragment_key = self._get_queue_key("query_fragment")

            original_query_len_result = r.llen(original_query_key)
            query_fragment_len_result = r.llen(query_fragment_key)

            original_query_length = int(cast(int, original_query_len_result)) if original_query_len_result is not None else 0
            query_fragment_length = int(cast(int, query_fragment_len_result)) if query_fragment_len_result is not None else 0

            original_query_deleted = r.delete(original_query_key)
            query_fragment_deleted = r.delete(query_fragment_key)

            original_query_cleared = original_query_length if original_query_deleted else 0
            query_fragment_cleared = query_fragment_length if query_fragment_deleted else 0

            return (original_query_cleared, query_fragment_cleared)
        except Exception as e:
            logger.error(f"Failed to clear Redis queues: {e}")
            return (0, 0)

    def close(self) -> None:
        """
        Close the queue handler and release any resources.
        """
        if self._redis_client:
            try:
                self._redis_client.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                self._redis_client = None

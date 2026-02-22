"""
Abstract base class for queue handlers.
"""

from abc import ABC, abstractmethod


class AbstractQueueHandler(ABC):
    """
    Abstract class for queue handlers.
    Queue handlers are responsible for managing original query and query fragment queues
    for asynchronous query processing in the partition cache system.
    """

    @abstractmethod
    def __init__(self, **kwargs):
        """
        Initialize the queue handler.

        Args:
            **kwargs: Configuration parameters specific to the queue implementation
        """
        pass

    @abstractmethod
    def push_to_original_query_queue(self, query: str, partition_key: str, partition_datatype: str | None = None) -> bool:
        """
        Push an original query to the original query queue to be processed into fragments.

        Args:
            query (str): The original query to be pushed to the original query queue.
            partition_key (str): The partition key for this query.
            partition_datatype (str): The datatype of the partition key (default: None).

        Returns:
            bool: True if the query was pushed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def push_to_query_fragment_queue(self, query_hash_pairs: list[tuple[str, str]], partition_key: str, partition_datatype: str | None = None, cache_backend: str | None = None) -> bool:
        """
        Push query fragments (as query-hash pairs) directly to the query fragment queue.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to fragment queue.
            partition_key (str): The partition key for these query fragments.
            partition_datatype (str): The datatype of the partition key (default: None).
            cache_backend (str): The cache backend to use for processing (default: None, uses processor config).

        Returns:
            bool: True if all fragments were pushed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def pop_from_original_query_queue(self) -> tuple[str, str, str] | None:
        """
        Pop an original query from the original query queue.

        Returns:
            Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if queue is empty or error occurred.
        """
        pass

    @abstractmethod
    def pop_from_query_fragment_queue(self) -> tuple[str, str, str, str, str | None] | None:
        """
        Pop a query fragment from the query fragment queue.

        Returns:
            Tuple[str, str, str, str, str | None] or None: (query, hash, partition_key, partition_datatype, cache_backend) tuple if available, None if queue is empty or error occurred.
        """
        pass

    @abstractmethod
    def get_queue_lengths(self) -> dict:
        """
        Get the current lengths of both original query and query fragment queues.

        Returns:
            dict: Dictionary with 'original_query_queue' and 'query_fragment_queue' queue lengths.
        """
        pass

    @abstractmethod
    def clear_original_query_queue(self) -> int:
        """
        Clear the original query queue and return the number of entries cleared.

        Returns:
            int: Number of entries cleared from the original query queue.
        """
        pass

    @abstractmethod
    def clear_query_fragment_queue(self) -> int:
        """
        Clear the query fragment queue and return the number of entries cleared.

        Returns:
            int: Number of entries cleared from the query fragment queue.
        """
        pass

    @abstractmethod
    def clear_all_queues(self) -> tuple[int, int]:
        """
        Clear both original query and query fragment queues.

        Returns:
            Tuple[int, int]: (original_query_cleared, query_fragment_cleared) number of entries cleared.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close the queue handler and release any resources.
        """
        pass


class AbstractPriorityQueueHandler(AbstractQueueHandler):
    """
    Abstract class for priority-enabled queue handlers.
    Extends basic queue functionality with priority support.
    """

    @abstractmethod
    def push_to_original_query_queue_with_priority(self, query: str, partition_key: str, priority: int = 1, partition_datatype: str | None = None) -> bool:
        """
        Push an original query to the original query queue with specified priority.
        If the query already exists, increment its priority.

        Args:
            query (str): The original query to be pushed to the original query queue.
            partition_key (str): The partition key for this query.
            priority (int): Initial priority for the query (default: 1).
            partition_datatype (str): The datatype of the partition key (default: None).

        Returns:
            bool: True if the query was pushed/updated successfully, False otherwise.
        """
        pass

    @abstractmethod
    def push_to_query_fragment_queue_with_priority(
        self, query_hash_pairs: list[tuple[str, str]], partition_key: str, priority: int = 1, partition_datatype: str | None = None, cache_backend: str | None = None
    ) -> bool:
        """
        Push query fragments with specified priority.
        If a fragment already exists, increment its priority.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to fragment queue.
            partition_key (str): The partition key for these query fragments.
            priority (int): Initial priority for the fragments (default: 1).
            partition_datatype (str): The datatype of the partition key (default: None).
            cache_backend (str): The cache backend to use for processing (default: None, uses processor config).

        Returns:
            bool: True if all fragments were pushed/updated successfully, False otherwise.
        """
        pass

    def push_to_original_query_queue(self, query: str, partition_key: str, partition_datatype: str | None = None) -> bool:
        """
        Default implementation using priority system with priority=1.
        """
        return self.push_to_original_query_queue_with_priority(query, partition_key, 1, partition_datatype)

    def push_to_query_fragment_queue(self, query_hash_pairs: list[tuple[str, str]], partition_key: str, partition_datatype: str | None = None, cache_backend: str | None = None) -> bool:
        """
        Default implementation using priority system with priority=1.
        """
        return self.push_to_query_fragment_queue_with_priority(query_hash_pairs, partition_key, 1, partition_datatype, cache_backend)

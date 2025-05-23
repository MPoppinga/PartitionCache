"""
Abstract base class for queue handlers.
"""

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional


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
    def push_to_original_query_queue(self, query: str) -> bool:
        """
        Push an original query to the original query queue to be processed into fragments.

        Args:
            query (str): The original query to be pushed to the original query queue.

        Returns:
            bool: True if the query was pushed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def push_to_query_fragment_queue(self, query_hash_pairs: List[Tuple[str, str]]) -> bool:
        """
        Push query fragments (as query-hash pairs) directly to the query fragment queue.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to fragment queue.

        Returns:
            bool: True if all fragments were pushed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def pop_from_original_query_queue(self) -> Optional[str]:
        """
        Pop an original query from the original query queue.
        
        Returns:
            str or None: The query string if available, None if queue is empty or error occurred.
        """
        pass

    @abstractmethod
    def pop_from_query_fragment_queue(self) -> Optional[Tuple[str, str]]:
        """
        Pop a query fragment from the query fragment queue.
        
        Returns:
            Tuple[str, str] or None: (query, hash) tuple if available, None if queue is empty or error occurred.
        """
        pass

    @abstractmethod
    def get_queue_lengths(self) -> dict:
        """
        Get the current lengths of both original query and query fragment queues.
        
        Returns:
            dict: Dictionary with 'original_query' and 'query_fragment' queue lengths.
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
    def clear_all_queues(self) -> Tuple[int, int]:
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
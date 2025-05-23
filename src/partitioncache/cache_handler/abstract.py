from abc import ABC, abstractmethod


class AbstractCacheHandler(ABC):
    """
    Abstract class for cache handlers.
    The Cache handlers are responsible for storing and retrieving partition keys from the cache.
    They contain the logic for storing and retrieving the partition keys from the cache, as well as the logic for intersecting sets of partition keys.
    """

    @abstractmethod
    def __init__(self):
        """
        Initialize the cache handler.
        """

    @abstractmethod
    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        """
        Retrieve a set of partition keys from the cache associated with the given key.

        Args:
            key (str): The key to look up in the cache.
            settype (type, optional): The type of set to retrieve (int or str). Defaults to int.

        Returns:
            set[int] | set[str] | None: The set of partition keys associated with the key, or None if not found.
        """
        raise NotImplementedError

    @abstractmethod
    def get_intersected(self, keys: set[str]) -> tuple[set[int] | set[str] | None, int]:
        """
        Get the intersection of all sets in the cache associated with the given keys.

        Args:
            keys (set[str]): A set of keys to intersect.

        Returns:
            tuple[set[int] | set[str] | None, int]: A tuple containing the intersected set and the count of matched keys.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        """
        Store a set in the cache associated with the given key.

        Args:
            key (str): The key to associate with the set.
            value (set[int] | set[str]): The set to store.
            settype (type, optional): The type of set (int or str). Defaults to int.
        """
        raise NotImplementedError

    @abstractmethod
    def set_null(self, key: str) -> None:
        """
        Store a null value in the cache associated with the given key, indicating that no valid results were stored for the given key.

        Args:
            key (str): The key to associate with the null value.
        """
        raise NotImplementedError

    @abstractmethod
    def is_null(self, key: str) -> bool:
        """
        Check if a key is associated with a null value in the cache.
        A null value indicates that the result is invalid and shall not be used for retrieval.
        This indicates for example that the query time out or the resultset was too large to be stored.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the key is associated with a null value, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Delete a key and its associated set of partition keys from the cache.

        Args:
            key (str): The key to delete.
        """
        raise NotImplementedError

    @abstractmethod
    def get_all_keys(self) -> list:
        """
        Retrieve all keys from the cache.

        Returns:
            list: A list of all keys in the cache.
        """
        raise NotImplementedError

    @abstractmethod
    def filter_existing_keys(self, keys: set) -> set:
        """
        Filter and return the set of keys that exist in the cache of the given set.

        Args:
            keys (set): A set of keys to verify.

        Returns:
            set: The subset set of keys that exist in the cache.
        """
        raise NotImplementedError

    def compact(self):
        """
        Compact the cache to optimize storage as recommended for some cache handlers.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close the cache handler and release any resources allocated by the cache handler.
        """
        raise NotImplementedError


class AbstractCacheHandler_Lazy(AbstractCacheHandler):
    """
    Abstract class for cache handlers that support lazy intersection.
    """

    @abstractmethod
    def get_intersected_lazy(self, keys: set[str]) -> tuple[str | None, int]:
        """
        Lazily get the intersection representation of all sets in the cache associated with the given keys.

        This method returns not the partition keys themselves, but a representation that can be used to reconstruct the partition keys on the fly.
        For example the SQL which is used to retrieve the partition keys.

        Only possible if the cache handler supports lazy intersection.

        Args:
            keys (set[str]): A set of keys to intersect.
        """
        raise NotImplementedError


class AbstractCacheHandler_Query(ABC):
    """
    Abstract class for cache handlers that store queries.
    """

    @abstractmethod
    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> None:
        """
        Store a query in the cache associated with the given key.

        Args:
            key (str): The key to associate with the query.
            querytext (str): The query to store.
            partition_key (str): The partition key for this query (default: "partition_key").
        """
        raise NotImplementedError

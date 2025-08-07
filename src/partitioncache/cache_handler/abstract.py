from abc import ABC, abstractmethod
from datetime import datetime
from logging import getLogger

logger = getLogger("PartitionCache")


class AbstractCacheHandler(ABC):
    """
    Abstract class for cache handlers.
    The Cache handlers are responsible for storing and retrieving partition keys from the cache.
    They contain the logic for storing and retrieving the partition keys from the cache, as well as the logic for intersecting sets of partition keys.
    The Cache handler also store the original query together with the last seen timestamp.
    """

    @classmethod
    @abstractmethod
    def get_supported_datatypes(cls) -> set[str]:
        """
        Get the set of datatypes supported by this cache handler.

        Returns:
            Set[str]: Set of supported datatype strings (e.g., {"integer", "text", "float", "timestamp"})
        """
        raise NotImplementedError

    @classmethod
    def supports_datatype(cls, datatype: str) -> bool:
        """
        Check if this cache handler supports a specific datatype.

        Args:
            datatype (str): The datatype to check (e.g., "integer", "text", "float", "timestamp")

        Returns:
            bool: True if supported, False otherwise
        """
        return datatype in cls.get_supported_datatypes()

    @classmethod
    def validate_datatype_compatibility(cls, datatype: str) -> None:
        """
        Validate that this cache handler supports the given Python type.

        Args:
            settype (str): The datatype to validate (e.g., "integer", "text", "float", "timestamp")

        Raises:
            ValueError: If the type is not supported
        """
        if not cls.supports_datatype(datatype):
            type_name = getattr(datatype, "__name__", str(datatype))
            handler_name = cls.__name__
            raise ValueError(f"Cache handler '{handler_name}' does not support Python type '{type_name}' ")

    @abstractmethod
    def __init__(self):
        """
        Initialize the cache handler.
        """

    @abstractmethod
    def __repr__(self) -> str:
        """
        Return a string representation of the cache handler.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """
        Retrieve a set of partition keys from the cache associated with the given key.

        Args:
            key (str): The key to look up in the cache.
            partition_key (str, optional): The partition key namespace. Defaults to "partition_key".

        Returns:
            set[int] | set[str] | set[float] | set[datetime] | None: The set of partition keys associated with the key, or None if not found.
        """
        raise NotImplementedError

    @abstractmethod
    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """
        Get the intersection of all sets in the cache associated with the given keys.

        Args:
            keys (set[str]): A set of keys to intersect.
            partition_key (str, optional): The partition key. Defaults to "partition_key".

        Returns:
            tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]: A tuple containing the intersected set and the count of matched keys.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str, partition_key: str = "partition_key", check_query: bool = False) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key (str): The hash to check.
            partition_key (str, optional): The partition key (column). Defaults to "partition_key".
            check_query (bool, optional): If False, only check cache entry existence (fast).
                                         If True, check query metadata first:
                                         - If query doesn't exist: return False
                                         - If query exists with 'ok' status: also check cache entry exists
                                         - If query exists with 'timeout'/'failed' status: return True (no cache check)
                                         Defaults to False.

        Returns:
            bool: True if the hash exists (and meets criteria), False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def set_cache(self, key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """
        Store a set of partition key identifiers in the cache associated with the given key.
        The type is inferred from the metadata table, or if not present, from the type of partition_key_identifiers.

        Args:
            key (str): The key to associate with the partition key identifiers.
            partition_key_identifiers (set[int] | set[str] | set[float] | set[datetime]): The set of partition key identifiers to store.
            partition_key (str, optional): The partition key (column) namespace. Defaults to "partition_key".

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Store a null value in the cache associated with the given key, indicating that no valid results were stored for the given key.

        Args:
            key (str): The key to associate with the null value.
            partition_key (str, optional): The partition key namespace. Defaults to "partition_key".

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def is_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Check if a key is associated with a null value in the cache.
        A null value indicates that the result is invalid and shall not be used for retrieval.
        This indicates for example that the query time out or the resultset was too large to be stored.

        Args:
            key (str): The key to check.
            partition_key (str, optional): The partition key namespace. Defaults to "partition_key".

        Returns:
            bool: True if the key is associated with a null value, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Delete a key and its associated set of partition keys from the cache.

        Args:
            key (str): The key to delete.
            partition_key (str, optional): The partition key namespace. Defaults to "partition_key".

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def get_all_keys(self, partition_key: str) -> list:
        """
        Retrieve all keys from the cache for a specific partition key.

        Args:
            partition_key (str): The partition key to filter by.

        Returns:
            list: A list of all keys in the cache for the specified partition.
        """
        raise NotImplementedError

    @abstractmethod
    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key", check_query: bool = False) -> set:
        """
        Filter and return the set of keys that exist in the cache of the given set.

        Args:
            keys (set): A set of hashes to verify.
            partition_key (str, optional): The partition key (column). Defaults to "partition_key".
            check_query (bool, optional): If False, only check cache entry existence (fast).
                                         If True, check query metadata first:
                                         - If query doesn't exist: exclude hash
                                         - If query exists with 'ok' status: also check cache entry exists
                                         - If query exists with 'timeout'/'failed' status: include hash (no cache check)
                                         Defaults to False.

        Returns:
            set: The subset set of hashes that exist in the cache (and meet criteria).
        """
        raise NotImplementedError

    @abstractmethod
    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """
        Store a query in the cache associated with the given key.

        Args:
            key (str): The key to associate with the query.
            querytext (str): The query to store.
            partition_key (str): The partition key for this query (default: "partition_key").

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def get_query(self, key: str, partition_key: str = "partition_key") -> str | None:
        """
        Retrieve the query text associated with the given key.

        Args:
            key (str): The key to look up in the queries table.
            partition_key (str): The partition key for this query (default: "partition_key").

        Returns:
            str | None: The query text if found, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def get_all_queries(self, partition_key: str) -> list[tuple[str, str]]:
        """
        Retrieve all query hash and text pairs for a specific partition.

        Args:
            partition_key (str): The partition key to filter by.

        Returns:
            list[tuple[str, str]]: List of (query_hash, query_text) tuples for the partition.
        """
        raise NotImplementedError

    @abstractmethod
    def get_datatype(self, partition_key: str) -> str | None:
        """
        Get the datatype of the cache handler. If the partition key is not set up, return None.
        """
        raise NotImplementedError

    @abstractmethod
    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """
        Register a partition key with the cache handler.
        The kwargs are used to pass additional information to the cache handler.
        For example the bitsize for the bit array cache handler.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close the cache handler and release any resources allocated by the cache handler.
        """
        raise NotImplementedError

    @abstractmethod
    def get_partition_keys(self) -> list[tuple[str, str]]:
        """
        Get all partition keys and their datatypes.
        """
        raise NotImplementedError

    @abstractmethod
    def get_instance(cls, *args, **kwargs) -> "AbstractCacheHandler":
        """
        Get an instance of the cache handler.
        """
        raise NotImplementedError

    @abstractmethod
    def set_query_status(self, key: str, partition_key: str = "partition_key", status: str = "ok") -> bool:
        """
        Set the status of a query in the cache.

        Args:
            key (str): The query hash to set status for.
            partition_key (str, optional): The partition key. Defaults to "partition_key".
            status (str, optional): The status to set ('ok', 'timeout', 'failed'). Defaults to "ok".

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def get_query_status(self, key: str, partition_key: str = "partition_key") -> str | None:
        """
        Get the status of a query from the cache.

        Args:
            key (str): The query hash to check.
            partition_key (str, optional): The partition key. Defaults to "partition_key".

        Returns:
            str | None: The status ('ok', 'timeout', 'failed') or None if not found.
        """
        raise NotImplementedError

    def set_entry(
        self,
        key: str,
        partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime],
        query_text: str,
        partition_key: str = "partition_key",
        force_update: bool = False,
    ) -> bool:
        """
        High-level method that atomically stores cache data and query metadata.

        This is the preferred way to populate the cache as it ensures both cache data
        and query metadata are stored consistently.

        Args:
            key (str): Cache key (query hash).
            partition_key_identifiers (set): Set of partition key identifiers to cache.
            query_text (str): SQL query text to store.
            partition_key (str, optional): Partition key (column) namespace. Defaults to "partition_key".
            force_update (bool, optional): If True, always update cache data.
                                          If False, only update metadata if cache entry exists.
                                          Defaults to False.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Check if entry already exists
            if not force_update and self.exists(key, partition_key, check_query=True):
                # Entry exists, only update query metadata
                return self.set_query(key, query_text, partition_key)
            else:
                # Entry doesn't exist or force_update=True, set both
                success_data = self.set_cache(key, partition_key_identifiers, partition_key)
                success_query = self.set_query(key, query_text, partition_key)
                return success_data and success_query
        except Exception as e:
            # Log error but don't raise exception to maintain API consistency
            logger.error(f"Failed to set cache entry for key {key}: {e}")
            return False


class AbstractCacheHandler_Lazy(AbstractCacheHandler):
    """
    Abstract class for cache handlers that support lazy intersection and lazy insertion.
    """

    @abstractmethod
    def get_intersected_lazy(self, keys: set[str], partition_key: str = "partition_key") -> tuple[str | None, int]:
        """
        Lazily get the intersection representation of all sets in the cache associated with the given keys.

        This method returns not the partition keys themselves, but a representation that can be used to reconstruct the partition keys on the fly.
        For example the SQL which is used to retrieve the partition keys.

        Only possible if the cache handler supports lazy intersection.

        Args:
            keys (set[str]): A set of keys to intersect.
            partition_key (str, optional): The partition key. Defaults to "partition_key".
        """
        raise NotImplementedError

    @abstractmethod
    def set_cache_lazy(self, key: str, query: str, partition_key: str = "partition_key") -> bool:
        """
        Store partition key identifiers in cache by executing the provided query directly.

        This method allows inserting cache data without moving partition keys through Python,
        providing better performance for large result sets. The query should return
        partition key values that will be stored in the cache.

        Args:
            key (str): Cache key (query hash).
            query (str): SQL query that returns partition key values to cache.
            partition_key (str, optional): Partition key namespace. Defaults to "partition_key".

        Returns:
            bool: True if successful, False otherwise.

        Note:
            The query should return a single column containing partition key values.
            For safety, queries containing DELETE or DROP statements will be rejected.
        """
        raise NotImplementedError

    def set_entry_lazy(
        self,
        key: str,
        query: str,
        query_text: str,
        partition_key: str = "partition_key",
        force_update: bool = False,
    ) -> bool:
        """
        High-level method that atomically stores cache data using lazy insertion and query metadata.

        This is the preferred way to populate the cache with lazy insertion as it ensures both
        cache data and query metadata are stored consistently without moving data through Python.

        Args:
            key (str): Cache key (query hash).
            query (str): SQL query that returns partition key values to cache.
            query_text (str): SQL query text to store for metadata.
            partition_key (str, optional): Partition key namespace. Defaults to "partition_key".
            force_update (bool, optional): If True, always update cache data.
                                          If False, only update metadata if cache entry exists.
                                          Defaults to False.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Check if entry already exists
            if not force_update and self.exists(key, partition_key, check_query=True):
                # Entry exists, only update query metadata
                return self.set_query(key, query_text, partition_key)
            else:
                # Entry doesn't exist or force_update=True, set both
                success_data = self.set_cache_lazy(key, query, partition_key)
                success_query = self.set_query(key, query_text, partition_key)
                return success_data and success_query
        except Exception as e:
            # Log error but don't raise exception to maintain API consistency
            logger.error(f"Failed to set cache entry lazily for key {key}: {e}")
            return False

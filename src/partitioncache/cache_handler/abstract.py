from abc import ABC, abstractmethod
from datetime import datetime


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
    def exists(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key (str): The key to check.
            partition_key (str, optional): The partition key. Defaults to "partition_key".

        Returns:
            bool: True if the key exists, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """
        Store a set in the cache associated with the given key. The type is inferred from the metadata table, or if not present, from the type of value.

        Args:
            key (str): The key to associate with the set.
            value (set[int] | set[str] | set[float] | set[datetime]): The set to store.
            partition_key (str, optional): The partition key namespace. Defaults to "partition_key".

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
    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key") -> set:
        """
        Filter and return the set of keys that exist in the cache of the given set.

        Args:
            keys (set): A set of keys to verify.
            partition_key (str, optional): The partition key. Defaults to "partition_key".

        Returns:
            set: The subset set of keys that exist in the cache.
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

    def compact(self):
        """
        Compact the cache to optimize storage as recommended for some cache handlers.
        """
        pass

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


class AbstractCacheHandler_Lazy(AbstractCacheHandler):
    """
    Abstract class for cache handlers that support lazy intersection.
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

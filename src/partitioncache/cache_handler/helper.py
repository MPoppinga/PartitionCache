"""
Partition-specific cache handler wrapper.

Usage:
    handler = get_cache_handler("postgresql_array")
    partition_handler = PartitionCacheHandler(handler, "user_id", int)

    # Use without specifying partition_key repeatedly
    partition_handler.set_set("key1", {1, 2, 3})
    result = partition_handler.get("key1")
"""

from datetime import datetime
from logging import getLogger

from .abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy

logger = getLogger("PartitionCache")

DataSet = set[int] | set[str] | set[float] | set[datetime]


class PartitionCacheHelper:
    """
    Helper wrapper around a cache handler for a specific partition key and datatype.

    This provides a cleaner interface by encapsulating the partition_key and datatype,
    reducing repetitive parameter passing and enabling early validation.
    """

    def __init__(self, cache_handler: AbstractCacheHandler, partition_key: str, settype: str | None):
        """
        Initialize the partition cache handler wrapper.

        Args:
            cache_handler: The underlying cache handler instance
            partition_key: The partition key for this wrapper
            settype: The datatype string for values (e.g., 'int', 'str', 'float', 'datetime')
            cache_handler_type: Optional cache handler type for early validation

        Raises:
            ValueError: If the cache handler doesn't support the specified datatype
        """
        self._cache_handler = cache_handler
        self._partition_key = partition_key
        self._settype = None

        # Check if partition key is already known and get the datatype
        # If datatype is specified, compare it with the known datatype
        known_datatype = self._cache_handler.get_datatype(partition_key)
        if settype is None:
            self._settype = known_datatype
        else:
            cache_handler.__class__.validate_datatype_compatibility(settype)

            if known_datatype is not None:
                if known_datatype != settype:
                    raise ValueError(f"Datatype mismatch: {known_datatype} != {settype}")

            else:
                # initialize the cache handler with the settype
                self._cache_handler.register_partition_key(partition_key, settype)
                self._settype = settype

        if self._settype is None:
            logger.info(f"Postponing datatype validation for partition key '{partition_key}'")

        logger.info(f"Created partition cache handler for '{partition_key}' with datatype '{self._settype}'")

    @property
    def partition_key(self) -> str:
        """Get the partition key for this handler."""
        return self._partition_key

    @property
    def datatype(self) -> str | None:
        """Get the datatype for this handler."""
        return self._settype

    @property
    def underlying_handler(self) -> AbstractCacheHandler:
        """Get the underlying cache handler."""
        return self._cache_handler

    def set_set(self, key: str, value: DataSet) -> bool:
        """
        Store a set in the cache.

        Args:
            key: The cache key
            value: The set of values to store

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self._cache_handler.set_set(key=key, value=value, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to set_set for key {key}: {e}")
            return False

    def get(self, key: str) -> DataSet | None:
        """
        Retrieve a set from the cache.

        Args:
            key: The cache key

        Returns:
            Set of values or None if not found
        """
        try:
            return self._cache_handler.get(
                key=key,
                settype=self._settype,  # type: ignore
                partition_key=self._partition_key,
            )
        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            return None

    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: The cache key

        Returns:
            bool: True if exists, False otherwise
        """
        try:
            return self._cache_handler.exists(key=key, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to check existence of key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete a key from the cache.

        Args:
            key: The cache key

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self._cache_handler.delete(key=key, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False

    def set_null(self, key: str) -> bool:
        """
        Store a null marker in the cache.

        Args:
            key: The cache key

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self._cache_handler.set_null(key=key, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to set_null for key {key}: {e}")
            return False

    def is_null(self, key: str) -> bool:
        """
        Check if a key has a null marker.

        Args:
            key: The cache key

        Returns:
            bool: True if null, False otherwise
        """
        try:
            return self._cache_handler.is_null(key=key, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to check is_null for key {key}: {e}")
            return False

    def set_query(self, key: str, querytext: str) -> bool:
        """
        Store a query associated with a key.

        Args:
            key: The cache key
            querytext: The SQL query text

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self._cache_handler.set_query(key=key, querytext=querytext, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to set_query for key {key}: {e}")
            return False

    def get_intersected(self, keys: set[str]) -> tuple[DataSet | None, int]:
        """
        Get intersection of multiple cache entries.

        Args:
            keys: Set of cache keys to intersect

        Returns:
            Tuple of (intersected set, count of matched keys)
        """
        try:
            return self._cache_handler.get_intersected(keys=keys, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to get_intersected for keys {keys}: {e}")
            return None, 0

    def filter_existing_keys(self, keys: set[str]) -> set[str]:
        """
        Filter keys to only those that exist in cache.

        Args:
            keys: Set of keys to filter

        Returns:
            Set of existing keys
        """
        try:
            return self._cache_handler.filter_existing_keys(keys=keys, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to filter_existing_keys: {e}")
            return set()

    def get_all_keys(self) -> list:
        """
        Get all keys in this partition.

        Returns:
            List of all keys
        """
        try:
            return self._cache_handler.get_all_keys(partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to get_all_keys: {e}")
            return []

    def delete_partition(self) -> bool:
        """
        Delete the entire partition and all its data.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if hasattr(self._cache_handler, "delete_partition"):
                return self._cache_handler.delete_partition(self._partition_key)  # type: ignore
            else:
                # Fallback: delete all keys individually
                all_keys = self.get_all_keys()
                success = True
                for key in all_keys:
                    if not self.delete(key):
                        success = False
                return success
        except Exception as e:
            logger.error(f"Failed to delete partition {self._partition_key}: {e}")
            return False

    def close(self) -> None:
        """Close the underlying cache handler."""
        try:
            self._cache_handler.close()
        except Exception as e:
            logger.error(f"Failed to close cache handler: {e}")
        del self._cache_handler

    def __repr__(self) -> str:
        return f"PartitionCacheHandler(partition_key='{self._partition_key}', datatype='{self._settype}', handler_type='{self._cache_handler.__repr__()}')"

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - only close if not a singleton."""
        # Don't close singleton instances as they may be reused
        if getattr(self._cache_handler.__class__, '_instance', None) is None:
            self.close()
        return False


class LazyPartitionCacheHelper(PartitionCacheHelper):
    """
    Wrapper for cache handlers that support lazy intersection.
    """

    def __init__(self, cache_handler: AbstractCacheHandler_Lazy, partition_key: str, settype: str | None):
        """
        Initialize the lazy partition cache handler wrapper.

        Args:
            cache_handler: The underlying lazy cache handler instance
            partition_key: The partition key for this wrapper
            settype: The Python type for values (int, str, float, datetime)
            cache_handler_type: Optional cache handler type for early validation
        """
        super().__init__(cache_handler, partition_key, settype)

        self._cache_handler: AbstractCacheHandler_Lazy = cache_handler

    def get_intersected_lazy(self, keys: set[str]) -> tuple[str | None, int]:
        """
        Get lazy intersection representation.

        Args:
            keys: Set of cache keys to intersect

        Returns:
            Tuple of (SQL representation, count of matched keys)
        """
        try:
            return self._cache_handler.get_intersected_lazy(keys=keys, partition_key=self._partition_key)
        except Exception as e:
            logger.error(f"Failed to get_intersected_lazy for keys {keys}: {e}")
            return None, 0


def create_partitioncache_helper(cache_handler: AbstractCacheHandler, partition_key: str, settype: str | None) -> PartitionCacheHelper:
    """
    Factory function to create the appropriate partition handler.

    Args:
        cache_handler: The underlying cache handler instance
        partition_key: The partition key for this wrapper
        settype: The Python type for values (int, str, float, datetime)
        cache_handler_type: Optional cache handler type for early validation

    Returns:
        PartitionCacheHandler or LazyPartitionCacheHandler instance
    """
    if isinstance(cache_handler, AbstractCacheHandler_Lazy):
        return LazyPartitionCacheHelper(cache_handler, partition_key, settype)
    else:
        return PartitionCacheHelper(cache_handler, partition_key, settype)

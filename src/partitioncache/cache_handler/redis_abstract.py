from functools import cache
import redis
from datetime import datetime

from partitioncache.cache_handler.abstract import AbstractCacheHandler


class RedisAbstractCacheHandler(AbstractCacheHandler):
    """
    Handles access to a Redis cache.
    This handler supports multiple partition keys but only integer and string datatypes.
    """

    _instance = None
    _refcount = 0

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        cls._refcount += 1
        return cls._instance

    def __init__(self, db_name, db_host, db_password, db_port) -> None:
        """
        Initialize the cache handler with the given db name.
        This handler supports multiple partition keys but only integer and string datatypes.
        """
        self.db = redis.Redis(host=db_host, port=db_port, db=db_name, password=db_password)

    @cache
    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        # Check if it's the old format (just a string) or new format (hash)
        key_type = self.db.type(metadata_key)
        if key_type == b"string":
            # Old format - just return the datatype
            datatype = self.db.get(metadata_key)
            if datatype is not None and isinstance(datatype, bytes):
                return datatype.decode()
        elif key_type == b"hash":
            # New format - get datatype from hash
            datatype = self.db.hget(metadata_key, "datatype")
            if datatype is not None and isinstance(datatype, bytes):
                return datatype.decode()
        return None

    def _set_partition_metadata(self, partition_key: str, datatype: str, bitsize: int | None = None) -> None:
        """Set the metadata for a partition key."""
        metadata_key = f"_partition_metadata:{partition_key}"

        # Check if partition key already exists with different datatype
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is not None and existing_datatype != datatype:
            raise ValueError(f"Partition key '{partition_key}' already exists with datatype '{existing_datatype}', cannot use datatype '{datatype}'")

        metadata = {"datatype": datatype}
        if bitsize is not None:
            metadata["bitsize"] = str(bitsize)

        self.db.hset(metadata_key, mapping=metadata)

    def _get_cache_key(self, key: str, partition_key: str) -> str:
        """Get the Redis key for a cache entry with partition key namespace."""
        return f"cache:{partition_key}:{key}"

    def exists(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Returns True if the key exists in the partition-specific cache, otherwise False.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        return self.db.exists(cache_key) != 0

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Set null value in partition-specific cache."""
        try:
            # Ensure partition exists with default datatype
            existing_datatype = self._get_partition_datatype(partition_key)
            if existing_datatype is None:
                self._set_partition_metadata(partition_key, "integer")

            cache_key = self._get_cache_key(key, partition_key)
            self.db.set(cache_key, "\x00")
            return True
        except Exception:
            return False

    def is_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Check if key has null value in partition-specific cache."""
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        return self.db.get(cache_key) == b"\x00"

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """Delete from partition-specific cache."""
        try:
            cache_key = self._get_cache_key(key, partition_key)
            self.db.delete(cache_key)

            # Also delete associated query
            query_key = f"query:{partition_key}:{key}"
            self.db.delete(query_key)
            return True
        except Exception:
            return False

    def _set_partition_datatype(self, partition_key: str, datatype: str) -> None:
        """Set the datatype for a partition key in metadata."""
        self._set_partition_metadata(partition_key, datatype)

    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """Store a query in the cache associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data = {"query": querytext, "partition_key": partition_key, "last_seen": str(datetime.now())}
            self.db.hset(query_key, mapping=query_data)
            return True
        except Exception:
            return False

    def close(self) -> None:
        self._refcount -= 1
        if self._refcount <= 0:
            self.db.close()
            self._instance = None
            self._refcount = 0

    def get_all_keys(self, partition_key: str) -> list:
        """Get all keys for a specific partition key."""
        # Get keys for specific partition
        pattern = f"cache:{partition_key}:*"
        try:
            cache_keys_response = self.db.keys(pattern)
            cache_keys = list(cache_keys_response) if cache_keys_response else []  # type: ignore
            # Extract original keys by removing the prefix
            prefix = f"cache:{partition_key}:"
            result = []
            for key in cache_keys:
                if isinstance(key, bytes):
                    result.append(key.decode()[len(prefix) :])
            return result
        except (TypeError, AttributeError):
            # Fallback for Redis typing issues - return empty list
            return []

    def get_partition_keys(self) -> list[tuple[str, str]]:
        """Get all partition keys and their datatypes."""
        try:
            metadata_keys = list(self.db.keys("_partition_metadata:*"))  # type: ignore
            result = []
            for metadata_key in metadata_keys:
                if isinstance(metadata_key, bytes):
                    partition_key = metadata_key.decode().split(":", 1)[1]
                    datatype_bytes = self.db.get(metadata_key)
                    if datatype_bytes is not None and isinstance(datatype_bytes, bytes):
                        datatype = datatype_bytes.decode()
                        result.append((partition_key, datatype))
            return sorted(result)
        except (TypeError, AttributeError):
            # Fallback for Redis typing issues
            return []

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        self._set_partition_metadata(partition_key, datatype, kwargs.get("bitsize"))

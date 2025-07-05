import logging
from datetime import datetime

import redis

from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = logging.getLogger("PartitionCache")


class RedisAbstractCacheHandler(AbstractCacheHandler):
    """
    Handles access to a Redis cache.
    This handler supports multiple partition keys but only integer and string datatypes.
    """

    _instance = None
    _refcount = 0
    _cached_datatype: dict[str, str] = {}

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

    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"

        if partition_key in self._cached_datatype:
            return self._cached_datatype[partition_key]

        key_type = self.db.type(metadata_key)
        if key_type == b"string":
            datatype = self.db.get(metadata_key)
            if datatype is not None and isinstance(datatype, bytes):
                self._cached_datatype[partition_key] = datatype.decode()
                return datatype.decode()
        elif key_type == b"hash":
            datatype = self.db.hget(metadata_key, "datatype")
            if datatype is not None and isinstance(datatype, bytes):
                self._cached_datatype[partition_key] = datatype.decode()
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

    def exists(self, key: str, partition_key: str = "partition_key", check_invalid_too: bool = False) -> bool:
        """
        Returns True if the key exists in the partition-specific cache, otherwise False.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        cache_exists = self.db.exists(cache_key) != 0


        if not check_invalid_too:
            return cache_exists

        # For check_invalid_too=False, check that termination bits don't exist (only valid entries)
        limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
        timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

        # If either termination bit exists, the result is invalid
        has_limit_bit = self.db.exists(limit_key) != 0
        has_timeout_bit = self.db.exists(timeout_key) != 0

        return cache_exists or has_limit_bit or has_timeout_bit

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

            # Delete termination bits
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)
            self.db.delete(limit_key, timeout_key)

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
            query_data = {"query": querytext, "partition_key": partition_key, "last_seen": str(datetime.now()), "status": "ok"}
            self.db.hset(query_key, mapping=query_data)
            return True
        except Exception:
            return False

    def get_query(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Retrieve the query text associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data: dict[bytes, bytes] = self.db.hgetall(query_key)  # type: ignore
            if query_data:
                query_bytes = query_data.get(b"query")
                if query_bytes and isinstance(query_bytes, bytes):
                    return query_bytes.decode()
            return None
        except Exception:
            return None

    def get_all_queries(self, partition_key: str) -> list[tuple[str, str]]:
        """Retrieve all query hash and text pairs for a specific partition."""
        try:
            pattern = f"query:{partition_key}:*"
            query_keys = self.db.keys(pattern)  # type: ignore

            queries = []
            for query_key in query_keys:  # type: ignore
                if isinstance(query_key, bytes):
                    query_key_str = query_key.decode()
                    query_data: dict[bytes, bytes] = self.db.hgetall(query_key_str)  # type: ignore
                    if query_data and b"query" in query_data:
                        # Extract hash from key: query:partition:hash -> hash
                        query_hash = query_key_str.split(":", 2)[-1]
                        query_value = query_data[b"query"]
                        if isinstance(query_value, bytes):
                            query_text = query_value.decode()
                            queries.append((query_hash, query_text))

            return queries
        except Exception:
            return []

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

                    key_type = self.db.type(metadata_key)
                    if key_type == b"string":
                        datatype_bytes = self.db.get(metadata_key)
                        if datatype_bytes is not None and isinstance(datatype_bytes, bytes):
                            datatype = datatype_bytes.decode()
                            result.append((partition_key, datatype))
                    elif key_type == b"hash":
                        metadata_key_str = metadata_key.decode() if isinstance(metadata_key, bytes) else metadata_key
                        datatype_bytes = self.db.hget(metadata_key_str, "datatype")
                        if datatype_bytes is not None and isinstance(datatype_bytes, bytes):
                            datatype = datatype_bytes.decode()
                            result.append((partition_key, datatype))
            return sorted(result)
        except (TypeError, AttributeError):
            # Fallback for Redis typing issues
            return []

    def clear_all_cache_data(self) -> int:
        """Clear all cache-related data from this Redis database.

        Returns:
            Number of keys deleted
        """
        deleted_count = 0
        try:
            # Find all cache-related keys using patterns
            patterns = [
                "cache:*",  # Cache data keys
                "query:*",  # Query tracking keys
                "_partition_metadata:*",  # Partition metadata
            ]

            for pattern in patterns:
                # Use SCAN to find keys matching pattern (safer than KEYS for large datasets)
                cursor = 0
                while True:
                    cursor, keys = self.db.scan(cursor, match=pattern, count=100)  # type: ignore
                    if keys:
                        deleted_count += self.db.delete(*keys)  # type: ignore
                    if cursor == 0:
                        break

            return deleted_count
        except Exception as e:
            logger.error(f"Failed to clear cache data: {e}")
            return deleted_count

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def set_query_status(self, key: str, partition_key: str = "partition_key", status: str = "ok") -> bool:
        """Set the status of a query using termination bits for Redis backend."""
        try:
            # Valid statuses
            valid_statuses = {'ok', 'timeout', 'failed'}
            if status not in valid_statuses:
                raise ValueError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

            # Get termination bit keys
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

            if status == "ok":
                # Remove any existing termination bits
                self.db.delete(limit_key, timeout_key)
                # Update query metadata if it exists
                query_key = f"query:{partition_key}:{key}"
                if self.db.exists(query_key):
                    self.db.hset(query_key, "status", "ok")
                    self.db.hset(query_key, "last_seen", str(datetime.now()))
            elif status == "failed":
                # Set limit termination bit
                self.db.set(limit_key, "\x00")
                # Remove timeout bit if it exists
                self.db.delete(timeout_key)
                # Update query metadata if it exists
                query_key = f"query:{partition_key}:{key}"
                if self.db.exists(query_key):
                    self.db.hset(query_key, "status", "failed")
                    self.db.hset(query_key, "last_seen", str(datetime.now()))
            elif status == "timeout":
                # Set timeout termination bit
                self.db.set(timeout_key, "\x00")
                # Remove limit bit if it exists
                self.db.delete(limit_key)
                # Update query metadata if it exists
                query_key = f"query:{partition_key}:{key}"
                if self.db.exists(query_key):
                    self.db.hset(query_key, "status", "timeout")
                    self.db.hset(query_key, "last_seen", str(datetime.now()))

            return True
        except Exception as e:
            logger.error(f"Failed to set query status for key {key}: {e}")
            return False

    def get_query_status(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Get the status of a query from termination bits for Redis backend."""
        try:
            # Check termination bits first
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

            if self.db.exists(limit_key):
                return "failed"
            elif self.db.exists(timeout_key):
                return "timeout"

            # Check query metadata for status
            query_key = f"query:{partition_key}:{key}"
            if self.db.exists(query_key):
                status_bytes = self.db.hget(query_key, "status")
                if status_bytes and isinstance(status_bytes, bytes):
                    return status_bytes.decode()
                # If no status field, assume ok if query exists
                return "ok"

            # Check if cache entry exists
            cache_key = self._get_cache_key(key, partition_key)
            if self.db.exists(cache_key):
                return "ok"

            return None
        except Exception:
            return None

    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key", check_invalid_too: bool = False) -> set:
        """
        Filter and return the set of keys that exist in the cache.
        For Redis, this checks both cache existence and termination bits when check_invalid_too=False.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return set()

        if not check_invalid_too:
            # Only valid entries - just check cache existence
            pipe = self.db.pipeline()
            cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
            for cache_key in cache_keys:
                pipe.exists(cache_key)
            existence_results = pipe.execute()

            existing_keys = set()
            for key, exists_result in zip(keys, existence_results, strict=False):
                if exists_result:
                    existing_keys.add(key)
            return existing_keys
        else:
            # Include both valid and invalid entries - check cache existence AND validate status
            pipe = self.db.pipeline()
            cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
            limit_keys = [self._get_cache_key(f"_LIMIT_{key}", partition_key) for key in keys]
            timeout_keys = [self._get_cache_key(f"_TIMEOUT_{key}", partition_key) for key in keys]

            # Check existence of cache keys
            for cache_key in cache_keys:
                pipe.exists(cache_key)
            # Check existence of termination bits
            for limit_key in limit_keys:
                pipe.exists(limit_key)
            for timeout_key in timeout_keys:
                pipe.exists(timeout_key)

            results = pipe.execute()

            # Split results
            num_keys = len(keys)
            cache_existence = results[:num_keys]
            limit_existence = results[num_keys:2*num_keys]
            timeout_existence = results[2*num_keys:3*num_keys]

            existing_keys = set()
            for key, cache_exists, has_limit, has_timeout in zip(keys, cache_existence, limit_existence, timeout_existence, strict=False):
                if cache_exists or has_limit or has_timeout:
                    existing_keys.add(key)

            return existing_keys

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        self._set_partition_metadata(partition_key, datatype, kwargs.get("bitsize"))

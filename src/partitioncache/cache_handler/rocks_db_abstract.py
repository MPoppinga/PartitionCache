import threading
from datetime import datetime

from rocksdb import (  # type: ignore
    DB,  # type: ignore
    Options,  # type: ignore
)

from partitioncache.cache_handler.abstract import AbstractCacheHandler


class RocksDBAbstractCacheHandler(AbstractCacheHandler):
    """
    Handles access to a RocksDB cache using bitarrays for efficient storage.
    This handler supports multiple partition keys but only integer datatypes.
    """

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, db_path: str, read_only: bool = False, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-checked locking
                    cls._instance = cls(db_path, read_only=read_only, **kwargs)
        return cls._instance

    def __init__(self, db_path: str, read_only: bool = False, **kwargs) -> None:
        self.db = DB(db_path, Options(create_if_missing=True), read_only=read_only)

        # rocksdb options
        # opts = rocksdb.Options()
        # opts.create_if_missing = True
        # opts.max_open_files = 200
        # opts.write_buffer_size = 150 * 1024 * 1024
        # opts.target_file_size_base = 300 * 1024 * 1024
        # opts.level0_file_num_compaction_trigger = 10
        # opts.level0_slowdown_writes_trigger = 20
        # opts.level0_stop_writes_trigger = 36
        # opts.max_background_compactions = 3
        # opts.max_background_flushes = 4
        # opts.max_write_buffer_number = 6
        # opts.table_cache_numshardbits = 6
        #
        # opts.merge_operator = UniqueIntListMergeOperator()
        #
        # cache = rocksdb.LRUCache(capacity=150 * (1024**2))
        # compressed_cache = rocksdb.LRUCache(capacity=80 * (1024**2))
        #
        # opts.table_factory = rocksdb.BlockBasedTableFactory(
        #    filter_policy=rocksdb.BloomFilterPolicy(10),
        #    block_cache=cache,
        #    block_cache_compressed=compressed_cache,
        # )

    def exists(self, key: str, partition_key: str = "partition_key", check_invalid_too: bool = False) -> bool:
        """
        Returns True if the key exists in the partition-specific cache, otherwise False.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        cache_exists = self.db.get(cache_key.encode()) is not None

        if not check_invalid_too:
            return cache_exists

        # For check_invalid_too=True, check if termination bits exist
        limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
        timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

        # If either termination bit exists, the result is invalid
        has_limit_bit = self.db.get(limit_key.encode()) is not None
        has_timeout_bit = self.db.get(timeout_key.encode()) is not None

        return cache_exists or has_limit_bit or has_timeout_bit

    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key", check_invalid_too: bool = False) -> set:
        """
        Checks in RocksDB which of the keys exists in cache and returns the set of existing keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return set()

        existing_keys = set()
        for key in keys:
            cache_key = self._get_cache_key(key, partition_key)
            cache_exists = self.db.get(cache_key.encode()) is not None

            if not check_invalid_too:
                if cache_exists:
                    existing_keys.add(key)
                continue

            # For check_invalid_too=True, check also if termination bits exist
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

            has_limit_bit = self.db.get(limit_key.encode()) is not None
            has_timeout_bit = self.db.get(timeout_key.encode()) is not None

            if cache_exists or has_limit_bit or has_timeout_bit:
                existing_keys.add(key)

        return existing_keys

    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        datatype_bytes = self.db.get(metadata_key.encode())
        return datatype_bytes.decode() if datatype_bytes is not None else None

    def _set_partition_datatype(self, partition_key: str, datatype: str) -> None:
        """Set the datatype for a partition key in metadata."""
        self._validate_datatype(datatype)
        metadata_key = f"_partition_metadata:{partition_key}"
        self.db.put(metadata_key.encode(), datatype.encode(), sync=True)

    def _validate_datatype(self, datatype: str) -> None:
        """Validate datatype for this handler. Override in concrete classes."""
        supported_datatypes = self.get_supported_datatypes()
        if datatype not in supported_datatypes:
            raise ValueError(f"Datatype '{datatype}' not supported by {self.__class__.__name__}. Supported: {supported_datatypes}")

    def _get_cache_key(self, key: str, partition_key: str) -> str:
        """Get the RocksDB key for a cache entry with partition key namespace."""
        return f"cache:{partition_key}:{key}"

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Set null value in partition-specific cache."""
        try:
            # Ensure partition exists with default datatype
            existing_datatype = self._get_partition_datatype(partition_key)
            if existing_datatype is None:
                self._set_partition_datatype(partition_key, "integer")

            cache_key = self._get_cache_key(key, partition_key)
            self.db.put(cache_key.encode(), b"\x00", sync=True)
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
        ret: bool = self.db.get(cache_key.encode()) == b"\x00"
        return ret

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """Delete from partition-specific cache."""
        try:
            cache_key = self._get_cache_key(key, partition_key)
            key_b = cache_key.encode("utf-8")
            self.db.delete(key_b)

            # Also delete associated query
            query_key = f"query:{partition_key}:{key}"
            self.db.delete(query_key.encode("utf-8"))

            # Delete termination bits
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)
            self.db.delete(limit_key.encode("utf-8"))
            self.db.delete(timeout_key.encode("utf-8"))

            return True
        except Exception:
            return False

    def get_partition_keys(self) -> list[tuple[str, str]]:
        """Get all partition keys and their datatypes."""
        result = []
        it = self.db.iterkeys()
        it.seek_to_first()

        for key in it:
            key_str = key.decode("utf-8")
            if key_str.startswith("_partition_metadata:"):
                partition_key = key_str.split(":", 1)[1]
                datatype_bytes = self.db.get(key)
                if datatype_bytes is not None:
                    datatype = datatype_bytes.decode()
                    result.append((partition_key, datatype))
        return sorted(result)

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype not in self.get_supported_datatypes():
            raise ValueError(f"Unsupported datatype: {datatype}")
        self._set_partition_datatype(partition_key, datatype)

    def close(self) -> None:
        """
        RocksDB does not have a close method, so we can skip this.
        """
        pass

    def compact(self) -> None:
        self.db.compact_range()

    def get_all_keys(self, partition_key: str) -> list:
        """
        Get all keys from the RocksDB cache for a specific partition key.

        Returns:
            list: List of keys
        """
        keys = []
        it = self.db.iterkeys()
        it.seek_to_first()

        # Filter keys for specific partition
        prefix = f"cache:{partition_key}:"
        prefix_len = len(prefix)
        for key in it:
            key_str = key.decode("utf-8")
            if key_str.startswith(prefix) and not key_str.startswith("_partition_metadata:"):
                keys.append(key_str[prefix_len:])
        return keys

    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """Store a query in the cache associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data = f"{querytext}|{partition_key}|{datetime.now().isoformat()}|ok"
            self.db.put(query_key.encode("utf-8"), query_data.encode("utf-8"), sync=True)
            return True
        except Exception:
            return False

    def get_query(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Retrieve the query text associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data = self.db.get(query_key.encode("utf-8"))
            if query_data:
                # Parse pipe-separated format: querytext|partition_key|timestamp
                parts = query_data.decode("utf-8").split("|", 2)
                return parts[0] if len(parts) >= 1 else None
            return None
        except Exception:
            return None

    def set_query_status(self, key: str, partition_key: str = "partition_key", status: str = "ok") -> bool:
        """Set the status of a query using termination bits for RocksDB backend."""
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
                self.db.delete(limit_key.encode("utf-8"))
                self.db.delete(timeout_key.encode("utf-8"))
                # Update query metadata if it exists
                query_key = f"query:{partition_key}:{key}"
                query_data = self.db.get(query_key.encode("utf-8"))
                if query_data:
                    # Update status in pipe-separated format: querytext|partition_key|timestamp|status
                    parts = query_data.decode("utf-8").split("|", 3)
                    if len(parts) >= 3:
                        parts = parts[:3] + ["ok"]  # Replace or add status
                        updated_data = "|".join(parts)
                        self.db.put(query_key.encode("utf-8"), updated_data.encode("utf-8"), sync=True)
            elif status == "failed":
                # Set limit termination bit
                self.db.put(limit_key.encode("utf-8"), b"\x00", sync=True)
                # Remove timeout bit if it exists
                self.db.delete(timeout_key.encode("utf-8"))
                # Update query metadata if it exists
                query_key = f"query:{partition_key}:{key}"
                query_data = self.db.get(query_key.encode("utf-8"))
                if query_data:
                    parts = query_data.decode("utf-8").split("|", 3)
                    if len(parts) >= 3:
                        parts = parts[:3] + ["failed"]
                        updated_data = "|".join(parts)
                        self.db.put(query_key.encode("utf-8"), updated_data.encode("utf-8"), sync=True)
            elif status == "timeout":
                # Set timeout termination bit
                self.db.put(timeout_key.encode("utf-8"), b"\x00", sync=True)
                # Remove limit bit if it exists
                self.db.delete(limit_key.encode("utf-8"))
                # Update query metadata if it exists
                query_key = f"query:{partition_key}:{key}"
                query_data = self.db.get(query_key.encode("utf-8"))
                if query_data:
                    parts = query_data.decode("utf-8").split("|", 3)
                    if len(parts) >= 3:
                        parts = parts[:3] + ["timeout"]
                        updated_data = "|".join(parts)
                        self.db.put(query_key.encode("utf-8"), updated_data.encode("utf-8"), sync=True)

            return True
        except Exception:
            # Could add logging here if logger is available
            return False

    def get_query_status(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Get the status of a query from termination bits for RocksDB backend."""
        try:
            # Check termination bits first
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

            if self.db.get(limit_key.encode("utf-8")) is not None:
                return "failed"
            elif self.db.get(timeout_key.encode("utf-8")) is not None:
                return "timeout"

            # Check query metadata for status
            query_key = f"query:{partition_key}:{key}"
            query_data = self.db.get(query_key.encode("utf-8"))
            if query_data:
                # Parse pipe-separated format: querytext|partition_key|timestamp|status
                parts = query_data.decode("utf-8").split("|", 3)
                if len(parts) >= 4:
                    return parts[3]
                # If no status field, assume ok if query exists
                return "ok"

            # Check if cache entry exists
            cache_key = self._get_cache_key(key, partition_key)
            if self.db.get(cache_key.encode()) is not None:
                return "ok"

            return None
        except Exception:
            return None

    def get_all_queries(self, partition_key: str) -> list[tuple[str, str]]:
        """Retrieve all query hash and text pairs for a specific partition."""
        try:
            prefix = f"query:{partition_key}:".encode()
            queries = []

            # Iterate through all keys with the partition prefix
            it = self.db.iterkeys()
            it.seek(prefix)

            for key in it:
                if not key.startswith(prefix):
                    break

                query_data = self.db.get(key)
                if query_data:
                    # Parse pipe-separated format: querytext|partition_key|timestamp|status
                    parts = query_data.decode("utf-8").split("|", 3)
                    if len(parts) >= 1:
                        # Extract hash from key: query:partition:hash -> hash
                        query_hash = key.decode("utf-8").split(":", 2)[-1]
                        queries.append((query_hash, parts[0]))

            return queries
        except Exception:
            return []

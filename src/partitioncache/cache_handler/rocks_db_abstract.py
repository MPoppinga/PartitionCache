from datetime import datetime
from bitarray import bitarray
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

    @classmethod
    def get_instance(cls, db_path: str, read_only: bool = False, **kwargs):
        if cls._instance is None:
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

    def exists(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Returns True if the key exists in the partition-specific cache, otherwise False.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        return self.db.get(cache_key.encode()) is not None

    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key") -> set:
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
            if self.db.get(cache_key.encode()) is not None:
                existing_keys.add(key)
        return existing_keys

    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        datatype_bytes = self.db.get(metadata_key.encode())
        return datatype_bytes.decode() if datatype_bytes is not None else None

    def _set_partition_datatype(self, partition_key: str, datatype: str) -> None:
        """Set the datatype for a partition key in metadata."""
        if datatype != "integer":
            raise ValueError("RocksDB bit handler only supports integer datatype")
        metadata_key = f"_partition_metadata:{partition_key}"
        self.db.put(metadata_key.encode(), datatype.encode(), sync=True)

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
        return self.db.get(cache_key.encode()) == b"\x00"

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """Delete from partition-specific cache."""
        try:
            cache_key = self._get_cache_key(key, partition_key)
            key_b = cache_key.encode("utf-8")
            self.db.delete(key_b)

            # Also delete associated query
            query_key = f"query:{partition_key}:{key}"
            self.db.delete(query_key.encode("utf-8"))
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
            query_data = f"{querytext}|{partition_key}|{datetime.now().isoformat()}"
            self.db.put(query_key.encode("utf-8"), query_data.encode("utf-8"), sync=True)
            return True
        except Exception:
            return False

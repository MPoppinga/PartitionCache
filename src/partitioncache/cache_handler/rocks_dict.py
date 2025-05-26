from logging import getLogger
from datetime import datetime

from rocksdict import AccessType, Options, Rdict

from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = getLogger("PartitionCache")


class RocksDictCacheHandler(AbstractCacheHandler):
    """
    Handles access to a RocksDB cache using RocksDict with native serialization.
    This handler supports multiple partition keys with datatypes: integer, float, text, timestamp.
    """

    _instance = None

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """RocksDict supports all datatypes with native serialization."""
        return {"integer", "float", "text", "timestamp"}

    def __repr__(self) -> str:
        return "rocksdict"

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        opts = Options(raw_mode=False)
        # Optimize for read performance
        opts.create_if_missing(True)
        opts.set_max_background_jobs(4)
        opts.optimize_for_point_lookup(64 * 1024 * 1024)  # 64MB block cache
        opts.set_max_open_files(-1)  # Keep all files open
        opts.set_allow_mmap_reads(True)  # Enable memory-mapped reads
        opts.increase_parallelism(4)  # Increase parallel operations
        self.db = Rdict(db_path, options=opts, access_type=AccessType.read_only() if read_only else AccessType.read_write())

        # Initialize metadata tracking for partition keys
        self._ensure_metadata_structure()
    
    def _ensure_metadata_structure(self) -> None:
        """Ensure metadata structure exists for tracking partition keys."""
        # RocksDict doesn't need explicit structure creation
        pass
    
    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        datatype = self.db.get(metadata_key)
        return datatype if datatype is not None and datatype != "NULL" else None
    
    def _set_partition_datatype(self, partition_key: str, datatype: str) -> None:
        """Set the datatype for a partition key in metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        self.db[metadata_key] = datatype
    
    def _get_cache_key(self, key: str, partition_key: str) -> str:
        """Get the RocksDict key for a cache entry with partition key namespace."""
        return f"cache:{partition_key}:{key}"

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache namespace."""
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None
        
        cache_key = self._get_cache_key(key, partition_key)
        result = self.db.get(cache_key)
        if result is None or result == "NULL":
            return None

        return result

    def exists(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Returns True if the key exists in the partition-specific cache, otherwise False.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        return cache_key in self.db

    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key") -> set:
        """
        Checks in RocksDict which of the keys exists in cache and returns the set of existing keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return set()
        
        existing_keys = set()
        for key in keys:
            cache_key = self._get_cache_key(key, partition_key)
            if cache_key in self.db:
                existing_keys.add(key)
        return existing_keys

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0
        
        count_match = 0
        result: set | None = None
        for key in keys:
            t = self.get(key, partition_key=partition_key)
            if t is None:
                continue
            if result is None:
                result = t
                count_match += 1
            else:
                result = result.intersection(t)
                count_match += 1
        return result, count_match

    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """
        Store a set in the database for a specific partition key. RocksDict will handle serialization.
        """
        # Try to get datatype from metadata
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is not None:
            if existing_datatype != "integer" and existing_datatype != "float" and existing_datatype != "text" and existing_datatype != "timestamp":
                raise ValueError(f"Unsupported datatype in metadata: {existing_datatype}")
            datatype = existing_datatype
        else:
            # Infer from value type
            sample = next(iter(value))
            if isinstance(sample, int):
                datatype = "integer"
            elif isinstance(sample, float):
                datatype = "float"
            elif isinstance(sample, str):
                datatype = "text"
            elif isinstance(sample, datetime):
                datatype = "timestamp"
            else:
                raise ValueError(f"Unsupported value type: {type(sample)}")
            self._set_partition_datatype(partition_key, datatype)
        val = value
        
        try:
            cache_key = self._get_cache_key(key, partition_key)
            logger.info(f"saving {len(val)} values in cache {cache_key}")
            self.db[cache_key] = val
            return True
        except Exception:
            return False

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Store a null value in the database for a specific partition key.
        """
        try:
            # Ensure partition exists with default datatype
            existing_datatype = self._get_partition_datatype(partition_key)
            if existing_datatype is None:
                self._set_partition_datatype(partition_key, "integer")
            
            cache_key = self._get_cache_key(key, partition_key)
            self.db[cache_key] = "NULL"
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
        return self.db.get(cache_key) == "NULL"

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Delete a key from the database for a specific partition key.
        """
        try:
            cache_key = self._get_cache_key(key, partition_key)
            if cache_key in self.db:
                del self.db[cache_key]
            
            # Also delete associated query
            query_key = f"query:{partition_key}:{key}"
            if query_key in self.db:
                del self.db[query_key]
            return True
        except Exception:
            return False

    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """Store a query in the cache associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data = {
                "query": querytext,
                "partition_key": partition_key,
                "last_seen": datetime.now().isoformat()
            }
            self.db[query_key] = query_data
            return True
        except Exception:
            return False

    def close(self) -> None:
        """
        Close the RocksDict database.
        """
        self.db.close()

    def compact(self) -> None:
        """
        Compact the database to optimize storage.
        """
        self.db.compact_range(None, None)

    def get_all_keys(self, partition_key: str) -> list:
        """
        Get all keys from the RocksDict cache for a specific partition key.

        Returns:
            list: List of keys
        """
        # Get keys for specific partition
        prefix = f"cache:{partition_key}:"
        prefix_len = len(prefix)
        keys = []
        for key in self.db.keys():
            key_str = str(key)
            if key_str.startswith(prefix) and not key_str.startswith("_partition_metadata:"):
                keys.append(key_str[prefix_len:])
        return keys
    
    def get_partition_keys(self) -> list[tuple[str, str]]:
        """Get all partition keys and their datatypes."""
        result = []
        for key in self.db.keys():
            key_str = str(key)
            if key_str.startswith("_partition_metadata:"):
                partition_key = key_str.split(":", 1)[1]
                datatype = self.db.get(key)
                if datatype is not None:
                    result.append((partition_key, datatype))
        return sorted(result)

    @classmethod
    def get_instance(cls, db_path: str, read_only: bool = False):
        if cls._instance is None:
            cls._instance = cls(db_path, read_only=read_only)
        return cls._instance

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)
    
    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype not in self.get_supported_datatypes():
            raise ValueError(f"Unsupported datatype: {datatype}")
        self._set_partition_datatype(partition_key, datatype)
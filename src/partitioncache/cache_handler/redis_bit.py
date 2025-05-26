from functools import cache
import uuid
from datetime import datetime
from logging import getLogger

from bitarray import bitarray

from partitioncache.cache_handler.redis_abstract import RedisAbstractCacheHandler

logger = getLogger("PartitionCache")


class RedisBitCacheHandler(RedisAbstractCacheHandler):
    """
    Handles access to a Redis Bitarray cache.
    This handler supports multiple partition keys but only integer datatypes (for bit arrays).
    """


    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """Redis bit handler supports only integer datatype."""
        return {"integer"}

    def __repr__(self) -> str:
        return "redis_bit"

    def __init__(self, *args, **kwargs) -> None:
        self.default_bitsize = kwargs.pop("bitsize")  # Bitsize should be configured correctly by user (bitsize=1001 to store values 0-1000)
        super().__init__(*args, **kwargs)

    def _get_partition_bitsize(self, partition_key: str) -> int | None:
        """Get the bitsize for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        stored_bitsize = self.db.hget(metadata_key, "bitsize")
        if stored_bitsize is not None and isinstance(stored_bitsize, bytes):
            try:
                return int(stored_bitsize.decode())
            except ValueError:
                return None
        return None

    def _ensure_partition_exists(self, partition_key: str, bitsize: int | None = None) -> None:
        """Ensure partition exists with correct datatype and bitsize."""
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is None:
            # Create new partition with bitsize
            if bitsize is None:
                bitsize = self.default_bitsize
            self._set_partition_metadata(partition_key, "integer", bitsize)
        elif existing_datatype != "integer":
            raise ValueError(f"Partition key '{partition_key}' already exists with datatype '{existing_datatype}', cannot use datatype 'integer'")

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache namespace."""

        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        key_type = self.db.type(cache_key)
        if key_type == b"none":
            return None

        value = self.db.get(cache_key)
        if value == b"\x00":  # Check for null byte marker
            return None

        bitval = bitarray(value.decode())  # type: ignore
        return set(bitval.search(bitarray("1")))



    
    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key") -> set:
        """
        Checks in Redis which of the keys exists in cache and returns the set of existing keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return set()

        pipe = self.db.pipeline()
        cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
        for cache_key in cache_keys:
            pipe.type(cache_key)
        key_types = pipe.execute()

        existing_keys = set()
        for key, key_type in zip(keys, key_types):
            if key_type == b"string":
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

        pipe = self.db.pipeline()
        cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
        for cache_key in cache_keys:
            pipe.type(cache_key)
        key_types = pipe.execute()

        valid_cache_keys: list[str] = [cache_key for cache_key, key_type in zip(cache_keys, key_types) if key_type == b"string"]
        valid_keys_count = len(valid_cache_keys)

        randuuid = str(uuid.uuid4())

        if not valid_cache_keys:
            return None, 0
        elif len(valid_cache_keys) == 1:
            # Get the original key for the single valid cache key
            original_key = next(key for key, cache_key in zip(keys, cache_keys) if cache_key in valid_cache_keys)
            return self.get(original_key, partition_key=partition_key), 1
        else:
            temp_key = f"temp_{randuuid}"
            self.db.bitop("AND", temp_key, *valid_cache_keys)
            bitval = bitarray(self.db.get(temp_key).decode())  # type: ignore
            self.db.delete(temp_key)
            return set(bitval.search(bitarray("1"))), valid_keys_count

    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store a set in the cache for a specific partition key. Only integer values are supported."""
        if not value:
            return True
        # Ensure partition exists with correct datatype and bitsize
        self._ensure_partition_exists(partition_key)
        val = bitarray(self._get_partition_bitsize(partition_key))
        try:
            for k in value:
                if isinstance(k, int):
                    val[k] = 1
                elif isinstance(k, str):
                    val[int(k)] = 1
                else:
                    raise ValueError("Only integer values are supported")
        except (IndexError, ValueError):
            raise ValueError(f"Value {value} is out of range for bitarray of size {self._get_partition_bitsize(partition_key)}")
        try:
            cache_key = self._get_cache_key(key, partition_key)
            self.db.set(cache_key, val.to01())
            return True
        except Exception as e:
            logger.error(f"Failed to set value for key {key} in partition {partition_key}: {e}")
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("Redis bit handler supports only integer datatype")
        
        bitsize = kwargs.get("bitsize", self.default_bitsize)
        self._ensure_partition_exists(partition_key, bitsize)

  



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

        # Determine the bitsize to use
        if bitsize is None:
            bitsize = self.default_bitsize

        if existing_datatype is None:
            # Create new partition with bitsize
            self._set_partition_metadata(partition_key, "integer", bitsize)
        elif existing_datatype != "integer":
            raise ValueError(f"Partition key '{partition_key}' already exists with datatype '{existing_datatype}', cannot use datatype 'integer'")
        else:
            # If the partition already exists, still ensure the bitsize is set
            current_bitsize = self._get_partition_bitsize(partition_key)
            if current_bitsize is None:
                self._set_partition_metadata(partition_key, "integer", bitsize)

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

        return self._decode_bits(value)  # type: ignore[arg-type]

    @staticmethod
    def _decode_bits(value: bytes | None) -> set[int]:
        """Decode a packed bit array (bit i = Redis bit offset i, big-endian within each byte)."""
        bitval = bitarray(endian="big")
        if value:
            bitval.frombytes(value)
        return set(bitval.search(bitarray("1")))

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        type_pipe = self.db.pipeline()
        cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
        for cache_key in cache_keys:
            type_pipe.type(cache_key)
        key_types = type_pipe.execute()
        string_cache_keys: list[str] = [cache_key for cache_key, key_type in zip(cache_keys, key_types, strict=False) if key_type == b"string"]

        # The null marker is exactly b"\x00". A packed bit array is never that value: an empty set is stored as b"",
        # a non-empty set with bitsize <= 8 has a non-zero byte, and bitsize > 8 needs at least two bytes.
        marker_pipe = self.db.pipeline()
        for cache_key in string_cache_keys:
            marker_pipe.getrange(cache_key, 0, 1)
        prefixes = marker_pipe.execute() if string_cache_keys else []
        valid_cache_keys: list[str] = [cache_key for cache_key, prefix in zip(string_cache_keys, prefixes, strict=False) if prefix != b"\x00"]
        valid_keys_count = len(valid_cache_keys)

        randuuid = str(uuid.uuid4())

        if not valid_cache_keys:
            return None, 0
        elif len(valid_cache_keys) == 1:
            # Get the original key for the single valid cache key
            original_key = next(key for key, cache_key in zip(keys, cache_keys, strict=False) if cache_key in valid_cache_keys)
            return self.get(original_key, partition_key=partition_key), 1
        else:
            temp_key = f"temp_{randuuid}"
            self.db.bitop("AND", temp_key, *valid_cache_keys)
            value = self.db.get(temp_key)  # None if all sources were empty strings: Redis then creates no destination key
            self.db.delete(temp_key)
            return self._decode_bits(value), valid_keys_count  # type: ignore[arg-type]

    def set_cache(self, key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store a set of partition key identifiers in the cache for a specific partition key. Only integer values are supported.

        The bits are stored packed (one bit per partition key, big-endian within each byte, matching Redis bit offsets),
        so BITOP AND intersects them directly. An empty set is stored as b"", which get() returns as set(): the query
        has no rows. All-zero bytes would equal the b"\x00" null marker for bitsizes up to 8.
        """
        # Ensure partition exists with correct datatype and bitsize
        self._ensure_partition_exists(partition_key)
        bitsize = self._get_partition_bitsize(partition_key)
        if bitsize is None:
            # Fallback to default bitsize if metadata is corrupted or missing
            bitsize = self.default_bitsize
            self._set_partition_metadata(partition_key, "integer", bitsize)
        val = bitarray(bitsize, endian="big")
        val.setall(0)
        try:
            for k in partition_key_identifiers:
                if isinstance(k, str):
                    try:
                        k = int(k)
                    except ValueError:
                        raise TypeError(k) from None
                if not isinstance(k, int):
                    raise TypeError(k)
                if k < 0:  # bitarray[-1] would silently set the last bit
                    raise IndexError(k)
                val[k] = 1
        except TypeError as e:
            raise ValueError(f"Only integer values are supported for bit arrays: {e.args[0]!r}") from None
        except IndexError:
            raise ValueError(f"Partition key identifiers {partition_key_identifiers} is out of range for bitarray of size {bitsize}") from None
        try:
            cache_key = self._get_cache_key(key, partition_key)
            self.db.set(cache_key, val.tobytes() if val.any() else b"")
            return True
        except Exception as e:
            logger.error(f"Failed to set partition key identifiers for hash {key} in partition {partition_key}: {e}")
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("Redis bit handler supports only integer datatype")

        bitsize = kwargs.get("bitsize", self.default_bitsize)
        self._ensure_partition_exists(partition_key, bitsize)

from datetime import datetime
from logging import getLogger

from bitarray import bitarray
from pyroaring import BitMap

from partitioncache.cache_handler.redis_abstract import RedisAbstractCacheHandler

logger = getLogger("PartitionCache")


class RedisRoaringBitCacheHandler(RedisAbstractCacheHandler):
    """
    Handles access to a Redis cache using roaring bitmaps (pyroaring).
    Roaring bitmaps are stored as serialized bytes in Redis string keys.
    This handler supports only integer datatypes.
    No bitsize parameter is needed since roaring bitmaps are dynamically sized.
    """

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """Redis roaring bit handler supports only integer datatype."""
        return {"integer"}

    def __repr__(self) -> str:
        return "redis_roaringbit"

    def get(self, key: str, partition_key: str = "partition_key") -> BitMap | None:  # type: ignore[override]
        """Get value from partition-specific cache namespace as a BitMap."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        key_type = self.db.type(cache_key)
        if key_type == b"none":
            return None

        value = self.db.get(cache_key)
        if value is None or value == b"\x00":
            return None

        return BitMap.deserialize(value)

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[BitMap | None, int]:  # type: ignore[override]
        """Returns the intersection of all roaring bitmaps associated with the given keys."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        cache_keys = [self._get_cache_key(key, partition_key) for key in keys]

        # Use pipeline to check key types
        pipe = self.db.pipeline()
        for cache_key in cache_keys:
            pipe.type(cache_key)
        key_types = pipe.execute()

        valid_cache_keys = [cache_key for cache_key, key_type in zip(cache_keys, key_types, strict=False) if key_type == b"string"]

        if not valid_cache_keys:
            return None, 0

        # Fetch all valid values via mget
        values = self.db.mget(valid_cache_keys)

        result: BitMap | None = None
        count_match = 0
        for value in values:
            if value is None or value == b"\x00":
                continue
            bm = BitMap.deserialize(value)
            if result is None:
                result = bm
            else:
                result &= bm
            count_match += 1

        return result, count_match

    def set_cache(
        self,
        key: str,
        partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime] | BitMap | bitarray | list,
        partition_key: str = "partition_key",
    ) -> bool:
        """
        Store partition key identifiers as a serialized roaring bitmap.
        Accepts set[int], BitMap, bitarray, or list of integers.
        """
        if not partition_key_identifiers:
            return True

        # Ensure partition exists with integer datatype
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is None:
            self._set_partition_metadata(partition_key, "integer")
        elif existing_datatype != "integer":
            raise ValueError(f"Partition key '{partition_key}' has datatype '{existing_datatype}', but roaring bitmap handler supports only 'integer'")

        # Convert input to BitMap
        if isinstance(partition_key_identifiers, BitMap):
            bm = partition_key_identifiers
        elif isinstance(partition_key_identifiers, bitarray):
            bm = BitMap(i for i, bit in enumerate(partition_key_identifiers) if bit)
        elif isinstance(partition_key_identifiers, list | set):
            for item in partition_key_identifiers:
                if not isinstance(item, int):
                    raise ValueError(f"Only integer values are supported for roaring bitmaps, got {type(item)}")
            bm = BitMap(partition_key_identifiers)
        else:
            raise ValueError(f"Unsupported partition key identifier type: {type(partition_key_identifiers)}")

        try:
            cache_key = self._get_cache_key(key, partition_key)
            self.db.set(cache_key, bm.serialize())
            return True
        except Exception as e:
            logger.error(f"Failed to set partition key identifiers for hash {key} in partition {partition_key}: {e}")
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("Redis roaring bit handler supports only integer datatype")
        self._set_partition_metadata(partition_key, datatype)

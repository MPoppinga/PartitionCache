from datetime import datetime
from logging import getLogger

from bitarray import bitarray
from pyroaring import BitMap

from partitioncache.cache_handler.rocksdict_abstract import RocksDictAbstractCacheHandler

logger = getLogger("PartitionCache")


class RocksDictRoaringBitCacheHandler(RocksDictAbstractCacheHandler):
    """
    Handles access to a RocksDB cache using RocksDict with roaring bitmap serialization.
    Roaring bitmaps are stored as serialized bytes. Only integer datatypes are supported.
    No bitsize parameter is needed since roaring bitmaps are dynamically sized.
    """

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """RocksDict roaring bit handler supports only integer datatype."""
        return {"integer"}

    def __repr__(self) -> str:
        return "rocksdict_roaringbit"

    def get(self, key: str, partition_key: str = "partition_key") -> BitMap | None:  # type: ignore[override]
        """Get value from partition-specific cache namespace as a BitMap."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        result = self.db.get(cache_key)
        if result is None or result == "NULL":
            return None

        return BitMap.deserialize(result)

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[BitMap | None, int]:  # type: ignore[override]
        """Returns the intersection of all roaring bitmaps associated with the given keys."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        result: BitMap | None = None
        count_match = 0
        for key in keys:
            bm = self.get(key, partition_key=partition_key)
            if bm is None:
                continue
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
            self._set_partition_datatype(partition_key, "integer")
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
            logger.info(f"saving {len(bm)} partition key identifiers in cache {cache_key}")
            self.db[cache_key] = bm.serialize()
            return True
        except Exception:
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("RocksDict roaring bit handler supports only integer datatype")
        self._set_partition_datatype(partition_key, datatype)

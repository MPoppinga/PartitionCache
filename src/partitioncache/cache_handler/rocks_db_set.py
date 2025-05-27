import struct
from datetime import datetime
from logging import getLogger


from partitioncache.cache_handler.rocks_db_abstract import RocksDBAbstractCacheHandler

logger = getLogger("PartitionCache")


class RocksDBCacheHandler(RocksDBAbstractCacheHandler):
    """
    Handles access to a RocksDB cache.
    This handler supports multiple partition keys but only integer and string datatypes.
    """

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """RocksDB supports integer and text datatypes only."""
        return {"integer", "text"}

    def __repr__(self) -> str:
        return "rocksdb"

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache namespace."""
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        search_space_list = self.db.get(cache_key.encode())
        if search_space_list is None or search_space_list == b"\x00":
            return None

        if datatype == "text":
            return set(search_space_list.decode().split(","))
        elif datatype == "integer":
            return set(struct.unpack(f"!{(len(search_space_list) // 4)}I", search_space_list))
        else:
            raise ValueError(f"Unsupported datatype: {datatype}")

    def get_intersected(
        self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """
        RocksDB has no native intersection operation, so we have to do it manually.
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

    def _format_int_set(self, values: set[int]) -> bytes:
        """
        Format a set of integers into a byte string.
        """
        assert all(0 <= i <= 4294967295 for i in values)
        return b"".join(struct.pack("!I", i) for i in values)

    def _format_str_set(self, values: set[str]) -> bytes:
        """
        Format a set of strings into a byte string.
        """
        return b",".join(i.encode() for i in values)

    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store a set in the cache for a specific partition key."""
        # Try to get datatype from metadata
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is not None:
            if existing_datatype == "integer":
                struct_value = self._format_int_set(value)  # type: ignore
            elif existing_datatype == "text":
                struct_value = self._format_str_set(value)  # type: ignore
            else:
                raise ValueError(f"Unsupported datatype in metadata: {existing_datatype}")
            datatype = existing_datatype
        else:
            # Infer from value type
            sample = next(iter(value))
            if isinstance(sample, int):
                datatype = "integer"
                struct_value = self._format_int_set(value)  # type: ignore
            elif isinstance(sample, str):
                datatype = "text"
                struct_value = self._format_str_set(value)  # type: ignore
            else:
                raise ValueError(f"Unsupported value type: {type(sample)}")
            self._set_partition_datatype(partition_key, datatype)
        try:
            cache_key = self._get_cache_key(key, partition_key)
            logger.info(f"saving {len(value)} values in cache {cache_key}")
            self.db.put(cache_key.encode(), struct_value, sync=True)
            return True
        except Exception:
            return False

from datetime import datetime
from logging import getLogger

from partitioncache.cache_handler.rocksdict_abstract import RocksDictAbstractCacheHandler

logger = getLogger("PartitionCache")


class RocksDictCacheHandler(RocksDictAbstractCacheHandler):
    """
    Handles access to a RocksDB cache using RocksDict with native serialization.
    This handler supports multiple partition keys with datatypes: integer, float, text, timestamp.
    """

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """RocksDict supports all datatypes with native serialization."""
        return {"integer", "float", "text", "timestamp"}

    def __repr__(self) -> str:
        return "rocksdict"

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache namespace."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        result = self.db.get(cache_key)
        if result is None or result == "NULL":
            return None

        return result

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """Returns the intersection of all sets in the cache that are associated with the given keys."""
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

    def set_cache(self, key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store a set of partition key identifiers in the database for a specific partition key. RocksDict will handle serialization."""
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is not None:
            if existing_datatype != "integer" and existing_datatype != "float" and existing_datatype != "text" and existing_datatype != "timestamp":
                raise ValueError(f"Unsupported datatype in metadata: {existing_datatype}")
            datatype = existing_datatype
        else:
            sample = next(iter(partition_key_identifiers))
            if isinstance(sample, int):
                datatype = "integer"
            elif isinstance(sample, float):
                datatype = "float"
            elif isinstance(sample, str):
                datatype = "text"
            elif isinstance(sample, datetime):
                datatype = "timestamp"
            else:
                raise ValueError(f"Unsupported partition key identifier type: {type(sample)}")
            self._set_partition_datatype(partition_key, datatype)
        val = partition_key_identifiers

        try:
            cache_key = self._get_cache_key(key, partition_key)
            logger.info(f"saving {len(val)} partition key identifiers in cache {cache_key}")
            self.db[cache_key] = val
            return True
        except Exception:
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype not in self.get_supported_datatypes():
            raise ValueError(f"Unsupported datatype: {datatype}")
        self._set_partition_datatype(partition_key, datatype)

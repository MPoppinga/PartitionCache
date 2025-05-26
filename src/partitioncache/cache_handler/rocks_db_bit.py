from datetime import datetime
from bitarray import bitarray

from partitioncache.cache_handler.rocks_db_abstract import RocksDBAbstractCacheHandler


class RocksDBBitCacheHandler(RocksDBAbstractCacheHandler):
    """
    Handles access to a RocksDB cache using bitarrays for efficient storage.
    This handler supports multiple partition keys but only integer datatypes.
    """

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """RocksDB bit handler supports only integer datatype."""
        return {"integer"}

    def __repr__(self) -> str:
        return "rocksdb_bit"

    def __init__(self, db_path: str, read_only: bool = False, **kwargs) -> None:
        self.default_bitsize = kwargs.pop("bitsize", 1000000)  # Bitsize should be configured correctly by user (bitsize=1001 to store values 0-1000)
        super().__init__(db_path, read_only)

    def _get_partition_bitsize(self, partition_key: str) -> int | None:
        """Get the bitsize for a partition key from metadata."""
        bitsize_key = f"_partition_bitsize:{partition_key}"
        bitsize_bytes = self.db.get(bitsize_key.encode())
        if bitsize_bytes is not None:
            try:
                return int(bitsize_bytes.decode())
            except ValueError:
                return None
        return None

    def _set_partition_bitsize(self, partition_key: str, bitsize: int) -> None:
        """Set the bitsize for a partition key in metadata."""
        bitsize_key = f"_partition_bitsize:{partition_key}"
        self.db.put(bitsize_key.encode(), str(bitsize).encode(), sync=True)

    def _ensure_partition_exists(self, partition_key: str, bitsize: int | None = None) -> None:
        """Ensure partition exists with correct datatype and bitsize."""
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is None:
            # Create new partition with bitsize
            actual_bitsize: int = bitsize if bitsize is not None else self.default_bitsize
            self._set_partition_datatype(partition_key, "integer")
            self._set_partition_bitsize(partition_key, actual_bitsize)
        elif existing_datatype != "integer":
            raise ValueError(f"Partition key '{partition_key}' already exists with datatype '{existing_datatype}', cannot use datatype 'integer'")

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache namespace."""

        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        value = self.db.get(cache_key.encode())
        if value is None or value == b"\x00":
            return None

        bitval = bitarray()
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

        count_match = 0
        result: bitarray | None = None

        for key in keys:
            cache_key = self._get_cache_key(key, partition_key)
            value = self.db.get(cache_key.encode())
            if value is None or value == b"\x00":
                continue

            bitval = bitarray()
            bitval.frombytes(value)

            if result is None:
                result = bitval
                count_match += 1
            else:
                result &= bitval
                count_match += 1

        if result is None:
            return None, count_match
        else:
            return set(result.search(bitarray("1"))), count_match

    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store a set in the cache for a specific partition key. Only integer values are supported."""
        if not value:
            return True
        # Ensure partition exists with correct datatype and bitsize
        self._ensure_partition_exists(partition_key)

        # Get the correct bitsize for this partition
        bitsize = self._get_partition_bitsize(partition_key)
        if bitsize is None:
            bitsize = self.default_bitsize

        bitval = bitarray(bitsize)
        try:
            for k in value:
                if isinstance(k, int):
                    bitval[k] = 1
                elif isinstance(k, str):
                    bitval[int(k)] = 1
                else:
                    raise ValueError("Only integer values are supported")
        except (IndexError, ValueError):
            raise ValueError(f"Value {value} is out of range for bitarray of size {bitsize}")
        try:
            cache_key = self._get_cache_key(key, partition_key)
            self.db.put(cache_key.encode(), bitval.tobytes(), sync=True)
            return True
        except Exception:
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("RocksDB bit handler supports only integer datatype")

        bitsize = kwargs.get("bitsize", self.default_bitsize)
        self._ensure_partition_exists(partition_key, bitsize)

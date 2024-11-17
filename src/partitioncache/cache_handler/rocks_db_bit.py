from bitarray import bitarray
from rocksdb import (
    DB,  # type: ignore
    Options,  # type: ignore
)

from partitioncache.cache_handler.abstract import AbstractCacheHandler


class RocksDBBitCacheHandler(AbstractCacheHandler):
    """
    Handles access to a RocksDB cache using bitarrays for efficient storage.
    """

    def __init__(self, db_path: str, read_only: bool = False, bitsize: int = 1000000) -> None:
        self.db = DB(db_path, Options(create_if_missing=True), read_only=read_only)
        # TODO Evaluate RocksDB options for best performance, currently using defaults

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

        self.allow_lazy = False
        self.bitsize = bitsize + 1  # Add one to the bitsize to avoid off by one errors

    def get(self, key: str, settype=int) -> set[int] | None:
        if settype is not int:
            raise ValueError("Only integer values are supported")

        value = self.db.get(key.encode())
        if value is None or value == b"\x00":
            return None

        bitval = bitarray()
        bitval.frombytes(value)
        return set(bitval.search(bitarray("1")))

    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.
        """
        return self.db.get(key.encode()) is not None

    def get_intersected(self, keys: set[str], settype=int) -> tuple[set[int] | None, int]:
        """
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """
        count_match = 0
        result: bitarray | None = None

        for key in keys:
            value = self.db.get(key.encode())
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

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        if settype is not int:
            raise ValueError("Only integer values are supported")

        bitval = bitarray(self.bitsize)
        try:
            for k in value:
                bitval[k] = 1
        except IndexError:
            raise ValueError(f"Value {value} is out of range for bitarray of size {self.bitsize}")

        if not value:
            return

        self.db.put(key.encode(), bitval.tobytes(), sync=True)

    def set_null(self, key: str) -> None:
        self.db.put(key.encode(), b"\x00", sync=True)

    def delete(self, key: str) -> None:
        self.db.delete(key.encode())

    def close(self) -> None:
        """
        RocksDB does not have a close method, so we can skip this.
        """
        pass

    def compact(self) -> None:
        self.db.compact_range()

    def get_all_keys(self) -> list:
        """
        Get all keys from the RocksDB cache.

        Returns:
            list: _description_
        """
        keys = []
        it = self.db.iterkeys()
        it.seek_to_first()
        for key in it:
            keys.append(key.decode("utf-8"))
        return keys

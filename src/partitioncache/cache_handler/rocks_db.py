import struct
from logging import getLogger

from rocksdb import (
    DB,  # type: ignore
    Options,  # type: ignore
)

from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = getLogger("PartitionCache")


class RocksDBCacheHandler(AbstractCacheHandler):
    """
    Handles access to a RocksDB cache.

    """

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        self.db = DB(db_path, Options(create_if_missing=True), read_only=read_only)  # type: ignore

    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        search_space_list = self.db.get(key.encode())
        if search_space_list is None or search_space_list == b"\x00":
            return None
        if settype is str:
            return set(search_space_list.decode().split(","))
        else:
            return set(struct.unpack(f"!{(len(search_space_list)//4)}I", search_space_list))

    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.
        """
        return self.db.get(key.encode()) is not None

    def filter_existing_keys(self, keys: set) -> set:
        """
        Checks in RocksDB which of the keys exists in cache and returns the set of existing keys.
        """
        existing_keys = set()
        for key in keys:
            if self.db.get(key.encode()) is not None:
                existing_keys.add(key)
        return existing_keys

    def get_intersected(self, keys: set[str], settype=int) -> tuple[set[int] | None, int]:
        """
        RocksDB has no native intersection operation, so we have to do it manually.
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """
        count_match = 0
        result: set | None = None
        for key in keys:
            t = self.get(key, settype=settype)
            if t is None:
                continue
            if result is None:
                result = t
                count_match += 1
            else:
                result = result.intersection(t)
                count_match += 1
        return result, count_match

    def format_int_set(self, values: set[int]) -> bytes:
        """
        Format a set of integers into a byte string.
        """
        assert all(0 <= i <= 4294967295 for i in values)
        return b"".join(struct.pack("!I", i) for i in values)

    def format_str_set(self, values: set[str]) -> bytes:
        """
        Format a set of strings into a byte string.
        """
        return b",".join(i.encode() for i in values)

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        if settype is int:
            # Join all integers into a single byte string (!I)
            struct_value = self.format_int_set(value)  # type: ignore

        elif settype is str:
            struct_value = self.format_str_set(value)  # type: ignore
        else:
            raise ValueError(f"Unsupported type {settype}")

        logger.info(f"saving {len(value)} values in cache {key}")
        self.db.put(key.encode(), struct_value, sync=True)

    def set_null(self, key: str) -> None:
        self.db.put(key.encode(), b"\x00", sync=True)

    def is_null(self, key: str) -> bool:
        return self.db.get(key.encode()) == b"\x00"

    def delete(self, key: str) -> None:
        key_b = key.encode("utf-8")
        self.db.delete(key_b)

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

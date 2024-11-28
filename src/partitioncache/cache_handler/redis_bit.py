import uuid

import redis
from bitarray import bitarray

from partitioncache.cache_handler.redis import RedisCacheHandler


class RedisBitCacheHandler(RedisCacheHandler):
    """
    Handles access to a Redis Bitarray cace
    """

    def __init__(self, db_name, db_host, db_password, db_port, bitsize) -> None:
        """
        Initialize the cache handler with the given db name."""
        self.db = redis.Redis(host=db_host, port=db_port, db=db_name, password=db_password)
        self.bitsize = bitsize + 1  # Add one to the bitsize to avoid off by one errors

    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        if settype is not int:
            raise ValueError("Only integer values are supported")

        key_type = self.db.type(key)
        if key_type == b"none":
            return None

        value = self.db.get(key)
        if value == "\x00":  # Check for null byte marker
            return None

        bitval = bitarray(value.decode())  # type: ignore
        return set(bitval.search(bitarray("1")))

    def get_intersected(self, keys: set[str], settype=int) -> tuple[set[int] | set[str] | None, int]:
        """
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """

        pipe = self.db.pipeline()
        for key in keys:
            pipe.type(key)
        key_types = pipe.execute()

        valid_keys: list[str] = [key for key, key_type in zip(keys, key_types) if key_type == b"string"]

        randuuid = str(uuid.uuid4())

        if not valid_keys:
            return None, 0
        elif len(valid_keys) == 1:
            return self.get(valid_keys[0], settype), 1
        else:
            self.db.bitop("AND", f"temp_{randuuid}", *valid_keys)
            bitval = bitarray(self.db.get(f"temp_{randuuid}").decode())  # type: ignore
            self.db.delete(f"temp_{randuuid}")
            return set(bitval.search(bitarray("1"))), len(valid_keys)

    def filter_existing_keys(self, keys: set) -> set:
        """
        Checks in redis which of the keys exists in cache and returns the set of existing keys.
        """
        pipe = self.db.pipeline()
        for key in keys:
            pipe.type(key)
        key_types = pipe.execute()
        return set([key for key, key_type in zip(keys, key_types) if key_type == b"string"])

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        if settype is str:
            raise ValueError("Only integer values are supported")

        val = bitarray(self.bitsize)
        try:
            for k in value:
                val[k] = 1
        except IndexError:
            raise ValueError(f"Value {value} is out of range for bitarray of size {self.bitsize}")

        if not value:
            return

        self.db.set(key, val.to01())

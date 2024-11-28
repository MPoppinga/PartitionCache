import redis

from partitioncache.cache_handler.abstract import AbstractCacheHandler


class RedisCacheHandler(AbstractCacheHandler):
    """
    Handles access to a RocksDB cache.
    """

    def __init__(self, db_name, db_host, db_password, db_port) -> None:
        """
        Initialize the cache handler with the given db name."""
        self.db = redis.Redis(host=db_host, port=db_port, db=db_name, password=db_password)

    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        key_type = self.db.type(key)

        if key_type == b"none":
            return None

        if key_type == b"string":
            value = self.db.get(key)
            if value == "\x00":  # Check for null byte marker
                return None
            raise ValueError(f"The key '{key}' contains a string, not a set")

        if key_type != b"set":
            raise ValueError(f"The key '{key}' does not contain a set")

        members = self.db.smembers(key)

        if settype is int:
            return set(int(member) for member in members)  # type: ignore
        elif settype is str:
            return set(member.decode() for member in members)  # type: ignore

        else:
            raise ValueError(f"Unsupported set type: {settype}")

    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.
        """
        return self.db.exists(key) != 0

    def filter_existing_keys(self, keys: set) -> set:
        """
        Checks in redis which of the keys exists in cache and returns the set of existing keys.
        """
        pipe = self.db.pipeline()
        for key in keys:
            pipe.type(key)
        key_types = pipe.execute()
        return set([key for key, key_type in zip(keys, key_types) if key_type == b"set"])

    def get_intersected(self, keys: set[str], settype=int) -> tuple[set[int] | set[str] | None, int]:
        """
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """

        pipe = self.db.pipeline()
        for key in keys:
            pipe.type(key)
        key_types = pipe.execute()

        valid_keys: list[str] = [key for key, key_type in zip(keys, key_types) if key_type == b"set"]

        if not valid_keys:
            return None, 0
        elif len(valid_keys) == 1:
            return self.get(valid_keys[0], settype), 1
        else:
            intersected = self.db.sinter(*valid_keys)  # type: ignore

            if settype is int:
                return set(int(member) for member in intersected), len(valid_keys)  # type: ignore
            elif settype is str:
                return set(member.decode() for member in intersected), len(valid_keys)  # type: ignore
            else:
                raise ValueError(f"Unsupported set type: {settype}")

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        if settype is int or settype is str:
            self.db.sadd(key, *value)
        else:
            raise ValueError(f"Unsupported set type: {settype}")

    def set_null(self, key: str) -> None:
        self.db.set(key, "\x00")

    def is_null(self, key: str) -> bool:
        return self.db.get(key) == "\x00"

    def delete(self, key: str) -> None:
        self.db.delete(key)

    def close(self) -> None:
        self.db.close()

    def get_all_keys(self) -> list:
        return [key.decode() for key in self.db.keys("*")]  # type: ignore

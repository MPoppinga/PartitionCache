from rocksdict import Rdict, Options, AccessType
from partitioncache.cache_handler.abstract import AbstractCacheHandler
from logging import getLogger

logger = getLogger("PartitionCache")


class RocksDictCacheHandler(AbstractCacheHandler):
    """
    Handles access to a RocksDB cache using RocksDict with native serialization.
    """

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        opts = Options(raw_mode=False)
        # Optimize for read performance
        opts.create_if_missing(True)
        opts.set_max_background_jobs(4)
        opts.optimize_for_point_lookup(64 * 1024 * 1024)  # 64MB block cache
        opts.set_max_open_files(-1)  # Keep all files open
        opts.set_allow_mmap_reads(True)  # Enable memory-mapped reads
        opts.increase_parallelism(4)  # Increase parallel operations
        self.db = Rdict(db_path, options=opts, access_type=AccessType.read_only() if read_only else AccessType.read_write())
        self.allow_lazy = False


    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        # get from DB
        result = self.db.get(key)
        if result is None or result == "NULL":
            return None

        return result

    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.
        """
        return key in self.db

    def get_intersected(self, keys: set[str], settype=int) -> tuple[set[int] | None, int]:
        """
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

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        """
        Store a set in the database. RocksDict will handle serialization.
        """
        logger.info(f"saving {len(value)} values in cache {key}")
        self.db[key] = value

    def set_null(self, key: str) -> None:
        """
        Store a null value in the database.
        """
        self.db[key] = "NULL"


    def delete(self, key: str) -> None:
        """
        Delete a key from the database.
        """
        if key in self.db:
            del self.db[key]

    def close(self) -> None:
        """
        Close the RocksDict database.
        """
        self.db.close()

    def compact(self) -> None:
        """
        Compact the database to optimize storage.
        """
        self.db.compact_range(None, None)

    def get_all_keys(self) -> list[str]:
        """
        Get all keys from the RocksDB cache.

        Returns:
            list[str]: List of keys
        """
        return [str(key) for key in self.db.keys()]

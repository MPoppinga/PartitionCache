import shelve
from pathlib import Path
from logging import getLogger
from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = getLogger("PartitionCache")


class ShelfCacheHandler(AbstractCacheHandler):
    """
    Handles access to a cache using shelve module.
    """

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        logger.warning(
            "ShelfCacheHandler does not support concurrent writes, "
            "New enties are only visible after the database is closed and reopened."
            "Use another cache backend if concurrent operation is desired."
        )

        # Ensure the parent directory exists
        db_path = str(Path(db_path).resolve())  # Convert to absolute path
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # Make sure the path doesn't end with .dat/.dir/.bak
        if db_path.endswith((".dat", ".dir", ".bak")):
            db_path = db_path[:-4]

        try:
            # Open the shelf file with appropriate flag
            flag = "r" if read_only else "c"
            self.db = shelve.open(db_path, flag=flag, writeback=True)
        except Exception as e:
            raise RuntimeError(f"Failed to open shelf database at {db_path}: {str(e)}")

        self.allow_lazy = False


    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        # If not in cache, get from DB
        try:
            result = self.db[key]
            if result == "NULL":
                return None
            return result
        except KeyError:
            return None

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
        Store a set in the database.
        """
        self.db[key] = value
        self.db.sync()  # Ensure data is written to disk

    def set_null(self, key: str) -> None:
        """
        Store a null value in the database.
        """
        self.db[key] = "NULL"
        self.db.sync()


    def delete(self, key: str) -> None:
        """
        Delete a key from the database.
        """
        try:
            del self.db[key]
            self.db.sync()
        except KeyError:
            pass


    def close(self) -> None:
        """
        Close the shelf database.
        """
        self.db.close()

    def compact(self) -> None:
        """
        Shelf doesn't support compaction, but we can sync to ensure all data is written.
        """
        self.db.sync()

    def get_all_keys(self) -> list[str]:
        """
        Get all keys from the shelf cache.

        Returns:
            list[str]: List of keys
        """
        return list(self.db.keys())

    def filter_existing_keys(self, keys: set) -> set:
        """
        Filter out keys that don't exist in the database.

        Args:
            keys (set): Set of keys to filter

        Returns:
            set: Set of existing keys
        """
        return {key for key in keys if key in self.db}

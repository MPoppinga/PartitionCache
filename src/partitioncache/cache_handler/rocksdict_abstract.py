from abc import abstractmethod
from datetime import datetime
from logging import getLogger

from rocksdict import AccessType, Options, Rdict

from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = getLogger("PartitionCache")


class RocksDictAbstractCacheHandler(AbstractCacheHandler):
    """
    Abstract base class for RocksDict-backed cache handlers.
    Provides shared metadata, query, key management, and lifecycle operations.
    Subclasses must implement get(), get_intersected(), set_cache(),
    get_supported_datatypes(), __repr__(), and register_partition_key().
    """

    _instance = None
    _refcount = 0
    _current_path = None

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        opts = Options(raw_mode=False)
        opts.create_if_missing(True)
        opts.set_max_background_jobs(4)
        opts.optimize_for_point_lookup(64 * 1024 * 1024)  # 64MB block cache
        opts.set_max_open_files(-1)
        opts.set_allow_mmap_reads(True)
        opts.increase_parallelism(4)
        self.db = Rdict(db_path, options=opts, access_type=AccessType.read_only() if read_only else AccessType.read_write())

        self._ensure_metadata_structure()

    def _ensure_metadata_structure(self) -> None:
        """Ensure metadata structure exists for tracking partition keys."""
        pass

    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        datatype = self.db.get(metadata_key)
        return datatype if datatype is not None and datatype != "NULL" else None

    def _set_partition_datatype(self, partition_key: str, datatype: str) -> None:
        """Set the datatype for a partition key in metadata."""
        metadata_key = f"_partition_metadata:{partition_key}"
        self.db[metadata_key] = datatype

    def _get_cache_key(self, key: str, partition_key: str) -> str:
        """Get the RocksDict key for a cache entry with partition key namespace."""
        return f"cache:{partition_key}:{key}"

    def exists(self, key: str, partition_key: str = "partition_key", check_query: bool = False) -> bool:
        """Returns True if the key exists in the partition-specific cache, otherwise False."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        if not check_query:
            cache_key = self._get_cache_key(key, partition_key)
            return cache_key in self.db
        else:
            query_status = self.get_query_status(key, partition_key)
            if query_status is None:
                return False
            elif query_status == "ok":
                cache_key = self._get_cache_key(key, partition_key)
                return cache_key in self.db
            else:
                return True

    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key", check_query: bool = False) -> set:
        """Checks in RocksDict which of the keys exists in cache and returns the set of existing keys."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return set()

        existing_keys = set()
        for key in keys:
            if not check_query:
                cache_key = self._get_cache_key(key, partition_key)
                if cache_key in self.db:
                    existing_keys.add(key)
            else:
                query_status = self.get_query_status(key, partition_key)
                if query_status is None:
                    continue
                elif query_status == "ok":
                    cache_key = self._get_cache_key(key, partition_key)
                    if cache_key in self.db:
                        existing_keys.add(key)
                else:
                    existing_keys.add(key)

        return existing_keys

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Store a null value in the database for a specific partition key."""
        try:
            existing_datatype = self._get_partition_datatype(partition_key)
            if existing_datatype is None:
                self._set_partition_datatype(partition_key, "integer")

            cache_key = self._get_cache_key(key, partition_key)
            self.db[cache_key] = "NULL"
            return True
        except Exception:
            return False

    def is_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Check if key has null value in partition-specific cache."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        cache_key = self._get_cache_key(key, partition_key)
        return self.db.get(cache_key) == "NULL"

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """Delete a key from the database for a specific partition key."""
        try:
            cache_key = self._get_cache_key(key, partition_key)
            if cache_key in self.db:
                del self.db[cache_key]

            query_key = f"query:{partition_key}:{key}"
            if query_key in self.db:
                del self.db[query_key]

            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)
            if limit_key in self.db:
                del self.db[limit_key]
            if timeout_key in self.db:
                del self.db[timeout_key]

            return True
        except Exception:
            return False

    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """Store a query in the cache associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data = {"query": querytext, "partition_key": partition_key, "last_seen": datetime.now().isoformat(), "status": "ok"}
            self.db[query_key] = query_data
            return True
        except Exception:
            return False

    def get_query(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Retrieve the query text associated with the given key."""
        try:
            query_key = f"query:{partition_key}:{key}"
            query_data = self.db.get(query_key)
            if query_data and isinstance(query_data, dict):
                return query_data.get("query")
            return None
        except Exception:
            return None

    def get_all_queries(self, partition_key: str) -> list[tuple[str, str]]:
        """Retrieve all query hash and text pairs for a specific partition."""
        try:
            prefix = f"query:{partition_key}:"
            queries = []

            for key in self.db.keys():
                key = str(key)
                if key.startswith(prefix):
                    query_data = self.db.get(key)
                    if query_data and isinstance(query_data, dict) and "query" in query_data:
                        query_hash = key.split(":", 2)[-1]
                        queries.append((query_hash, query_data["query"]))

            return queries
        except Exception:
            return []

    def close(self) -> None:
        """Close the RocksDict database."""
        self._refcount -= 1
        if self._refcount <= 0:
            try:
                self.db.close()
            except Exception:
                pass
            self._instance = None
            self._refcount = 0

    def compact(self) -> None:
        """Compact the database to optimize storage."""
        self.db.compact_range(None, None)

    def get_all_keys(self, partition_key: str) -> list:
        """Get all keys from the RocksDict cache for a specific partition key."""
        prefix = f"cache:{partition_key}:"
        prefix_len = len(prefix)
        keys = []
        for key in self.db.keys():
            key_str = str(key)
            if key_str.startswith(prefix) and not key_str.startswith("_partition_metadata:"):
                keys.append(key_str[prefix_len:])
        return keys

    def get_partition_keys(self) -> list[tuple[str, str]]:
        """Get all partition keys and their datatypes."""
        result = []
        for key in self.db.keys():
            key_str = str(key)
            if key_str.startswith("_partition_metadata:"):
                partition_key = key_str.split(":", 1)[1]
                datatype = self.db.get(key)
                if datatype is not None:
                    result.append((partition_key, datatype))
        return sorted(result)

    @classmethod
    def get_instance(cls, db_path: str, read_only: bool = False):
        if cls._instance is None or cls._current_path != db_path:
            if cls._instance is not None:
                try:
                    cls._instance.db.close()
                except Exception:
                    pass
                cls._instance = None
                cls._refcount = 0
            cls._instance = cls(db_path, read_only=read_only)
            cls._current_path = db_path
        cls._refcount += 1
        return cls._instance

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def set_query_status(self, key: str, partition_key: str = "partition_key", status: str = "ok") -> bool:
        """Set the status of a query using termination bits for RocksDict backend."""
        try:
            valid_statuses = {"ok", "timeout", "failed"}
            if status not in valid_statuses:
                raise ValueError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

            if status == "ok":
                if limit_key in self.db:
                    del self.db[limit_key]
                if timeout_key in self.db:
                    del self.db[timeout_key]
                query_key = f"query:{partition_key}:{key}"
                query_data = self.db.get(query_key)
                if query_data and isinstance(query_data, dict):
                    query_data["status"] = "ok"
                    query_data["last_seen"] = datetime.now().isoformat()
                    self.db[query_key] = query_data
            elif status == "failed":
                self.db[limit_key] = "NULL"
                if timeout_key in self.db:
                    del self.db[timeout_key]
                query_key = f"query:{partition_key}:{key}"
                query_data = self.db.get(query_key)
                if query_data and isinstance(query_data, dict):
                    query_data["status"] = "failed"
                    query_data["last_seen"] = datetime.now().isoformat()
                    self.db[query_key] = query_data
            elif status == "timeout":
                self.db[timeout_key] = "NULL"
                if limit_key in self.db:
                    del self.db[limit_key]
                query_key = f"query:{partition_key}:{key}"
                query_data = self.db.get(query_key)
                if query_data and isinstance(query_data, dict):
                    query_data["status"] = "timeout"
                    query_data["last_seen"] = datetime.now().isoformat()
                    self.db[query_key] = query_data

            return True
        except Exception as e:
            logger.error(f"Failed to set query status for key {key}: {e}")
            return False

    def get_query_status(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Get the status of a query from termination bits for RocksDict backend."""
        try:
            limit_key = self._get_cache_key(f"_LIMIT_{key}", partition_key)
            timeout_key = self._get_cache_key(f"_TIMEOUT_{key}", partition_key)

            if limit_key in self.db:
                return "failed"
            elif timeout_key in self.db:
                return "timeout"

            query_key = f"query:{partition_key}:{key}"
            query_data = self.db.get(query_key)
            if query_data and isinstance(query_data, dict):
                status = query_data.get("status")
                if status:
                    return status
                return "ok"

            cache_key = self._get_cache_key(key, partition_key)
            if cache_key in self.db:
                return "ok"

            return None
        except Exception:
            return None

    @abstractmethod
    def get(self, key: str, partition_key: str = "partition_key"):
        raise NotImplementedError

    @abstractmethod
    def get_intersected(self, keys: set[str], partition_key: str = "partition_key"):
        raise NotImplementedError

    @abstractmethod
    def set_cache(self, key: str, partition_key_identifiers, partition_key: str = "partition_key") -> bool:
        raise NotImplementedError

    @abstractmethod
    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        raise NotImplementedError

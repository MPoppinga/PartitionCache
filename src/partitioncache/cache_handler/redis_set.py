from datetime import datetime

from partitioncache.cache_handler.redis_abstract import RedisAbstractCacheHandler


class RedisCacheHandler(RedisAbstractCacheHandler): 
    """
    Handles access to a Redis cache.
    This handler supports multiple partition keys but only integer and string datatypes.
    """
    


    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """Redis supports integer and text datatypes only."""
        return {"integer", "text"}

    def __repr__(self) -> str:
        return "redis"
    
   


    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache namespace."""
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None
        
        cache_key = self._get_cache_key(key, partition_key)
        key_type = self.db.type(cache_key)

        if key_type == b"none":
            return None

        if key_type == b"string":
            value = self.db.get(cache_key)
            if value == b"\x00":  # Check for null byte marker
                return None
            raise ValueError(f"The key '{cache_key}' contains a string, not a set")

        if key_type != b"set":
            raise ValueError(f"The key '{cache_key}' does not contain a set")

        members = self.db.smembers(cache_key)
        
        
        settype = None
        datatype = self._get_partition_datatype(partition_key)
        if datatype is not None:
            if datatype == "integer":
                settype = int
            elif datatype == "text":
                settype = str
            else:
                raise ValueError(f"Unsupported datatype: {datatype}")

        if settype is int:
            return set(int(member) for member in members)  # type: ignore
        elif settype is str:
            return set(member.decode() for member in members)  # type: ignore
        else:
            raise ValueError(f"Unsupported set type: {settype}")


    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key") -> set:
        """
        Checks in redis which of the keys exists in cache and returns the set of existing keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return set()
        
        pipe = self.db.pipeline()
        cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
        for cache_key in cache_keys:
            pipe.type(cache_key)
        key_types = pipe.execute()
        
        existing_keys = set()
        for key, key_type in zip(keys, key_types):
            if key_type == b"set":
                existing_keys.add(key)
        return existing_keys

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """
        Returns the intersection of all sets in the cache that are associated with the given keys.
        """
        # Check if partition exists
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        pipe = self.db.pipeline()
        cache_keys = [self._get_cache_key(key, partition_key) for key in keys]
        for cache_key in cache_keys:
            pipe.type(cache_key)
        key_types = pipe.execute()

        valid_cache_keys: list[str] = [cache_key for cache_key, key_type in zip(cache_keys, key_types) if key_type == b"set"]
        valid_keys_count = len(valid_cache_keys)

        if not valid_cache_keys:
            return None, 0
        elif len(valid_cache_keys) == 1:
            # Get the original key for the single valid cache key
            original_key = next(key for key, cache_key in zip(keys, cache_keys) if cache_key in valid_cache_keys)
            return self.get(original_key, partition_key), 1
        else:
            intersected = self.db.sinter(*valid_cache_keys)  # type: ignore
            
        settype = None
        datatype = self._get_partition_datatype(partition_key)
        if datatype is not None:
            if datatype == "integer":
                settype = int
            elif datatype == "text":
                settype = str
            else:
                raise ValueError(f"Unsupported datatype: {datatype}")

            if settype is int:
                return set(int(member) for member in intersected), valid_keys_count  # type: ignore
            elif settype is str:
                return set(member.decode() for member in intersected), valid_keys_count  # type: ignore
            else:
                raise ValueError(f"Unsupported set type: {settype}")
        return None, 0 

    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store a set in the cache for a specific partition key."""
        # Try to get datatype from metadata
        existing_datatype = self._get_partition_datatype(partition_key)
        if existing_datatype is not None:
            if existing_datatype == "integer":
                str_values = [str(v) for v in value]
            elif existing_datatype == "text":
                str_values = [str(v) for v in value]
            else:
                raise ValueError(f"Unsupported datatype in metadata: {existing_datatype}")
            datatype = existing_datatype
        else:
            # Infer from value type
            sample = next(iter(value))
            if isinstance(sample, int):
                datatype = "integer"
                str_values = [str(v) for v in value]
            elif isinstance(sample, str):
                datatype = "text"
                str_values = [str(v) for v in value]
            else:
                raise ValueError(f"Unsupported value type: {type(sample)}")
            self._set_partition_datatype(partition_key, datatype)
        try:
            cache_key = self._get_cache_key(key, partition_key)
            self.db.sadd(cache_key, *str_values)
            return True
        except Exception:
            return False



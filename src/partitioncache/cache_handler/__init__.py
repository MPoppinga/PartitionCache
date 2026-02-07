import os

from partitioncache.cache_handler.abstract import AbstractCacheHandler
from partitioncache.cache_handler.environment_config import EnvironmentConfigManager


def get_cache_handler(cache_type: str, singleton: bool = False) -> AbstractCacheHandler:
    # Handle backward compatibility for old cache type names
    if cache_type == "redis":
        cache_type = "redis_set"
    elif cache_type == "rocksdb":
        cache_type = "rocksdb_set"

    if cache_type == "postgresql_array":
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        config = EnvironmentConfigManager.get_postgresql_array_config()
        if singleton:
            return PostgreSQLArrayCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return PostgreSQLArrayCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "postgresql_bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

        config = EnvironmentConfigManager.get_postgresql_bit_config()
        if singleton:
            return PostgreSQLBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return PostgreSQLBitCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "redis_set":
        from partitioncache.cache_handler.redis_set import RedisCacheHandler

        config = EnvironmentConfigManager.get_redis_config("set")
        if singleton:
            return RedisCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RedisCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "redis_bit":
        from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler

        config = EnvironmentConfigManager.get_redis_config("bit")
        if singleton:
            return RedisBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RedisBitCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "rocksdb_set":
        from partitioncache.cache_handler.rocks_db_set import RocksDBCacheHandler

        config = EnvironmentConfigManager.get_rocksdb_config("set")
        if singleton:
            return RocksDBCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RocksDBCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "rocksdb_bit":
        from partitioncache.cache_handler.rocks_db_bit import RocksDBBitCacheHandler

        config = EnvironmentConfigManager.get_rocksdb_config("bit")
        if singleton:
            return RocksDBBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RocksDBBitCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "postgresql_roaringbit":
        from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

        config = EnvironmentConfigManager.get_postgresql_roaringbit_config()
        if singleton:
            return PostgreSQLRoaringBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return PostgreSQLRoaringBitCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "redis_roaringbit":
        from partitioncache.cache_handler.redis_roaringbit import RedisRoaringBitCacheHandler

        config = EnvironmentConfigManager.get_redis_config("roaringbit")
        if singleton:
            return RedisRoaringBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RedisRoaringBitCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "rocksdict":
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        config = EnvironmentConfigManager.get_rocksdb_config("dict")
        if singleton:
            return RocksDictCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RocksDictCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "rocksdict_roaringbit":
        from partitioncache.cache_handler.rocksdict_roaringbit import RocksDictRoaringBitCacheHandler

        config = EnvironmentConfigManager.get_rocksdb_config("roaringbit")
        if singleton:
            return RocksDictRoaringBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return RocksDictRoaringBitCacheHandler(**config)  # type: ignore[no-any-return]
    elif cache_type == "duckdb_bit":
        from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

        config = EnvironmentConfigManager.get_duckdb_bit_config()
        if singleton:
            return DuckDBBitCacheHandler.get_instance(**config)  # type: ignore[no-any-return]
        else:
            return DuckDBBitCacheHandler(**config)  # type: ignore[no-any-return]
    else:
        raise ValueError(f"Unsupported cache type: {cache_type}")

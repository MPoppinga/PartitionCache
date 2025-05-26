"""
PartitionCache - A caching middleware for partition-based query optimization.

This module provides an API for creating and managing partition cache handlers
with automatic datatype validation and consistent interfaces.
"""

from datetime import datetime
from typing import Union

from partitioncache.apply_cache import extend_query_with_partition_keys, get_partition_keys, get_partition_keys_lazy
from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.helper import PartitionCacheHelper, create_partitioncache_helper
try:
    from partitioncache.cache_handler.rocks_db_set import RocksDBCacheHandler
    from partitioncache.cache_handler.rocks_db_bit import RocksDBBitCacheHandler
    ROCKSDB_AVAILABLE = True
except ImportError:
    RocksDBCacheHandler = None
    RocksDBBitCacheHandler = None
    ROCKSDB_AVAILABLE = False
from partitioncache.queue import get_queue_lengths, push_to_original_query_queue, push_to_query_fragment_queue

# Type aliases for better API clarity
DataType = Union[int, str, float, datetime]
DataSet = Union[set[int], set[str], set[float], set[datetime]]


def create_cache_helper(cache_type: str, partition_key: str, datatype: str | None) -> PartitionCacheHelper:
    """
    Create a partition cache handler
    """
    
    # Create the underlying cache handler first
    cache_handler = get_cache_handler(cache_type, singleton=True)   
    return create_partitioncache_helper(cache_handler, partition_key, datatype)
    


def list_cache_types() -> dict[str, list[str]]:
    """
    List all available cache types and their supported datatypes.

    Returns:
        dict: Mapping of cache type to list of supported datatypes
    """
    # List of known handler names and their classes
    from partitioncache.cache_handler.redis_set import RedisCacheHandler
    from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler
    from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler
    from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
    from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

    handler_classes = {
        "redis": RedisCacheHandler,
        "redis_bit": RedisBitCacheHandler,
        "rocksdict": RocksDictCacheHandler,
        "postgresql_array": PostgreSQLArrayCacheHandler,
        "postgresql_bit": PostgreSQLBitCacheHandler,
    }
    
    if ROCKSDB_AVAILABLE:
        handler_classes["rocksdb"] = RocksDBCacheHandler
        handler_classes["rocksdb_bit"] = RocksDBBitCacheHandler
    return {name: sorted(list(cls.get_supported_datatypes())) for name, cls in handler_classes.items()}


__all__ = [
    "create_cache_helper",
    "list_cache_types",
    "get_partition_keys",
    "get_partition_keys_lazy",
    "extend_query_with_partition_keys",
    "push_to_original_query_queue",
    "push_to_query_fragment_queue",
    "get_queue_lengths",
    "PartitionCacheHelper",
    "DataType",
    "DataSet",
]

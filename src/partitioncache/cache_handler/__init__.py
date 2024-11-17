import os
from partitioncache.cache_handler.abstract import AbstractCacheHandler


def get_cache_handler(cache_type: str) -> AbstractCacheHandler:
    if cache_type == "postgresql_array":
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
        
        db_name = os.getenv("DB_NAME")
        if not db_name:
            raise ValueError("DB_NAME environment variable not set")
        db_host = os.getenv("DB_HOST") 
        if not db_host:
            raise ValueError("DB_HOST environment variable not set")
        db_user = os.getenv("DB_USER")
        if not db_user:
            raise ValueError("DB_USER environment variable not set")
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            raise ValueError("DB_PASSWORD environment variable not set")
        db_port = os.getenv("DB_PORT")
        if not db_port:
            raise ValueError("DB_PORT environment variable not set")
        db_table = os.getenv("PG_CACHE_TABLE")
        if not db_table:
            raise ValueError("PG_CACHE_TABLE environment variable not set")
        return PostgreSQLArrayCacheHandler(
            db_name=db_name,
            db_host=db_host,
            db_user=db_user,
            db_password=db_password,
            db_port=db_port,
            db_table=db_table,
        )
    elif cache_type == "postgresql_bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
        
        db_name = os.getenv("DB_NAME")
        if not db_name:
            raise ValueError("DB_NAME environment variable not set")
        db_host = os.getenv("DB_HOST")
        if not db_host:
            raise ValueError("DB_HOST environment variable not set") 
        db_user = os.getenv("DB_USER")
        if not db_user:
            raise ValueError("DB_USER environment variable not set")
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            raise ValueError("DB_PASSWORD environment variable not set")
        db_port = os.getenv("DB_PORT")
        if not db_port:
            raise ValueError("DB_PORT environment variable not set")
        db_table = os.getenv("PG_BIT_CACHE_TABLE")
        if not db_table:
            raise ValueError("PG_BIT_CACHE_TABLE environment variable not set")
        bitsize = os.getenv("PG_BIT_CACHE_BITSIZE")
        if not bitsize:
            raise ValueError("PG_BIT_CACHE_BITSIZE environment variable not set")
        return PostgreSQLBitCacheHandler(
            db_name=db_name,
            db_host=db_host,
            db_user=db_user,
            db_password=db_password,
            db_port=db_port,
            db_table=db_table,
            bitsize=int(bitsize),
        )
    elif cache_type == "redis":
        from partitioncache.cache_handler.redis import RedisCacheHandler
        
        db_name = os.getenv("REDIS_CACHE_DB")
        if not db_name:
            raise ValueError("REDIS_CACHE_DB environment variable not set")
        db_host = os.getenv("REDIS_HOST")
        if not db_host:
            raise ValueError("REDIS_HOST environment variable not set")
        db_password = os.getenv("REDIS_PASSWORD", "")
        db_port = os.getenv("REDIS_PORT")
        if not db_port:
            raise ValueError("REDIS_PORT environment variable not set")
        return RedisCacheHandler(
            db_name=db_name,
            db_host=db_host,
            db_password=db_password,
            db_port=db_port,
        )
    elif cache_type == "redis_bit":
        from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler
        
        db_name = os.getenv("REDIS_BIT_DB")
        if not db_name:
            raise ValueError("REDIS_BIT_DB environment variable not set")
        db_host = os.getenv("REDIS_HOST")
        if not db_host:
            raise ValueError("REDIS_HOST environment variable not set")
        db_password = os.getenv("REDIS_PASSWORD", "")
        db_port = os.getenv("REDIS_PORT")
        if not db_port:
            raise ValueError("REDIS_PORT environment variable not set")
        bitsize = os.getenv("REDIS_BIT_BITSIZE")
        if not bitsize:
            raise ValueError("REDIS_BIT_BITSIZE environment variable not set")
        return RedisBitCacheHandler(
            db_name=db_name,
            db_host=db_host,
            db_password=db_password,
            db_port=db_port,
            bitsize=int(bitsize),
        )
    elif cache_type == "rocksdb":
        from partitioncache.cache_handler.rocks_db import RocksDBCacheHandler
        
        db_path = os.getenv("ROCKSDB_PATH")
        if not db_path:
            raise ValueError("ROCKSDB_PATH environment variable not set")
        return RocksDBCacheHandler(db_path=db_path)
    elif cache_type == "rocksdb_bit":
        from partitioncache.cache_handler.rocks_db_bit import RocksDBBitCacheHandler
        
        db_path = os.getenv("ROCKSDB_BIT_PATH")
        if not db_path:
            raise ValueError("ROCKSDB_BIT_PATH environment variable not set")
        bitsize = os.getenv("ROCKSDB_BIT_BITSIZE")
        if not bitsize:
            raise ValueError("ROCKSDB_BIT_BITSIZE environment variable not set")
        return RocksDBBitCacheHandler(
            db_path=db_path,
            bitsize=int(bitsize),
        )
    elif cache_type == "rocksdict":
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler
        
        db_path = os.getenv("ROCKSDB_DICT_PATH")
        if not db_path:
            raise ValueError("ROCKSDB_DICT_PATH environment variable not set")
        return RocksDictCacheHandler(db_path=db_path)
    elif cache_type == "shelf":
        from partitioncache.cache_handler.shelf import ShelfCacheHandler
        
        db_path = os.getenv("SHELF_PATH")
        if not db_path:
            raise ValueError("SHELF_PATH environment variable not set")
        read_only = os.getenv("SHELF_READ_ONLY", "false").lower() == "true"
        return ShelfCacheHandler(
            db_path=db_path,
            read_only=read_only
        )
    else:
        raise ValueError(f"Unsupported cache type: {cache_type}")

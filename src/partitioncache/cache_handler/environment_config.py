"""
Environment configuration manager for cache handlers.

This module provides centralized environment variable validation and extraction
to eliminate duplication across cache handler implementations.
"""

import os
from typing import Any


class EnvironmentConfigManager:
    """Centralized environment variable management for cache handlers."""

    @staticmethod
    def get_postgresql_config() -> dict[str, str | int]:
        """
        Get PostgreSQL connection configuration from environment variables.

        Returns:
            Dictionary with PostgreSQL connection parameters

        Raises:
            ValueError: If required environment variables are missing
        """
        config: dict[str, str | int] = {}

        # Required variables
        required_vars = ["DB_NAME", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_PORT"]
        for var in required_vars:
            value = os.getenv(var)
            if not value:
                raise ValueError(f"{var} environment variable not set")
            config[var.lower()] = value

        # Convert port to int
        config["db_port"] = int(config["db_port"])

        return config

    @staticmethod
    def get_postgresql_array_config() -> dict[str, Any]:
        """
        Get PostgreSQL array cache handler configuration.

        Returns:
            Dictionary with configuration parameters

        Raises:
            ValueError: If required environment variables are missing
        """
        config = EnvironmentConfigManager.get_postgresql_config()

        table_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX")
        if not table_prefix:
            raise ValueError("PG_ARRAY_CACHE_TABLE_PREFIX environment variable not set")

        config["db_tableprefix"] = table_prefix
        return config

    @staticmethod
    def get_postgresql_bit_config() -> dict[str, str | int]:
        """
        Get PostgreSQL bit cache handler configuration.

        Returns:
            Dictionary with configuration parameters

        Raises:
            ValueError: If required environment variables are missing
        """
        config = EnvironmentConfigManager.get_postgresql_config()

        table_prefix = os.getenv("PG_BIT_CACHE_TABLE_PREFIX")
        if not table_prefix:
            raise ValueError("PG_BIT_CACHE_TABLE_PREFIX environment variable not set")

        bitsize = os.getenv("PG_BIT_CACHE_BITSIZE")
        if not bitsize:
            raise ValueError("PG_BIT_CACHE_BITSIZE environment variable not set")

        config["db_tableprefix"] = table_prefix
        config["bitsize"] = int(bitsize)
        return config

    @staticmethod
    def get_postgresql_roaringbit_config() -> dict[str, Any]:
        """
        Get PostgreSQL roaring bit cache handler configuration.

        Returns:
            Dictionary with configuration parameters

        Raises:
            ValueError: If required environment variables are missing
        """
        config = EnvironmentConfigManager.get_postgresql_config()

        table_prefix = os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX")
        if not table_prefix:
            raise ValueError("PG_ROARINGBIT_CACHE_TABLE_PREFIX environment variable not set")

        config["db_tableprefix"] = table_prefix
        return config

    @staticmethod
    def get_redis_config(cache_type: str) -> dict[str, Any]:
        """
        Get Redis connection configuration from environment variables.

        Args:
            cache_type: Type of Redis cache ("set" or "bit")

        Returns:
            Dictionary with Redis connection parameters

        Raises:
            ValueError: If required environment variables are missing
        """
        config = {}

        if cache_type == "set":
            # Support both REDIS_SET_DB (preferred) and REDIS_CACHE_DB (legacy)
            db_name = os.getenv("REDIS_SET_DB") or os.getenv("REDIS_CACHE_DB")
            if not db_name:
                raise ValueError("REDIS_SET_DB or REDIS_CACHE_DB environment variable not set")

            # Support redis_set specific variables with fallback to generic Redis variables
            host = os.getenv("REDIS_SET_HOST") or os.getenv("REDIS_HOST")
            if not host:
                raise ValueError("REDIS_SET_HOST or REDIS_HOST environment variable not set")

            password = os.getenv("REDIS_SET_PASSWORD") or os.getenv("REDIS_PASSWORD", "")

            port = os.getenv("REDIS_SET_PORT") or os.getenv("REDIS_PORT")
            if not port:
                raise ValueError("REDIS_SET_PORT or REDIS_PORT environment variable not set")

            config = {
                "db_name": db_name,
                "db_host": host,
                "db_password": password,
                "db_port": port,
            }

        elif cache_type == "bit":
            db_name = os.getenv("REDIS_BIT_DB")
            if not db_name:
                raise ValueError("REDIS_BIT_DB environment variable not set")

            # Support redis_bit specific variables with fallback to generic Redis variables
            host = os.getenv("REDIS_BIT_HOST") or os.getenv("REDIS_HOST")
            if not host:
                raise ValueError("REDIS_BIT_HOST or REDIS_HOST environment variable not set")

            password = os.getenv("REDIS_BIT_PASSWORD") or os.getenv("REDIS_PASSWORD", "")

            port = os.getenv("REDIS_BIT_PORT") or os.getenv("REDIS_PORT")
            if not port:
                raise ValueError("REDIS_BIT_PORT or REDIS_PORT environment variable not set")

            bitsize = os.getenv("REDIS_BIT_BITSIZE")
            if not bitsize:
                raise ValueError("REDIS_BIT_BITSIZE environment variable not set")

            config = {
                "db_name": db_name,
                "db_host": host,
                "db_password": password,
                "db_port": port,
                "bitsize": int(bitsize),
            }
        else:
            raise ValueError(f"Unsupported Redis cache type: {cache_type}")

        return config

    @staticmethod
    def get_duckdb_bit_config() -> dict[str, Any]:
        """
        Get DuckDB bit cache handler configuration.

        Returns:
            Dictionary with configuration parameters

        Environment Variables:
            DUCKDB_BIT_PATH: Path to DuckDB database file (default: ":memory:")
            DUCKDB_BIT_TABLE_PREFIX: Table prefix for cache tables (default: "partitioncache")
            DUCKDB_BIT_BITSIZE: Default bitsize for DuckDB BITSTRING (default: "100000")

        Note:
            This handler uses DuckDB's native BITSTRING data type with native
            bitwise operations and BIT_AND aggregates for optimal performance.
        """
        config = {}

        # Database path (optional, defaults to in-memory)
        db_path = os.getenv("DUCKDB_BIT_PATH", ":memory:")
        config["database"] = db_path

        # Table prefix (optional)
        table_prefix = os.getenv("DUCKDB_BIT_TABLE_PREFIX", "partitioncache")
        config["table_prefix"] = table_prefix

        # Bitsize (optional)
        bitsize = os.getenv("DUCKDB_BIT_BITSIZE", "100000")
        config["bitsize"] = int(bitsize)

        return config

    @staticmethod
    def get_rocksdb_config(cache_type: str) -> dict[str, Any]:
        """
        Get RocksDB configuration from environment variables.

        Args:
            cache_type: Type of RocksDB cache ("set", "bit", or "dict")

        Returns:
            Dictionary with RocksDB configuration parameters

        Raises:
            ValueError: If required environment variables are missing
        """
        config = {}

        if cache_type == "set":
            db_path = os.getenv("ROCKSDB_PATH")
            if not db_path:
                raise ValueError("ROCKSDB_PATH environment variable not set")
            config["db_path"] = db_path

        elif cache_type == "bit":
            db_path = os.getenv("ROCKSDB_BIT_PATH")
            if not db_path:
                raise ValueError("ROCKSDB_BIT_PATH environment variable not set")

            bitsize = os.getenv("ROCKSDB_BIT_BITSIZE")
            if not bitsize:
                raise ValueError("ROCKSDB_BIT_BITSIZE environment variable not set")

            config = {
                "db_path": db_path,
                "bitsize": int(bitsize),
            }

        elif cache_type == "dict":
            db_path = os.getenv("ROCKSDB_DICT_PATH")
            if not db_path:
                raise ValueError("ROCKSDB_DICT_PATH environment variable not set")
            config["db_path"] = db_path

        else:
            raise ValueError(f"Unsupported RocksDB cache type: {cache_type}")

        return config

    @staticmethod
    def validate_environment(cache_type: str) -> bool:
        """
        Validate that all required environment variables are set for a given cache type.

        Args:
            cache_type: Cache handler type to validate

        Returns:
            True if all required variables are set

        Raises:
            ValueError: If required environment variables are missing
        """
        try:
            if cache_type == "postgresql_array":
                EnvironmentConfigManager.get_postgresql_array_config()
            elif cache_type == "postgresql_bit":
                EnvironmentConfigManager.get_postgresql_bit_config()
            elif cache_type == "postgresql_roaringbit":
                EnvironmentConfigManager.get_postgresql_roaringbit_config()
            elif cache_type == "redis_set":
                EnvironmentConfigManager.get_redis_config("set")
            elif cache_type == "redis_bit":
                EnvironmentConfigManager.get_redis_config("bit")
            elif cache_type == "rocksdb_set":
                EnvironmentConfigManager.get_rocksdb_config("set")
            elif cache_type == "rocksdb_bit":
                EnvironmentConfigManager.get_rocksdb_config("bit")
            elif cache_type == "rocksdict":
                EnvironmentConfigManager.get_rocksdb_config("dict")
            elif cache_type == "duckdb_bit":
                EnvironmentConfigManager.get_duckdb_bit_config()
            else:
                raise ValueError(f"Unsupported cache type: {cache_type}")

            return True

        except ValueError:
            raise

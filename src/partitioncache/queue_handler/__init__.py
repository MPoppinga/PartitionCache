"""
Queue handler factory module.

This module provides a factory function to create queue handler instances
based on environment configuration.
"""

import os
from typing import Optional

from partitioncache.queue_handler.abstract import AbstractQueueHandler


def get_queue_handler(provider: Optional[str] = None) -> AbstractQueueHandler:
    """
    Factory function to create queue handler instances.

    Args:
        provider (Optional[str]): Queue provider type. If None, reads from QUERY_QUEUE_PROVIDER env var.
                                Defaults to 'postgresql' if not specified.

    Returns:
        AbstractQueueHandler: An instance of the appropriate queue handler.

    Raises:
        ValueError: If the provider is unsupported or required environment variables are missing.
    """
    if provider is None:
        provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")

    if provider == "postgresql":
        from partitioncache.queue_handler.postgresql import PostgreSQLQueueHandler

        # Required environment variables for PostgreSQL
        required_vars = ["PG_QUEUE_HOST", "PG_QUEUE_PORT", "PG_QUEUE_USER", "PG_QUEUE_PASSWORD", "PG_QUEUE_DB"]
        missing_vars = [var for var in required_vars if os.getenv(var) is None]

        if missing_vars:
            raise ValueError(f"Missing required PostgreSQL environment variables: {', '.join(missing_vars)}")

        # Safe to assert non-null since we validated above
        return PostgreSQLQueueHandler(
            host=str(os.getenv("PG_QUEUE_HOST")),
            port=int(os.getenv("PG_QUEUE_PORT", 5432)),
            user=str(os.getenv("PG_QUEUE_USER")),
            password=str(os.getenv("PG_QUEUE_PASSWORD")),
            dbname=str(os.getenv("PG_QUEUE_DB")),
        )

    elif provider == "redis":
        from partitioncache.queue_handler.redis import RedisQueueHandler

        # Required environment variables for Redis
        required_vars = ["REDIS_HOST", "REDIS_PORT", "QUERY_QUEUE_REDIS_DB"]
        missing_vars = [var for var in required_vars if os.getenv(var) is None]

        if missing_vars:
            raise ValueError(f"Missing required Redis environment variables: {', '.join(missing_vars)}")

        # Safe to assert non-null since we validated above
        return RedisQueueHandler(
            host=str(os.getenv("REDIS_HOST")),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("QUERY_QUEUE_REDIS_DB", 1)),
            password=os.getenv("REDIS_PASSWORD"),  # This can be None
            queue_key=str(os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY", "query_queue")),
        )

    else:
        supported_providers = ["postgresql", "redis"]
        raise ValueError(f"Unsupported queue provider: {provider}. Supported providers: {', '.join(supported_providers)}")


def validate_queue_configuration(provider: Optional[str] = None) -> bool:
    """
    Validate that the queue configuration is properly set up.

    Args:
        provider (Optional[str]): Queue provider type. If None, reads from QUERY_QUEUE_PROVIDER env var.

    Returns:
        bool: True if configuration is valid, False otherwise.
    """
    try:
        # Attempt to create a queue handler instance
        get_queue_handler(provider)
        return True
    except ValueError:
        return False

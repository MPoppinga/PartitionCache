"""
Helper functions to push queries to the queue to be cached asynchronously by an observer process.
"""

import os
from logging import getLogger

logger = getLogger("PartitionCache")


def push_to_queue(query: str):
    """
    Push a query to the queue.

    Requires the following environment variables to be set, based on the QUERY_QUEUE_PROVIDER:
    
    QUERY_QUEUE_PROVIDER: "redis":
    - REDIS_HOST
    - REDIS_PORT
    - QUERY_QUEUE_REDIS_DB
    - QUERY_QUEUE_REDIS_QUEUE_KEY

    Args:
        query (str): The query to be pushed to the queue.

    Returns:
        bool: True if the query was pushed to the queue, False otherwise.
        
    
    """

    if os.environ.get("QUERY_QUEUE_PROVIDER", None) == "redis":
        import redis

        if os.getenv("REDIS_HOST") is None:
            raise ValueError("REDIS_HOST not set")
        if os.getenv("REDIS_PORT") is None:
            raise ValueError("REDIS_PORT not set")
        if os.getenv("QUERY_QUEUE_REDIS_DB") is None:
            raise ValueError("QUERY_QUEUE_REDIS_DB not set")
        if os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY") is None:
            raise ValueError("QUERY_QUEUE_REDIS_QUEUE_KEY not set")
        
        if os.getenv("REDIS_PASSWORD") is None:         
            r = redis.Redis(
                host=os.getenv("REDIS_HOST", ""),
                port=int(os.getenv("REDIS_PORT", 0)),
                db=int(os.getenv("QUERY_QUEUE_REDIS_DB", 1)),
            )
        else:
            r = redis.Redis(
                host=os.getenv("REDIS_HOST", ""),
                port=int(os.getenv("REDIS_PORT", 0)),
                db=int(os.getenv("QUERY_QUEUE_REDIS_DB", 1)),
                password=os.getenv("REDIS_PASSWORD", ""),
            )

        r.rpush(os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY", "queue"), query)
        logger.debug("Pushed query to queue")
    else:
        logger.warning("No valid query queue provider specified, cannot populate partition cache")
        return False
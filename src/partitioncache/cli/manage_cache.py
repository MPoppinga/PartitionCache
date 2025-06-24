import argparse
import logging
import os
import pickle
import sys
from logging import getLogger
from typing import Any

from dotenv import load_dotenv
from tqdm import tqdm

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.abstract import AbstractCacheHandler
from partitioncache.queue import get_queue_lengths
from partitioncache.queue_handler import get_queue_handler

logger = getLogger("PartitionCache")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def get_cache_type_from_env() -> str:
    """
    Get cache type from environment variables.
    Returns the cache type from CACHE_BACKEND, defaulting to postgresql_array.
    """
    return os.getenv("CACHE_BACKEND", "postgresql_array")


def setup_queue_tables() -> None:
    """
    Set up queue tables by initializing a queue handler.
    """
    try:
        provider = os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")
        logger.info(f"Setting up queue tables for provider: {provider}")

        # Initialize queue handler - this creates all tables
        queue_handler = get_queue_handler(provider)

        logger.info("Queue tables setup completed successfully")

        # Close the handler to free resources
        queue_handler.close()

    except Exception as e:
        logger.error(f"Failed to setup queue tables: {e}")
        raise


def setup_cache_metadata_tables() -> None:
    """
    Set up cache metadata tables by initializing cache handlers.
    This ensures the metadata tables exist for the configured cache backends.
    """
    try:
        cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
        logger.info(f"Setting up cache metadata tables for backend: {cache_backend}")

        # Initialize cache handler - this creates metadata tables
        cache_handler = get_cache_handler(cache_backend)

        logger.info("Cache metadata tables setup completed successfully")

        # Close the handler to free resources
        cache_handler.close()

    except Exception as e:
        logger.error(f"Failed to setup cache metadata tables: {e}")
        raise


def setup_all_tables() -> None:
    """
    Set up both queue and cache metadata tables.
    """
    logger.info("Setting up PartitionCache tables...")

    try:
        # Setup queue tables first
        setup_queue_tables()

        # Setup cache metadata tables
        setup_cache_metadata_tables()

        logger.info("PartitionCache setup completed successfully!")
        logger.info("Your project is now ready to use PartitionCache.")

    except Exception as e:
        logger.error(f"Failed to setup PartitionCache tables: {e}")
        raise


def validate_environment() -> bool:
    """
    Validate that the environment is properly configured for PartitionCache.
    """
    issues = []

    # Check cache backend configuration
    cache_backend = os.getenv("CACHE_BACKEND")
    if not cache_backend:
        issues.append("CACHE_BACKEND environment variable not set")
    else:
        try:
            # Try to create a cache handler to validate configuration
            cache_handler = get_cache_handler(cache_backend)
            cache_handler.close()
            logger.info(f"✓ Cache backend '{cache_backend}' configuration is valid")
        except Exception as e:
            issues.append(f"Cache backend '{cache_backend}' configuration error: {e}")

    # Check queue provider configuration
    queue_provider = os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")
    try:
        # Try to create a queue handler to validate configuration
        queue_handler = get_queue_handler(queue_provider)
        queue_handler.close()
        logger.info(f"✓ Queue provider '{queue_provider}' configuration is valid")
    except Exception as e:
        issues.append(f"Queue provider '{queue_provider}' configuration error: {e}")

    if issues:
        logger.error("Environment validation failed:")
        for issue in issues:
            logger.error(f"  - {issue}")
        return False
    else:
        logger.info("✓ Environment validation passed")
        return True


def check_table_status() -> None:
    """
    Check the status of PartitionCache tables and provide setup guidance.
    """
    logger.info("Checking PartitionCache table status...")

    # Check queue tables
    try:
        queue_provider = os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")
        queue_handler = get_queue_handler(queue_provider)
        lengths = queue_handler.get_queue_lengths()
        queue_handler.close()

        logger.info(f"✓ Queue tables ({queue_provider}) are accessible")
        logger.info(f"  - Original query queue: {lengths.get('original_query_queue', 0)} items")
        logger.info(f"  - Query fragment queue: {lengths.get('query_fragment_queue', 0)} items")

    except Exception as e:
        logger.warning(f"⚠ Queue tables may need setup: {e}")
        logger.info("  Run: python -m partitioncache.cli.manage_cache setup queue")

    # Check cache metadata tables
    try:
        cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
        cache_handler = get_cache_handler(cache_backend)

        # Try to access metadata functionality
        try:
            partitions = cache_handler.get_partition_keys()  # type: ignore
            logger.info(f"✓ Cache metadata tables ({cache_backend}) are accessible")
            logger.info(f"  - Found {len(partitions)} partition keys")
        except AttributeError:
            logger.info(f"✓ Cache backend ({cache_backend}) is accessible")

        cache_handler.close()

    except Exception as e:
        logger.warning(f"⚠ Cache metadata tables may need setup: {e}")
        logger.info("  Run: python -m partitioncache.cli.manage_cache setup cache")


def copy_cache(from_cache_type: str, to_cache_type: str):
    added = 0
    skipped = 0
    prefixed_skipped = 0

    from_cache = get_cache_handler(from_cache_type)
    to_cache = get_cache_handler(to_cache_type)

    # Implement a method to get all keys from the 'from' cache
    all_keys = get_all_keys(from_cache)

    for key in tqdm(all_keys, desc="Copying cache", unit="key"):
        # Skip prefixed entries
        if key.startswith("_LIMIT_") or key.startswith("_TIMEOUT_"):
            prefixed_skipped += 1
            continue

        if not to_cache.exists(key):
            value = from_cache.get(key)
            if value is not None:
                to_cache.set_set(key, value)
                added += 1
        else:
            skipped += 1

    from_cache.close()
    to_cache.close()
    logger.info(f"Copy completed: {added} keys copied, {skipped} keys skipped, {prefixed_skipped} prefixed keys skipped")


def export_cache(cache_type: str, archive_file: str):
    cache = get_cache_handler(cache_type)
    all_keys = get_all_keys(cache)

    # skip prefixed entries
    all_keys = [key for key in all_keys if not (key.startswith("_LIMIT_") or key.startswith("_TIMEOUT_"))]

    with open(archive_file, "wb") as file:
        for key in tqdm(all_keys, desc="Exporting cache", unit="key"):
            value = cache.get(key)
            if value is not None:
                pickle.dump({key: value}, file, protocol=pickle.HIGHEST_PROTOCOL)

    logger.info(f"Export {len(all_keys)} keys to {archive_file}")
    cache.close()


def restore_cache(cache_type: str, archive_file: str):
    cache = get_cache_handler(cache_type)
    restored = 0
    skipped = 0

    with open(archive_file, "rb") as file:
        while True:
            try:
                data = pickle.load(file)
                key, value = list(data.items())[0]
                if not cache.exists(key):
                    cache.set_set(key, value)
                    restored += 1
                else:
                    skipped += 1
            except EOFError:
                break

    logger.info(f"Restore completed: {restored} keys restored, {skipped} keys skipped")
    cache.close()


def delete_cache(cache_type: str):
    cache = get_cache_handler(cache_type)
    all_keys = get_all_keys(cache)
    deleted = 0

    for key in tqdm(all_keys, desc="Deleting cache", unit="key"):
        cache.delete(key)
        deleted += 1

    logger.info(f"Deleted {deleted} keys from {cache_type} cache")
    cache.close()


def get_all_keys(cache: AbstractCacheHandler) -> list[str]:
    """Get all keys from all partitions in the cache."""
    all_keys = []

    try:
        # Get partition keys using the cache handler's method
        partitions = cache.get_partition_keys()

        if partitions:
            for partition_info in partitions:
                try:
                    # Handle different return types
                    if isinstance(partition_info, tuple):
                        partition_key = partition_info[0]
                    else:
                        partition_key = str(partition_info)

                    keys = cache.get_all_keys(partition_key)
                    all_keys.extend(keys)

                except Exception as e:
                    logger.debug(f"Error getting keys for partition {partition_info}: {e}")
                    continue
    except (AttributeError, TypeError) as e:
        logger.debug(f"Error getting partition keys: {e}")
        raise e

    return all_keys


def count_cache(cache_type: str) -> None:
    """Count cache entries with improved PostgreSQL support."""
    if cache_type.startswith("postgresql"):
        # Use PostgreSQL-specific counting for better accuracy
        partitions = get_partition_overview(cache_type)

        if not partitions:
            logger.info(f"No partitions found for cache type: {cache_type}")
            return

        total_entries = sum(p["total_entries"] for p in partitions)
        valid_entries = sum(p["valid_entries"] for p in partitions)
        limit_entries = sum(p["limit_entries"] for p in partitions)
        timeout_entries = sum(p["timeout_entries"] for p in partitions)

        logger.info(f"Cache statistics for {cache_type}:")
        logger.info(f"  Total keys: {total_entries}")
        logger.info(f"  Valid entries: {valid_entries}")
        logger.info(f"  Limit entries: {limit_entries}")
        logger.info(f"  Timeout entries: {timeout_entries}")
        logger.info(f"  Partitions: {len(partitions)}")

    else:
        # For other cache types
        cache = get_cache_handler(cache_type)
        all_keys = get_all_keys(cache)

        limit_count = 0
        timeout_count = 0
        valid_count = 0

        for key in all_keys:
            if key.find("_LIMIT_") == 0:
                limit_count += 1
            elif key.find("_TIMEOUT_") == 0:
                timeout_count += 1
            else:
                valid_count += 1

        logger.info(f"Cache statistics for {cache_type}:")
        logger.info(f"  Total keys: {len(all_keys)}")
        logger.info(f"  Valid entries: {valid_count}")
        logger.info(f"  Limit entries: {limit_count}")
        logger.info(f"  Timeout entries: {timeout_count}")

        cache.close()


def count_queue():
    """Count entries in both original query and query fragment queues."""
    provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")  # Default to PostgreSQL

    try:
        queue_lengths = get_queue_lengths()
        original_query_count = queue_lengths.get("original_query_queue", 0)
        query_fragment_count = queue_lengths.get("query_fragment_queue", 0)

        logger.info(f"Queue statistics (using {provider}):")
        logger.info(f"  Original query queue: {original_query_count} entries")
        logger.info(f"  Query fragment queue: {query_fragment_count} entries")
        logger.info(f"  Total queue entries: {original_query_count + query_fragment_count}")

    except Exception as e:
        logger.error(f"Error counting queues: {e}")


def clear_queue():
    """Clear both original query and query fragment queues."""
    provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")

    try:
        from partitioncache.queue import clear_all_queues

        original_query_cleared, query_fragment_cleared = clear_all_queues()

        logger.info(f"{provider.title()} queues cleared:")
        logger.info(f"  Original query queue: {original_query_cleared} entries cleared")
        logger.info(f"  Query fragment queue: {query_fragment_cleared} entries cleared")

    except Exception as e:
        logger.error(f"Error clearing {provider} queues: {e}")


def clear_original_query_queue():
    """Clear only the original query queue."""
    provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")

    try:
        from partitioncache.queue import clear_original_query_queue

        deleted_count = clear_original_query_queue()
        logger.info(f"Original query queue: {deleted_count} entries cleared")
    except Exception as e:
        logger.error(f"Error clearing {provider} original query queue: {e}")


def clear_query_fragment_queue():
    """Clear only the query fragment queue."""
    provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")

    try:
        from partitioncache.queue import clear_query_fragment_queue

        deleted_count = clear_query_fragment_queue()
        logger.info(f"Query fragment queue: {deleted_count} entries cleared")
    except Exception as e:
        logger.error(f"Error clearing {provider} query fragment queue: {e}")


def delete_partition(cache_type: str, partition_key: str):
    """Delete a specific partition and all its data."""
    try:
        cache = get_cache_handler(cache_type)

        if hasattr(cache, "delete_partition"):
            success = cache.delete_partition(partition_key)  # type: ignore
            if success:
                logger.info(f"Successfully deleted partition '{partition_key}' from {cache_type} cache")
            else:
                logger.error(f"Failed to delete partition '{partition_key}' from {cache_type} cache")
        else:
            logger.error(f"Cache handler {cache_type} does not support partition deletion")

        cache.close()
    except ValueError as e:
        logger.error(f"Cache configuration error for {cache_type}: {e}")
    except Exception as e:
        logger.error(f"Error deleting partition {partition_key} from {cache_type}: {e}")


def prune_old_queries(cache_type: str, days_old: int):
    """Remove queries older than specified days."""
    try:
        cache = get_cache_handler(cache_type)

        if hasattr(cache, "prune_old_queries"):
            removed_count = cache.prune_old_queries(days_old)  # type: ignore
            logger.info(f"Pruned {removed_count} old queries (older than {days_old} days) from {cache_type} cache")
        else:
            logger.warning(f"Cache handler {cache_type} does not support automatic query pruning")
            logger.info("Manual pruning: Use 'cache cleanup remove-termination' to remove timeout/limit entries")

        cache.close()
    except ValueError as e:
        logger.error(f"Cache configuration error for {cache_type}: {e}")
    except Exception as e:
        logger.error(f"Error pruning old queries from {cache_type}: {e}")


def prune_all_caches(days_old: int):
    """Remove old queries from all cache types."""
    cache_types = ["postgresql_array", "postgresql_bit", "redis_set", "redis_bit", "rocksdb_set", "rocksdb_bit", "rocksdict"]

    total_removed = 0
    for cache_type in cache_types:
        try:
            cache = get_cache_handler(cache_type)
            if hasattr(cache, "prune_old_queries"):
                removed_count = cache.prune_old_queries(days_old)  # type: ignore
                total_removed += removed_count
                logger.info(f"{cache_type}: Pruned {removed_count} old queries")
            cache.close()
        except ValueError:
            logger.debug(f"Cache type {cache_type} not configured, skipping")
        except Exception as e:
            logger.error(f"Error pruning {cache_type}: {e}")

    logger.info(f"Total queries pruned across all caches: {total_removed}")


def delete_all_caches():
    confirmation = input("Are you sure you want to delete all caches? This action cannot be undone. (yes/no): ")
    if confirmation.lower() == "yes":
        cache_types = [
            "postgresql_array",
            "postgresql_bit",
            "redis_set",
            "redis_bit",
            "rocksdb_set",
            "rocksdb_bit",
        ]
        for cache_type in cache_types:
            logger.info(f"Deleting cache: {cache_type}")
            try:
                delete_cache(cache_type)
            except ValueError as e:
                logger.error(f"Error deleting {cache_type}: Cache type not properly setup: {e}")
            except Exception as e:
                logger.error(f"Error deleting {cache_type}: {e}")
        logger.info("All caches have been deleted.")
    else:
        logger.info("Delete all caches action canceled.")


def count_all_caches():
    cache_types = [
        "postgresql_array",
        "postgresql_bit",
        "redis_set",
        "redis_bit",
        "rocksdb_set",
        "rocksdb_bit",
    ]
    total = 0
    for cache_type in cache_types:
        try:
            cache = get_cache_handler(cache_type)
            count = len(get_all_keys(cache))
            logger.info(f"{cache_type}: {count} keys")
            total += count
            cache.close()
        except ValueError as e:
            logger.error(f"Error counting {cache_type}: Cache type not properly setup: {e}")
        except Exception as e:
            logger.error(f"Error counting {cache_type}: {e}")
    logger.info(f"Total keys across all caches: {total}")


def remove_termination_entries(cache_type: str):
    cache = get_cache_handler(cache_type)
    all_keys = get_all_keys(cache)
    removed = 0

    for key in tqdm(all_keys, desc="Removing termination entries", unit="key"):
        if key.find("_LIMIT_") == 0 or key.find("_TIMEOUT_") == 0:
            cache.delete(key)
            removed += 1

    logger.info(f"Removed {removed} termination entries from {cache_type} cache")
    cache.close()


def remove_large_entries(cache_type: str, max_entries: int):
    cache = get_cache_handler(cache_type)
    all_keys = get_all_keys(cache)
    removed = 0
    total = 0

    for key in tqdm(all_keys, desc="Removing large entries", unit="key"):
        total += 1
        value = cache.get(key)
        if isinstance(value, set) and len(value) > max_entries:
            cache.delete(key)
            removed += 1

    logger.info(f"Removed {removed} entries with more than {max_entries} items from {cache_type} cache")
    logger.info(f"Total entries processed: {total}")
    cache.close()


def evict_cache_manual(cache_type: str, strategy: str, threshold: int):
    """
    Manually evict cache entries based on a given strategy and threshold.
    This function contains Python-based logic and does not depend on the
    pg_cron SQL functions.
    """
    logger.info(f"Starting manual eviction for cache '{cache_type}' with strategy '{strategy}' and threshold {threshold}")
    cache = get_cache_handler(cache_type)

    if not hasattr(cache, "get_partition_keys"):
        logger.error(f"Cache type {cache_type} does not support partitions, eviction not supported.")
        return

    partition_keys = cache.get_partition_keys()
    total_removed = 0

    for partition_info in tqdm(partition_keys, desc="Processing partitions"):
        partition_key = partition_info[0] if isinstance(partition_info, tuple) else str(partition_info)

        try:
            all_cache_keys = cache.get_all_keys(partition_key)
            current_count = len(all_cache_keys)

            if current_count <= threshold:
                continue

            to_remove_count = current_count - threshold

            if cache_type.find("postgresql") == 0:
                import psycopg
                from psycopg import sql

                conn = psycopg.connect(
                    host=os.getenv("DB_HOST"),
                    port=int(os.getenv("DB_PORT", "5432")),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    dbname=os.getenv("DB_NAME"),
                )

                if cache_type == "postgresql_array":
                    table_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "partitioncache")
                elif cache_type == "postgresql_bit":
                    table_prefix = os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "partitioncache")
                else:  # roaringbit
                    table_prefix = os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "partitioncache")

                queries_table = f"{table_prefix}_queries"
                cache_table = f"{table_prefix}_cache_{partition_key}"

                removed_hashes = []
                with conn.cursor() as cur:
                    if strategy == "oldest":
                        cur.execute(
                            sql.SQL("""
                            SELECT query_hash FROM {}
                            WHERE partition_key = %s AND status = 'ok'
                            ORDER BY last_seen ASC
                            LIMIT %s
                        """).format(sql.Identifier(queries_table)),
                            (partition_key, to_remove_count),
                        )
                        removed_hashes = [row[0] for row in cur.fetchall()]

                    elif strategy == "largest":
                        # This assumes the cache table has a _count column, which is standard for postgres backends
                        cur.execute(
                            sql.SQL("""
                            SELECT t1.query_hash
                            FROM {} t1 JOIN {} t2 ON t1.query_hash = t2.query_hash AND t2.partition_key = %s
                            WHERE t2.status = 'ok'
                            ORDER BY t1.partition_keys_count DESC
                            LIMIT %s
                        """).format(sql.Identifier(cache_table), sql.Identifier(queries_table)),
                            (partition_key, to_remove_count),
                        )
                        removed_hashes = [row[0] for row in cur.fetchall()]

                    if removed_hashes:
                        cur.execute(sql.SQL("DELETE FROM {} WHERE query_hash = ANY(%s)").format(sql.Identifier(cache_table)), (removed_hashes,))
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE query_hash = ANY(%s) AND partition_key = %s").format(sql.Identifier(queries_table)),
                            (removed_hashes, partition_key),
                        )
                        total_removed += len(removed_hashes)
                conn.commit()
                conn.close()

            else:  # For non-postgresql caches
                if strategy == "oldest":
                    logger.warning(f"Strategy 'oldest' is not supported for non-PostgreSQL cache '{cache_type}'. Skipping partition '{partition_key}'.")
                    continue

                elif strategy == "largest":
                    # This can be slow as it fetches all values for a partition
                    key_sizes = []
                    for key in all_cache_keys:
                        if key.find("_") == 0:
                            continue  # Skip internal keys
                        value = cache.get(key)
                        if value and hasattr(value, "__len__"):
                            key_sizes.append((key, len(value)))

                    key_sizes.sort(key=lambda x: x[1], reverse=True)

                    for i in range(min(to_remove_count, len(key_sizes))):
                        key_to_delete = key_sizes[i][0]
                        cache.delete(key_to_delete)
                        total_removed += 1

        except Exception as e:
            logger.error(f"Failed to evict from partition {partition_key}: {e}")

    logger.info(f"Manual eviction completed. Total entries removed: {total_removed}")
    cache.close()


def get_partition_overview(cache_type: str) -> list[dict[str, Any]]:
    """
    Get comprehensive overview of all partitions and their statistics.
    """
    cache = get_cache_handler(cache_type)
    partitions = []

    try:
        # Get partition metadata
        partition_data = cache.get_partition_keys()

        for partition_info in partition_data:
            if isinstance(partition_info, tuple) and len(partition_info) >= 2:
                partition_key, datatype = partition_info[0], partition_info[1]
            else:
                # Handle case where partition_info is just a string
                partition_key = str(partition_info)
                datatype = "unknown"

            # Get cache statistics for this partition
            try:
                cache_keys = cache.get_all_keys(partition_key)
                total_entries = len(cache_keys)

                # Count different types of entries
                limit_count = sum(1 for k in cache_keys if k.find("_LIMIT_") == 0)
                timeout_count = sum(1 for k in cache_keys if k.find("_TIMEOUT_") == 0)
                valid_count = total_entries - limit_count - timeout_count

            except Exception as e:
                logger.debug(f"Error getting keys for partition {partition_key}: {e}")
                total_entries = 0
                valid_count = 0
                limit_count = 0
                timeout_count = 0

            partitions.append(
                {
                    "partition_key": partition_key,
                    "datatype": datatype,
                    "total_entries": total_entries,
                    "valid_entries": valid_count,
                    "limit_entries": limit_count,
                    "timeout_entries": timeout_count,
                }
            )

        # For PostgreSQL backends, also try to get direct table counts
        if cache_type.find("postgresql") == 0:
            partitions = _get_postgresql_partition_overview(cache_type, partitions)

    except Exception as e:
        logger.debug(f"Error getting partition overview: {e}")
        # Fallback: try to find partitions by examining available cache tables
        if cache_type.find("postgresql") == 0:
            partitions = _get_postgresql_partition_overview(cache_type, [])

    finally:
        cache.close()

    return partitions


def _get_postgresql_partition_overview(cache_type: str, existing_partitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Get partition overview specifically for PostgreSQL backends by examining actual tables.
    """
    import psycopg
    from psycopg import sql

    try:
        # Get database connection parameters from environment
        conn = psycopg.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME"),
        )

        # Determine table prefix
        if cache_type == "postgresql_array":
            table_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "partitioncache")
        else:
            table_prefix = os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "partitioncache")

        with conn.cursor() as cur:
            # Find all cache tables
            cur.execute(
                """
                SELECT tablename FROM pg_tables
                WHERE tablename LIKE %s AND tablename LIKE %s
                ORDER BY tablename
            """,
                [f"{table_prefix}_%", r"%cache_%"],
            )

            cache_tables = cur.fetchall()
            partitions = []

            for (table_name,) in cache_tables:
                # Extract partition key from table name
                # Format: prefix_cache_partitionkey
                cache_prefix = f"{table_prefix}_cache_"
                if table_name.find(cache_prefix) == 0:
                    partition_key = table_name[len(cache_prefix) :]

                    # Get table row count
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
                    result = cur.fetchone()
                    total_entries = result[0] if result else 0

                    # Get datatype from metadata if available
                    metadata_table = f"{table_prefix}_partition_metadata"
                    try:
                        cur.execute(sql.SQL("SELECT datatype FROM {} WHERE partition_key = %s").format(sql.Identifier(metadata_table)), [partition_key])
                        result = cur.fetchone()
                        datatype = result[0] if result else "unknown"
                    except Exception:
                        datatype = "unknown"

                    # For detailed entry analysis, we'd need to query the actual table
                    # For now, assume all entries are valid (no _LIMIT_ or _TIMEOUT_ in PostgreSQL cache)
                    partitions.append(
                        {
                            "partition_key": partition_key,
                            "datatype": datatype,
                            "total_entries": total_entries,
                            "valid_entries": total_entries,
                            "limit_entries": 0,
                            "timeout_entries": 0,
                            "table_name": table_name,
                        }
                    )

            # If we found tables but no existing partitions, use our findings
            if partitions and not existing_partitions:
                return partitions

            # Otherwise, enhance existing partitions with table data
            for partition in existing_partitions:
                for pg_partition in partitions:
                    if partition["partition_key"] == pg_partition["partition_key"]:
                        partition.update(pg_partition)
                        break

        conn.close()
        return existing_partitions if existing_partitions else partitions

    except Exception as e:
        logger.debug(f"Error getting PostgreSQL partition overview: {e}")
        return existing_partitions


def show_partition_overview(cache_type: str) -> None:
    """
    Display comprehensive partition overview.
    """
    partitions = get_partition_overview(cache_type)

    if not partitions:
        logger.info(f"No partitions found for cache type: {cache_type}")
        return

    logger.info(f"\n=== Partition Overview for {cache_type} ===")
    logger.info(f"{'Partition Key':<20} {'Data Type':<12} {'Total':<8} {'Valid':<8} {'Limit':<8} {'Timeout':<8}")
    logger.info("-" * 70)

    total_entries = 0
    total_valid = 0
    total_limit = 0
    total_timeout = 0

    for partition in partitions:
        partition_key = partition["partition_key"]
        datatype = partition["datatype"]
        total = partition["total_entries"]
        valid = partition["valid_entries"]
        limit = partition["limit_entries"]
        timeout = partition["timeout_entries"]

        logger.info(f"{partition_key:<20} {datatype:<12} {total:<8} {valid:<8} {limit:<8} {timeout:<8}")

        total_entries += total
        total_valid += valid
        total_limit += limit
        total_timeout += timeout

    logger.info("-" * 70)
    logger.info(f"{'TOTAL':<20} {'':<12} {total_entries:<8} {total_valid:<8} {total_limit:<8} {total_timeout:<8}")

    # Additional information
    if "table_name" in partitions[0]:
        logger.info("\nTable Details:")
        for partition in partitions:
            logger.info(f"  {partition['partition_key']}: {partition.get('table_name', 'N/A')}")


def main():
    # Main parser
    parser = argparse.ArgumentParser(
        description="PartitionCache Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Setup new project
  pcache-manage setup all

  # Check environment and tables
  pcache-manage status

  # Cache operations (uses CACHE_BACKEND from environment)
  pcache-manage cache count
  pcache-manage cache export --file backup.pkl
  pcache-manage cache copy --from redis_set --to postgresql_array

  # Queue operations
  pcache-manage queue count
  pcache-manage queue clear

  # Maintenance (uses CACHE_BACKEND from environment)
  pcache-manage maintenance prune --days 30
  pcache-manage maintenance cleanup --remove-termination

Note: Cache type parameters are optional for most commands and will use
the CACHE_BACKEND environment variable when not specified. Use --env
to load configuration from a custom environment file.
        """,
    )

    parser.add_argument(
        "--env",
        dest="env_file",
        default=".env",
        help="Environment file to use (default: .env)",
    )

    # Create subparsers
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Setup commands
    setup_parser = subparsers.add_parser("setup", help="Setup PartitionCache tables and configuration")
    setup_subparsers = setup_parser.add_subparsers(dest="setup_command", help="Setup operations")

    setup_subparsers.add_parser("all", help="Set up all tables (recommended for new projects)")
    setup_subparsers.add_parser("queue", help="Set up queue tables only")
    setup_subparsers.add_parser("cache", help="Set up cache metadata tables only")

    # Status commands
    status_parser = subparsers.add_parser("status", help="Check PartitionCache status and configuration")
    status_subparsers = status_parser.add_subparsers(dest="status_command", help="Status operations")

    status_subparsers.add_parser("env", help="Validate environment configuration")
    status_subparsers.add_parser("tables", help="Check table status and accessibility")

    # Cache commands
    cache_parser = subparsers.add_parser("cache", help="Cache operations")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", help="Cache operations")

    # Cache count
    cache_count_parser = cache_subparsers.add_parser("count", help="Count cache entries")
    cache_count_parser.add_argument("--type", dest="cache_type", help="Cache type (default: from CACHE_BACKEND env var)")
    cache_count_parser.add_argument("--all", action="store_true", help="Count all cache types")

    # Cache overview
    cache_overview_parser = cache_subparsers.add_parser("overview", help="Show detailed partition overview")
    cache_overview_parser.add_argument("--type", dest="cache_type", help="Cache type (default: from CACHE_BACKEND env var)")

    # Cache copy
    cache_copy_parser = cache_subparsers.add_parser("copy", help="Copy cache from one type to another")
    cache_copy_parser.add_argument("--from", dest="from_cache_type", required=True, help="Source cache type")
    cache_copy_parser.add_argument("--to", dest="to_cache_type", required=True, help="Destination cache type")

    # Cache export/import
    cache_export_parser = cache_subparsers.add_parser("export", help="Export cache to file")
    cache_export_parser.add_argument("--type", dest="cache_type", help="Cache type to export (default: from CACHE_BACKEND env var)")
    cache_export_parser.add_argument("--file", dest="archive_file", required=True, help="Archive file path")

    cache_import_parser = cache_subparsers.add_parser("import", help="Import cache from file")
    cache_import_parser.add_argument("--type", dest="cache_type", help="Cache type to import to (default: from CACHE_BACKEND env var)")
    cache_import_parser.add_argument("--file", dest="archive_file", required=True, help="Archive file path")

    # Cache delete
    cache_delete_parser = cache_subparsers.add_parser("delete", help="Delete cache")
    cache_delete_parser.add_argument("--type", dest="cache_type", help="Cache type to delete (default: from CACHE_BACKEND env var)")
    cache_delete_parser.add_argument("--all", action="store_true", help="Delete all cache types (with confirmation)")

    # Queue commands
    queue_parser = subparsers.add_parser("queue", help="Queue operations")
    queue_subparsers = queue_parser.add_subparsers(dest="queue_command", help="Queue operations")
    queue_subparsers.add_parser("count", help="Count queue entries")

    queue_clear_parser = queue_subparsers.add_parser("clear", help="Clear queue entries")
    queue_clear_parser.add_argument("--original", action="store_true", help="Clear only original query queue")
    queue_clear_parser.add_argument("--fragment", action="store_true", help="Clear only fragment query queue")

    # Maintenance commands
    maintenance_parser = subparsers.add_parser("maintenance", help="Maintenance operations")
    maintenance_subparsers = maintenance_parser.add_subparsers(dest="maintenance_command", help="Maintenance operations")

    # Prune commands
    prune_parser = maintenance_subparsers.add_parser("prune", help="Prune old queries")
    prune_parser.add_argument("--days", type=int, default=30, help="Remove queries older than this many days")
    prune_parser.add_argument("--type", dest="cache_type", help="Specific cache type to prune (default: all)")

    # Evict command
    evict_parser = maintenance_subparsers.add_parser("evict", help="Evict cache entries based on a strategy")
    evict_parser.add_argument("--type", dest="cache_type", help="Cache type to evict from (default: from CACHE_BACKEND env var)")
    evict_parser.add_argument("--strategy", choices=["oldest", "largest"], required=True, help="Eviction strategy")
    evict_parser.add_argument("--threshold", type=int, required=True, help="Cache size threshold to trigger eviction")

    # Cleanup commands
    cleanup_parser = maintenance_subparsers.add_parser("cleanup", help="Clean up cache entries")
    cleanup_parser.add_argument("--type", dest="cache_type", help="Cache type to clean (default: from CACHE_BACKEND env var)")
    cleanup_parser.add_argument("--remove-termination", action="store_true", help="Remove termination entries (_LIMIT_, _TIMEOUT_)")
    cleanup_parser.add_argument("--remove-large", type=int, metavar="MAX_ENTRIES", help="Remove entries with more than MAX_ENTRIES items")

    # Partition commands
    partition_parser = maintenance_subparsers.add_parser("partition", help="Partition management")
    partition_parser.add_argument("--delete", dest="partition_key", help="Delete specific partition")
    partition_parser.add_argument("--type", dest="cache_type", help="Cache type (default: from CACHE_BACKEND env var)")

    args = parser.parse_args()

    # Load environment variables
    load_dotenv(args.env_file)

    # Handle commands
    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == "setup":
            if not args.setup_command:
                setup_parser.print_help()
                return

            if args.setup_command == "all":
                setup_all_tables()
            elif args.setup_command == "queue":
                setup_queue_tables()
            elif args.setup_command == "cache":
                setup_cache_metadata_tables()

        elif args.command == "status":
            if not args.status_command:
                # Default: check both environment and tables
                validate_environment()
                check_table_status()
                return

            if args.status_command == "env":
                validate_environment()
            elif args.status_command == "tables":
                check_table_status()

        elif args.command == "cache":
            if not args.cache_command:
                cache_parser.print_help()
                return

            if args.cache_command == "count":
                if args.all:
                    count_all_caches()
                else:
                    cache_type = args.cache_type or get_cache_type_from_env()
                    count_cache(cache_type)
            elif args.cache_command == "overview":
                cache_type = args.cache_type or get_cache_type_from_env()
                show_partition_overview(cache_type)
            elif args.cache_command == "copy":
                copy_cache(args.from_cache_type, args.to_cache_type)
            elif args.cache_command == "export":
                cache_type = args.cache_type or get_cache_type_from_env()
                export_cache(cache_type, args.archive_file)
            elif args.cache_command == "import":
                cache_type = args.cache_type or get_cache_type_from_env()
                restore_cache(cache_type, args.archive_file)
            elif args.cache_command == "delete":
                if args.all:
                    delete_all_caches()
                else:
                    cache_type = args.cache_type or get_cache_type_from_env()
                    delete_cache(cache_type)

        elif args.command == "queue":
            if not args.queue_command:
                queue_parser.print_help()
                return

            if args.queue_command == "count":
                count_queue()
            elif args.queue_command == "clear":
                if args.original:
                    clear_original_query_queue()
                elif args.fragment:
                    clear_query_fragment_queue()
                else:
                    clear_queue()

        elif args.command == "maintenance":
            if not args.maintenance_command:
                maintenance_parser.print_help()
                return

            if args.maintenance_command == "prune":
                if args.cache_type:
                    prune_old_queries(args.cache_type, args.days)
                else:
                    prune_all_caches(args.days)
            elif args.maintenance_command == "cleanup":
                cache_type = args.cache_type or get_cache_type_from_env()
                if args.remove_termination:
                    remove_termination_entries(cache_type)
                elif args.remove_large:
                    remove_large_entries(cache_type, args.remove_large)
                else:
                    cleanup_parser.print_help()
            elif args.maintenance_command == "evict":
                cache_type = args.cache_type or get_cache_type_from_env()
                evict_cache_manual(cache_type, args.strategy, args.threshold)
            elif args.maintenance_command == "partition":
                cache_type = args.cache_type or get_cache_type_from_env()
                if args.partition_key:
                    delete_partition(cache_type, args.partition_key)
                else:
                    partition_parser.print_help()

    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)

    # Cleanup for RocksDB - Handle for all rocksdb cache types
    if hasattr(args, "cache_type") and args.cache_type and ("rocksdb" in args.cache_type):
        try:
            cache = get_cache_handler(args.cache_type)
            if hasattr(cache, "compact"):
                cache.compact()  # type: ignore
            cache.close()
        except Exception as e:
            logger.error(f"Error compacting {args.cache_type}: {e}")


if __name__ == "__main__":
    main()

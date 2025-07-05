import argparse
import os
import pickle
import sys
from logging import getLogger
from typing import Any

from tqdm import tqdm

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.abstract import AbstractCacheHandler
from partitioncache.cache_handler.redis_abstract import RedisAbstractCacheHandler
from partitioncache.cache_handler.redis_set import RedisCacheHandler
from partitioncache.cli.common_args import add_environment_args, add_verbosity_args, configure_logging, load_environment_with_validation
from partitioncache.queue import get_queue_lengths
from partitioncache.queue_handler import get_queue_handler

logger = getLogger("PartitionCache")


def get_cache_type_from_env() -> str:
    """
    Get cache type from environment variables.
    Returns the cache type from CACHE_BACKEND, defaulting to postgresql_array.
    """
    return os.getenv("CACHE_BACKEND", "postgresql_array")


def detect_configured_cache_backends() -> list[str]:
    """
    Detect all cache backends that have valid environment configurations.

    Returns:
        List of cache backend names that are properly configured
    """
    from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

    configured_backends = []

    # Test each backend type
    backend_configs = {
        "postgresql_array": lambda: EnvironmentConfigManager.get_postgresql_array_config(),
        "postgresql_bit": lambda: EnvironmentConfigManager.get_postgresql_bit_config(),
        "postgresql_roaringbit": lambda: EnvironmentConfigManager.get_postgresql_roaringbit_config(),
        "redis_set": lambda: EnvironmentConfigManager.get_redis_config("set"),
        "redis_bit": lambda: EnvironmentConfigManager.get_redis_config("bit"),
        "rocksdb_set": lambda: EnvironmentConfigManager.get_rocksdb_config("set"),
        "rocksdb_bit": lambda: EnvironmentConfigManager.get_rocksdb_config("bit"),
        "rocksdict": lambda: EnvironmentConfigManager.get_rocksdb_config("dict"),
    }

    for backend_name, config_func in backend_configs.items():
        try:
            config_func()  # Try to get configuration
            configured_backends.append(backend_name)
        except ValueError:
            # Configuration is missing/invalid for this backend
            pass
        except Exception:
            # Other errors (e.g., import issues) - skip this backend
            pass

    return configured_backends


def detect_configured_queue_providers() -> list[str]:
    """
    Detect all queue providers that have valid environment configurations.

    Returns:
        List of queue provider names that are properly configured
    """
    from partitioncache.queue_handler import get_queue_handler

    configured_providers = []

    # Test each provider type
    providers = ["postgresql", "redis"]

    for provider in providers:
        try:
            # Try to create queue handler to test configuration
            handler = get_queue_handler(provider)
            handler.close()
            configured_providers.append(provider)
        except ValueError:
            # Configuration is missing/invalid for this provider
            pass
        except Exception:
            # Other errors - skip this provider
            pass

    return configured_providers


def setup_queue_tables(queue_provider: str | None = None) -> None:
    """
    Set up queue tables by initializing a queue handler.

    Args:
        queue_provider: Queue provider to use (defaults to QUERY_QUEUE_PROVIDER env var)
    """
    try:
        provider = queue_provider or os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")
        logger.info(f"Setting up queue tables for provider: {provider}")

        # Initialize queue handler - this creates all tables
        queue_handler = get_queue_handler(provider)

        logger.info("Queue tables setup completed successfully")

        # Close the handler to free resources
        queue_handler.close()

    except Exception as e:
        logger.error(f"Failed to setup queue tables: {e}")
        raise


def setup_cache_metadata_tables(cache_backend: str | None = None) -> None:
    """
    Set up cache metadata tables by initializing cache handlers.
    This ensures the metadata tables exist for the configured cache backends.

    Args:
        cache_backend: Cache backend to use (defaults to CACHE_BACKEND env var)
    """
    try:
        backend = cache_backend or os.getenv("CACHE_BACKEND", "postgresql_array")
        logger.info(f"Setting up cache metadata tables for backend: {backend}")

        # Initialize cache handler - this creates metadata tables
        cache_handler = get_cache_handler(backend)

        logger.info("Cache metadata tables setup completed successfully")

        # Close the handler to free resources
        cache_handler.close()

    except Exception as e:
        logger.error(f"Failed to setup cache metadata tables: {e}")
        raise


def setup_all_tables(cache_backend: str | None = None, queue_provider: str | None = None) -> None:
    """
    Set up both queue and cache metadata tables.

    Args:
        cache_backend: Cache backend to use (defaults to CACHE_BACKEND env var)
        queue_provider: Queue provider to use (defaults to QUERY_QUEUE_PROVIDER env var)
    """
    logger.info("Setting up PartitionCache tables...")

    try:
        # Setup queue tables first
        setup_queue_tables(queue_provider)

        # Setup cache metadata tables
        setup_cache_metadata_tables(cache_backend)

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
            logger.debug(f"✓ Cache backend '{cache_backend}' configuration is valid")
        except Exception as e:
            issues.append(f"Cache backend '{cache_backend}' configuration error: {e}")

    # Check queue provider configuration
    queue_provider = os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")
    try:
        # Try to create a queue handler to validate configuration
        queue_handler = get_queue_handler(queue_provider)
        queue_handler.close()
        logger.debug(f"✓ Queue provider '{queue_provider}' configuration is valid")
    except Exception as e:
        issues.append(f"Queue provider '{queue_provider}' configuration error: {e}")

    if issues:
        logger.error("Environment validation failed:")
        for issue in issues:
            logger.error(f"  - {issue}")
        return False
    else:
        logger.debug("✓ Environment validation passed")
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

        logger.debug(f"✓ Queue tables ({queue_provider}) are accessible")
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
            logger.debug(f"✓ Cache metadata tables ({cache_backend}) are accessible")
            logger.info(f"  - Found {len(partitions)} partition keys")
        except AttributeError:
            logger.debug(f"✓ Cache backend ({cache_backend}) is accessible")

        cache_handler.close()

    except Exception as e:
        logger.warning(f"⚠ Cache metadata tables may need setup: {e}")
        logger.info("  Run: python -m partitioncache.cli.manage_cache setup cache")


def copy_cache(from_cache_type: str, to_cache_type: str, partition_key: str | None = None):
    added = 0
    skipped = 0
    prefixed_skipped = 0
    partitions_registered = 0

    from_cache = get_cache_handler(from_cache_type)
    to_cache = get_cache_handler(to_cache_type)

    try:
        # Get all partitions from source
        all_partitions = from_cache.get_partition_keys()

        # Filter partitions if specific partition key requested
        if partition_key:
            from_partitions = [p for p in all_partitions if (isinstance(p, tuple) and p[0] == partition_key) or (isinstance(p, str) and p == partition_key)]
            if not from_partitions:
                logger.error(f"Partition key '{partition_key}' not found in source cache")
                logger.info(f"Available partitions: {[p[0] if isinstance(p, tuple) else p for p in all_partitions]}")
                from_cache.close()
                to_cache.close()
                return
            logger.info(f"Copying only partition: {partition_key}")
        else:
            from_partitions = all_partitions
            logger.info(f"Copying all {len(from_partitions)} partitions")

        # First, copy partition metadata for selected partitions
        for partition_info in from_partitions:
            if isinstance(partition_info, tuple):
                pk, datatype = partition_info[0], partition_info[1]
            else:
                pk = str(partition_info)
                datatype = from_cache.get_datatype(pk) or "unknown"

            # Register partition in target if it doesn't exist
            try:
                if to_cache.get_datatype(pk) is None:
                    to_cache.register_partition_key(pk, datatype)
                    partitions_registered += 1
                    logger.info(f"Registered partition '{pk}' with datatype '{datatype}'")
            except Exception as e:
                logger.error(f"Failed to register partition '{pk}': {e}")

        # Copy keys by partition
        queries_copied = 0
        for partition_info in from_partitions:
            if isinstance(partition_info, tuple):
                current_partition_key = partition_info[0]
            else:
                current_partition_key = str(partition_info)

            try:
                keys = from_cache.get_all_keys(current_partition_key)

                for key in tqdm(keys, desc=f"Copying {current_partition_key}", unit="key", leave=False):
                    # Skip prefixed entries
                    if key.startswith("_LIMIT_") or key.startswith("_TIMEOUT_"):
                        prefixed_skipped += 1
                        continue

                    if not to_cache.exists(key, current_partition_key):
                        value = from_cache.get(key, current_partition_key)
                        if value is not None:
                            to_cache.set_set(key, value, current_partition_key)
                            added += 1
                    else:
                        skipped += 1

                # Copy queries metadata for this partition
                try:
                    queries = from_cache.get_all_queries(current_partition_key)
                    for query_hash, query_text in queries:
                        try:
                            if to_cache.set_query(query_hash, query_text, current_partition_key):
                                queries_copied += 1
                        except Exception as e:
                            logger.debug(f"Failed to copy query {query_hash} for partition {current_partition_key}: {e}")
                except Exception as e:
                    logger.debug(f"Error copying queries for partition {current_partition_key}: {e}")

            except Exception as e:
                logger.error(f"Error copying partition {current_partition_key}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        raise

    from_cache.close()
    to_cache.close()
    logger.info(
        f"Copy completed: {added} keys copied, {skipped} keys skipped, {prefixed_skipped} prefixed keys skipped, {partitions_registered} partitions registered, {queries_copied} queries copied"
    )


def export_cache(cache_type: str, archive_file: str, partition_key: str | None = None):
    cache = get_cache_handler(cache_type)
    exported_count = 0
    total_keys_found = 0

    with open(archive_file, "wb") as file:
        # First, export partition metadata
        try:
            all_partitions = cache.get_partition_keys()

            # Filter partitions if specific partition key requested
            if partition_key:
                partitions = [p for p in all_partitions if (isinstance(p, tuple) and p[0] == partition_key) or (isinstance(p, str) and p == partition_key)]
                if not partitions:
                    logger.error(f"Partition key '{partition_key}' not found in cache")
                    logger.info(f"Available partitions: {[p[0] if isinstance(p, tuple) else p for p in all_partitions]}")
                    cache.close()
                    return
                logger.info(f"Exporting only partition: {partition_key}")
            else:
                partitions = all_partitions
                logger.info(f"Exporting all {len(partitions)} partitions")

            # Build partition metadata for selected partitions
            partition_metadata = []
            for partition_info in partitions:
                if isinstance(partition_info, tuple):
                    pk, datatype = partition_info[0], partition_info[1]
                else:
                    pk = str(partition_info)
                    datatype = cache.get_datatype(pk) or "unknown"
                partition_metadata.append((pk, datatype))

            # Export partition metadata as special entry
            pickle.dump({"__PARTITION_METADATA__": partition_metadata}, file, protocol=pickle.HIGHEST_PROTOCOL)

            # Export queries metadata for selected partitions
            queries_metadata = []
            for partition_info in partitions:
                if isinstance(partition_info, tuple):
                    current_partition_key = partition_info[0]
                else:
                    current_partition_key = str(partition_info)

                try:
                    # Get all queries for this partition
                    queries = cache.get_all_queries(current_partition_key)
                    for query_hash, query_text in queries:
                        queries_metadata.append((query_hash, query_text, current_partition_key))
                except Exception as e:
                    logger.debug(f"Error exporting queries for partition {current_partition_key}: {e}")
                    continue

            if queries_metadata:
                logger.info(f"Exporting {len(queries_metadata)} queries metadata")
                pickle.dump({"__QUERIES_METADATA__": queries_metadata}, file, protocol=pickle.HIGHEST_PROTOCOL)

            # Export keys by partition
            for partition_info in partitions:
                if isinstance(partition_info, tuple):
                    current_partition_key = partition_info[0]
                else:
                    current_partition_key = str(partition_info)

                try:
                    keys = cache.get_all_keys(current_partition_key)
                    # skip prefixed entries
                    keys = [key for key in keys if not (key.startswith("_LIMIT_") or key.startswith("_TIMEOUT_"))]
                    total_keys_found += len(keys)

                    for key in tqdm(keys, desc=f"Exporting {current_partition_key}", unit="key", leave=False):
                        value = cache.get(key, current_partition_key)
                        if value is not None:
                            # Include partition key in export
                            pickle.dump({"key": key, "value": value, "partition_key": current_partition_key}, file, protocol=pickle.HIGHEST_PROTOCOL)
                            exported_count += 1

                except Exception as e:
                    logger.debug(f"Error exporting partition {current_partition_key}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error getting partitions: {e}")
            raise

    logger.info(f"Export completed: {exported_count} keys exported from {total_keys_found} total keys, queries metadata included, to {archive_file}")
    cache.close()


def restore_cache(cache_type: str, archive_file: str, target_partition_key: str | None = None):
    cache = get_cache_handler(cache_type)
    restored = 0
    skipped_already_exists = 0
    skipped_partition_filtered = 0
    partitions_registered = 0

    with open(archive_file, "rb") as file:
        while True:
            try:
                data = pickle.load(file)

                # Handle partition metadata
                if "__PARTITION_METADATA__" in data:
                    partition_metadata = data["__PARTITION_METADATA__"]
                    logger.info(f"Found partition metadata for {len(partition_metadata)} partitions")

                    # Register partitions found in metadata
                    partitions_to_register = set()

                    for pk, datatype in partition_metadata:
                        # If target partition specified, we'll register it when needed
                        if target_partition_key:
                            if pk == target_partition_key:
                                partitions_to_register.add((target_partition_key, datatype))
                        else:
                            # Register all original partitions
                            partitions_to_register.add((pk, datatype))

                    # If target partition specified but not found in metadata, register with inferred type
                    if target_partition_key and not any(p[0] == target_partition_key for p in partitions_to_register):
                        # Will infer datatype from first data entry
                        partitions_to_register.add((target_partition_key, None))

                    for effective_pk, datatype in partitions_to_register:
                        if datatype is None:
                            continue  # Will be handled during data import

                        try:
                            # Check if partition already exists
                            existing_datatype = cache.get_datatype(effective_pk)
                            if existing_datatype is None:
                                # Register new partition
                                cache.register_partition_key(effective_pk, datatype)
                                partitions_registered += 1
                                logger.info(f"Registered partition '{effective_pk}' with datatype '{datatype}'")
                            elif existing_datatype != datatype:
                                logger.warning(f"Partition '{effective_pk}' exists with different datatype: '{existing_datatype}' vs '{datatype}'")
                        except Exception as e:
                            logger.error(f"Failed to register partition '{effective_pk}': {e}")
                    continue

                # Handle queries metadata
                if "__QUERIES_METADATA__" in data:
                    queries_metadata = data["__QUERIES_METADATA__"]
                    logger.info(f"Found queries metadata for {len(queries_metadata)} queries")

                    queries_restored = 0
                    for query_hash, query_text, source_partition_key in queries_metadata:
                        # Skip if filtering by specific partition and this doesn't match
                        if target_partition_key and source_partition_key != target_partition_key:
                            continue

                        # Use target partition if specified, otherwise use source partition
                        effective_partition_key = target_partition_key if target_partition_key else source_partition_key

                        try:
                            # Store the query using the cache's set_query method
                            if cache.set_query(query_hash, query_text, effective_partition_key):
                                queries_restored += 1
                        except Exception as e:
                            logger.debug(f"Failed to restore query {query_hash} for partition {effective_partition_key}: {e}")

                    logger.info(f"Restored {queries_restored} queries metadata")
                    continue

                # Handle new format with explicit partition key
                if "key" in data and "value" in data and "partition_key" in data:
                    key = data["key"]
                    value = data["value"]
                    source_partition_key = data["partition_key"]

                    # Skip if filtering by specific partition and this doesn't match
                    if target_partition_key and source_partition_key != target_partition_key:
                        skipped_partition_filtered += 1
                        continue

                    # Use target partition if specified, otherwise use source partition
                    effective_partition_key = target_partition_key if target_partition_key else source_partition_key

                    # Ensure target partition is registered if needed
                    try:
                        if cache.get_datatype(effective_partition_key) is None:
                            # Infer datatype from value
                            if value and len(value) > 0:
                                sample_value = next(iter(value))
                                if isinstance(sample_value, int):
                                    datatype = "integer"
                                elif isinstance(sample_value, float):
                                    datatype = "float"
                                elif isinstance(sample_value, str):
                                    datatype = "text"
                                else:
                                    datatype = "text"  # default fallback

                                cache.register_partition_key(effective_partition_key, datatype)
                                partitions_registered += 1
                                logger.info(f"Auto-registered partition '{effective_partition_key}' with datatype '{datatype}'")
                    except Exception as e:
                        logger.warning(f"Could not register partition '{effective_partition_key}': {e}")

                    if not cache.exists(key, effective_partition_key):
                        cache.set_set(key, value, effective_partition_key)
                        restored += 1
                    else:
                        skipped_already_exists += 1

                # Handle legacy format (backward compatibility)
                elif len(data) == 1 and "__PARTITION_METADATA__" not in data:
                    key, value = list(data.items())[0]
                    # Use default partition key for legacy format
                    partition_key = "partition_key"

                    # Ensure default partition exists
                    try:
                        if cache.get_datatype(partition_key) is None:
                            # Try to infer datatype from value
                            if value and len(value) > 0:
                                sample_value = next(iter(value))
                                if isinstance(sample_value, int):
                                    datatype = "integer"
                                elif isinstance(sample_value, float):
                                    datatype = "float"
                                elif isinstance(sample_value, str):
                                    datatype = "text"
                                else:
                                    datatype = "text"  # default fallback

                                cache.register_partition_key(partition_key, datatype)
                                partitions_registered += 1
                                logger.info(f"Auto-registered default partition '{partition_key}' with datatype '{datatype}'")
                    except Exception as e:
                        logger.warning(f"Could not auto-register partition: {e}")

                    if not cache.exists(key, partition_key):
                        cache.set_set(key, value, partition_key)
                        restored += 1
                    else:
                        skipped_already_exists += 1

            except EOFError:
                break
            except Exception as e:
                logger.error(f"Error processing import entry: {e}")
                continue

    total_skipped = skipped_already_exists + skipped_partition_filtered
    logger.info(
        f"Restore completed: {restored} keys restored, {total_skipped} keys skipped ({skipped_already_exists} already exist, {skipped_partition_filtered} partition filtered), {partitions_registered} partitions registered"
    )
    cache.close()


def delete_cache(cache_type: str):
    cache = get_cache_handler(cache_type)
    deleted = 0
    total_keys_found = 0

    try:
        # Get all partitions
        partitions = cache.get_partition_keys()

        # Delete keys by partition
        for partition_info in partitions:
            if isinstance(partition_info, tuple):
                partition_key = partition_info[0]
            else:
                partition_key = str(partition_info)

            try:
                keys = cache.get_all_keys(partition_key)
                total_keys_found += len(keys)

                for key in tqdm(keys, desc=f"Deleting {partition_key}", unit="key", leave=False):
                    success = cache.delete(key, partition_key)
                    if success:
                        deleted += 1

            except Exception as e:
                logger.debug(f"Error deleting from partition {partition_key}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        raise

    logger.info(f"Deleted {deleted} keys from {total_keys_found} total keys in {cache_type} cache")
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
                        partition_key = str(partition_info)  # type: ignore[unreachable]

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


def show_comprehensive_status() -> None:
    """
    Show comprehensive status of PartitionCache including queue and cache statistics.
    Now shows status for ALL configured backends, not just the current ones.
    """
    logger.info("\n" + "=" * 70)
    logger.info("PartitionCache Status Overview")
    logger.info("=" * 70)

    # Environment Configuration
    logger.info("\n📋 Configuration:")
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
    queue_provider = os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")
    logger.info(f"  Default Cache Backend: {cache_backend}")
    logger.info(f"  Default Queue Provider: {queue_provider}")

    # Detect all configured backends
    configured_cache_backends = detect_configured_cache_backends()
    configured_queue_providers = detect_configured_queue_providers()

    logger.info(f"  Configured Cache Backends: {', '.join(configured_cache_backends) if configured_cache_backends else 'None'}")
    logger.info(f"  Configured Queue Providers: {', '.join(configured_queue_providers) if configured_queue_providers else 'None'}")

    # Queue Status for all configured providers
    logger.info("\n📊 Queue Status:")

    for provider in configured_queue_providers:
        logger.info(f"\n  Provider: {provider}")
        try:
            # Temporarily override environment to check this provider
            original_provider = os.environ.get("QUERY_QUEUE_PROVIDER")
            os.environ["QUERY_QUEUE_PROVIDER"] = provider

            queue_lengths = get_queue_lengths()
            original_query_count = queue_lengths.get("original_query_queue", 0)
            query_fragment_count = queue_lengths.get("query_fragment_queue", 0)
            total_queue_count = original_query_count + query_fragment_count

            logger.info(f"    Original Query Queue: {original_query_count:,} entries")
            logger.info(f"    Query Fragment Queue: {query_fragment_count:,} entries")
            logger.info(f"    Total Queue Entries: {total_queue_count:,}")

            # Check if pg_cron processor is enabled for PostgreSQL
            if provider == "postgresql":
                try:
                    queue_handler = get_queue_handler(provider)
                    if hasattr(queue_handler, "_check_pg_cron_job_exists") and queue_handler._check_pg_cron_job_exists():  # type: ignore
                        logger.info("    ✓ PostgreSQL Queue Processor (pg_cron) is enabled")
                    else:
                        logger.info("    ⚠ PostgreSQL Queue Processor (pg_cron) is not enabled")
                        logger.info("      Run: pcache-postgresql-queue-processor enable")
                    queue_handler.close()
                except Exception:
                    pass

            # Restore original environment
            if original_provider:
                os.environ["QUERY_QUEUE_PROVIDER"] = original_provider
            elif "QUERY_QUEUE_PROVIDER" in os.environ:
                del os.environ["QUERY_QUEUE_PROVIDER"]

        except Exception as e:
            logger.error(f"    ❌ Error accessing {provider} queue: {e}")
            logger.info(f"      Run: pcache-manage setup queue --queue {provider}")

    if not configured_queue_providers:
        logger.info("  ❌ No queue providers configured")
        logger.info("    Run: pcache-manage setup queue")

    # Cache Status for all configured backends
    logger.info("\n💾 Cache Status:")

    for backend in configured_cache_backends:
        logger.info(f"\n  Backend: {backend}")
        try:
            if backend.startswith("postgresql"):
                # Use PostgreSQL-specific counting for better accuracy
                partitions = get_partition_overview(backend)

                if not partitions:
                    logger.info("    No partitions found")
                else:
                    total_entries = sum(p["total_entries"] for p in partitions)
                    valid_entries = sum(p["valid_entries"] for p in partitions)
                    limit_entries = sum(p["limit_entries"] for p in partitions)
                    timeout_entries = sum(p["timeout_entries"] for p in partitions)

                    logger.info(f"    Total Cache Entries: {total_entries:,}")
                    logger.info(f"    Valid Entries: {valid_entries:,}")
                    logger.info(f"    Limit Entries: {limit_entries:,}")
                    logger.info(f"    Timeout Entries: {timeout_entries:,}")
                    logger.info(f"    Partitions: {len(partitions)}")

                    # Show top 3 partitions by size (reduced for multi-backend display)
                    if len(partitions) > 0:
                        sorted_partitions = sorted(partitions, key=lambda x: x["total_entries"], reverse=True)
                        logger.info("    Top Partitions:")
                        for i, partition in enumerate(sorted_partitions[:3]):
                            logger.info(f"      {i + 1}. {partition['partition_key']:<12}: {partition['total_entries']:,} entries")
                        if len(partitions) > 3:
                            logger.info(f"      ... and {len(partitions) - 3} more partitions")
            else:
                # For other cache types - add connection timeout protection
                try:
                    cache_handler = get_cache_handler(backend)

                    # Quick connectivity test with timeout for Redis/RocksDB
                    if backend.startswith(("redis_", "rocksdb_")):
                        # Test basic connectivity first - this will fail fast if service unavailable
                        try:
                            if isinstance(cache_handler, RedisAbstractCacheHandler):
                                # Quick Redis ping with timeout
                                if hasattr(cache_handler, "db") and hasattr(cache_handler.db, "ping"):
                                    cache_handler.db.ping()  # type: ignore[attr-defined]
                            elif backend.startswith("rocksdb_"):
                                from partitioncache.cache_handler.rocks_db_abstract import RocksDBAbstractCacheHandler
                                assert isinstance(cache_handler, RocksDBAbstractCacheHandler)
                                # Quick RocksDB access test
                                if hasattr(cache_handler, "db") and hasattr(cache_handler.db, "iterkeys"):
                                    _ = list(cache_handler.db.iterkeys())[:1]  # type: ignore[attr-defined]
                        except Exception as conn_error:
                            cache_handler.close()
                            raise Exception(f"Connection failed: {conn_error}") from conn_error

                    all_keys = get_all_keys(cache_handler)

                    limit_count = sum(1 for key in all_keys if key.startswith("_LIMIT_"))
                    timeout_count = sum(1 for key in all_keys if key.startswith("_TIMEOUT_"))
                    valid_count = len(all_keys) - limit_count - timeout_count

                    logger.info(f"    Total Cache Entries: {len(all_keys):,}")
                    logger.info(f"    Valid Entries: {valid_count:,}")
                    logger.info(f"    Limit Entries: {limit_count:,}")
                    logger.info(f"    Timeout Entries: {timeout_count:,}")

                    # Get partition information
                    try:
                        partitions = cache_handler.get_partition_keys()
                        logger.info(f"    Partitions: {len(partitions)}")
                    except (AttributeError, TypeError):
                        pass

                    cache_handler.close()
                except Exception as conn_error:
                    raise Exception(f"Cache backend connection error: {conn_error}") from conn_error

        except Exception as e:
            logger.error(f"    ❌ Error accessing {backend} cache: {e}")
            logger.info(f"      Run: pcache-manage setup cache --cache {backend}")

    if not configured_cache_backends:
        logger.info("  ❌ No cache backends configured")
        logger.info("    Run: pcache-manage setup cache")

    # System Health Check
    logger.info("\n🔍 System Health:")
    health_issues = []

    # Check environment
    if not os.getenv("CACHE_BACKEND"):
        health_issues.append("CACHE_BACKEND environment variable not set")

    # Check if eviction manager is set up (for any PostgreSQL backend)
    postgresql_backends = [b for b in configured_cache_backends if b.startswith("postgresql")]
    if postgresql_backends:
        try:
            # Import here to avoid circular dependency
            from partitioncache.cli.postgresql_cache_eviction import check_eviction_job_exists

            if not check_eviction_job_exists():
                health_issues.append("PostgreSQL Eviction Manager not enabled (pcache-postgresql-eviction-manager enable)")
        except Exception:
            pass

    if health_issues:
        for issue in health_issues:
            logger.info(f"  ⚠ {issue}")
    else:
        logger.info("  ✓ All systems operational")

    logger.info("\n" + "=" * 70 + "\n")


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
    removed = 0
    total_keys_found = 0

    try:
        # Get all partitions
        partitions = cache.get_partition_keys()

        # Remove termination entries by partition
        for partition_info in partitions:
            if isinstance(partition_info, tuple):
                partition_key = partition_info[0]
            else:
                partition_key = str(partition_info)

            try:
                keys = cache.get_all_keys(partition_key)
                total_keys_found += len(keys)

                for key in tqdm(keys, desc=f"Cleaning {partition_key}", unit="key", leave=False):
                    if key.find("_LIMIT_") == 0 or key.find("_TIMEOUT_") == 0:
                        success = cache.delete(key, partition_key)
                        if success:
                            removed += 1

            except Exception as e:
                logger.debug(f"Error cleaning partition {partition_key}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        raise

    logger.info(f"Removed {removed} termination entries from {total_keys_found} total keys in {cache_type} cache")
    cache.close()


def remove_large_entries(cache_type: str, max_entries: int):
    cache = get_cache_handler(cache_type)
    removed = 0
    total_keys_processed = 0

    try:
        # Get all partitions
        partitions = cache.get_partition_keys()

        # Remove large entries by partition
        for partition_info in partitions:
            if isinstance(partition_info, tuple):
                partition_key = partition_info[0]
            else:
                partition_key = str(partition_info)

            try:
                keys = cache.get_all_keys(partition_key)

                for key in tqdm(keys, desc=f"Checking {partition_key}", unit="key", leave=False):
                    total_keys_processed += 1
                    value = cache.get(key, partition_key)
                    if isinstance(value, set) and len(value) > max_entries:
                        success = cache.delete(key, partition_key)
                        if success:
                            removed += 1

            except Exception as e:
                logger.debug(f"Error processing partition {partition_key}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        raise

    logger.info(f"Removed {removed} entries with more than {max_entries} items from {cache_type} cache")
    logger.info(f"Total entries processed: {total_keys_processed}")
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
                        value = cache.get(key, partition_key)
                        if value and hasattr(value, "__len__"):
                            key_sizes.append((key, len(value)))

                    key_sizes.sort(key=lambda x: x[1], reverse=True)

                    for i in range(min(to_remove_count, len(key_sizes))):
                        key_to_delete = key_sizes[i][0]
                        cache.delete(key_to_delete, partition_key)
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
                partition_key = str(partition_info)  # type: ignore[unreachable]
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
  # Setup new project with specific backends
  pcache-manage setup all                                   # Use env defaults
  pcache-manage setup all --cache redis_set --queue postgresql
  pcache-manage setup cache --cache postgresql_array
  pcache-manage setup queue --queue redis

  # Check comprehensive system status (shows ALL configured backends)
  pcache-manage status                    # Shows all configured backends + stats
  pcache-manage status all               # Same as above
  pcache-manage status env               # Environment validation only
  pcache-manage status tables            # Table accessibility only

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

Note: Setup commands now support --cache and --queue options to override
environment defaults. Status command shows ALL configured backends with
valid environment variables. Use --env to load configuration from a custom file.
        """,
    )

    # Add common environment arguments
    add_environment_args(parser)
    add_verbosity_args(parser)

    # Create subparsers
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Setup commands
    setup_parser = subparsers.add_parser("setup", help="Setup PartitionCache tables and configuration")
    setup_subparsers = setup_parser.add_subparsers(dest="setup_command", help="Setup operations")

    # Setup all command
    setup_all = setup_subparsers.add_parser("all", help="Set up all tables (recommended for new projects)")
    setup_all.add_argument("--cache", type=str, help="Cache backend to setup (defaults to CACHE_BACKEND env var)")
    setup_all.add_argument("--queue", type=str, help="Queue provider to setup (defaults to QUERY_QUEUE_PROVIDER env var)")

    # Setup queue command
    setup_queue = setup_subparsers.add_parser("queue", help="Set up queue tables only")
    setup_queue.add_argument("--queue", type=str, help="Queue provider to setup (defaults to QUERY_QUEUE_PROVIDER env var)")

    # Setup cache command
    setup_cache = setup_subparsers.add_parser("cache", help="Set up cache metadata tables only")
    setup_cache.add_argument("--cache", type=str, help="Cache backend to setup (defaults to CACHE_BACKEND env var)")

    # Status commands
    status_parser = subparsers.add_parser("status", help="Check PartitionCache status and configuration")
    status_subparsers = status_parser.add_subparsers(dest="status_command", help="Status operations")

    status_subparsers.add_parser("all", help="Show comprehensive status overview (default)")
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
    cache_copy_parser.add_argument("--partition-key", dest="partition_key", help="Copy only specific partition key (default: all partitions)")

    # Cache export/import
    cache_export_parser = cache_subparsers.add_parser("export", help="Export cache to file")
    cache_export_parser.add_argument("--type", dest="cache_type", help="Cache type to export (default: from CACHE_BACKEND env var)")
    cache_export_parser.add_argument("--file", dest="archive_file", required=True, help="Archive file path")
    cache_export_parser.add_argument("--partition-key", dest="partition_key", help="Export only specific partition key (default: all partitions)")

    cache_import_parser = cache_subparsers.add_parser("import", help="Import cache from file")
    cache_import_parser.add_argument("--type", dest="cache_type", help="Cache type to import to (default: from CACHE_BACKEND env var)")
    cache_import_parser.add_argument("--file", dest="archive_file", required=True, help="Archive file path")
    cache_import_parser.add_argument(
        "--partition-key",
        dest="target_partition_key",
        help="Import only from specific partition or import to specific partition (default: preserve original partition keys)",
    )

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

    # Configure logging based on verbosity
    configure_logging(args)

    # Load environment variables
    load_environment_with_validation(args.env_file)

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
                setup_all_tables(cache_backend=getattr(args, "cache", None), queue_provider=getattr(args, "queue", None))
            elif args.setup_command == "queue":
                setup_queue_tables(queue_provider=getattr(args, "queue", None))
            elif args.setup_command == "cache":
                setup_cache_metadata_tables(cache_backend=getattr(args, "cache", None))

        elif args.command == "status":
            if not args.status_command or args.status_command == "all":
                # Default: show comprehensive status
                show_comprehensive_status()
                return

            if args.status_command == "env":
                env_valid = validate_environment()
                if not env_valid:
                    sys.exit(1)
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
                copy_cache(args.from_cache_type, args.to_cache_type, getattr(args, "partition_key", None))
            elif args.cache_command == "export":
                cache_type = args.cache_type or get_cache_type_from_env()
                export_cache(cache_type, args.archive_file, getattr(args, "partition_key", None))
            elif args.cache_command == "import":
                cache_type = args.cache_type or get_cache_type_from_env()
                restore_cache(cache_type, args.archive_file, getattr(args, "target_partition_key", None))
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

import argparse
import os
import pickle
from logging import getLogger

import redis
from dotenv import load_dotenv
from tqdm import tqdm  # type: ignore

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = getLogger("PartitionCache")


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
    all_keys = [key for key in all_keys if not key.startswith("_LIMIT_") and not key.startswith("_TIMEOUT_")]

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
    return cache.get_all_keys()


def count_cache(cache_type: str):
    cache = get_cache_handler(cache_type)
    all_keys = get_all_keys(cache)

    limit_count = 0
    timeout_count = 0
    valid_count = 0

    for key in all_keys:
        if key.startswith("_LIMIT_"):
            limit_count += 1
        elif key.startswith("_TIMEOUT_"):
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
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", ""),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("QUERY_QUEUE_REDIS_DB", 1)),  # Ensure this matches your queue DB
    )
    queue_length = r.llen("query_queue")
    logger.info(f"Number of entries in the queue: {queue_length}")
    long_running_queries_length = r.llen("long_running_queries")
    logger.info(f"Number of long running queries: {long_running_queries_length}")
    r.close()


def clear_queue():
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", ""),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("QUERY_QUEUE_REDIS_DB", 1)),  # Ensure this matches your queue DB
    )
    r.delete("query_queue")
    r.delete("long_running_queries")
    logger.info("Queue cleared.")
    r.close()


def delete_all_caches():
    confirmation = input("Are you sure you want to delete all caches? This action cannot be undone. (yes/no): ")
    if confirmation.lower() == "yes":
        cache_types = [
            "postgresql_array",
            "postgresql_bit",
            "redis",
            "redis_bit",
            "rocksdb",
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
        "redis",
        "redis_bit",
        "rocksdb",
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
        if key.startswith("_LIMIT_") or key.startswith("_TIMEOUT_"):
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


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Manage cache operations")

    parser.add_argument(
        "--env",
        dest="env_file",
        help="Environment file to use",
    )

    parser.add_argument(
        "--export",
        action="store_true",
        help="Export mode: export the cache",
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copy mode: copy the cache",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete mode: delete the cache",
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore mode: restore the cache",
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help="Count mode: count the number of keys in the cache",
    )

    parser.add_argument(
        "--count-queue",
        action="store_true",
        help="Count the number of entries in the queue",
    )

    parser.add_argument(
        "--clear-queue",
        action="store_true",
        help="Clear the queue",
    )

    parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete all caches",
    )
    parser.add_argument(
        "--count-all",
        action="store_true",
        help="Count all caches",
    )

    # Optional arguments for copy mode
    parser.add_argument(
        "--from",
        dest="from_cache_type",
        help="Source cache type (e.g., 'redis', 'rocksdb')",
    )
    parser.add_argument(
        "--to",
        dest="to_cache_type",
        help="Destination cache type (e.g., 'redis_bit', 'rocksdb_bit')",
    )

    # Optional arguments for delete, export, and restore modes
    parser.add_argument(
        "--cache",
        dest="cache_type",
        help="Cache type for delete, archive, and restore operations",
    )

    # Optional arguments for Export and restore modes
    parser.add_argument(
        "--file",
        dest="archive_file",
        help="Archive file path for archive and restore operations",
    )

    # Optional arguments for remove prefixed entries mode
    parser.add_argument(
        "--remove-termination",
        action="store_true",
        help="Remove termination entries (_LIMIT_ and _TIMEOUT_)",
    )

    parser.add_argument(
        "--remove-large",
        action="store_true",
        help="Remove entries with more than specified number of items",
    )
    parser.add_argument(
        "--max-entries",
        type=int,
        help="Maximum number of entries allowed in a set (for --remove-large)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv(args.env_file)

    if args.copy:
        if not args.from_cache_type or not args.to_cache_type:
            parser.error("Copy mode requires --from and --to arguments")
        copy_cache(args.from_cache_type, args.to_cache_type)
    elif args.delete:
        if not args.cache_type:
            parser.error("Delete mode requires --cache argument")
        delete_cache(args.cache_type)
    elif args.export:
        if not args.cache_type or not args.archive_file:
            parser.error("Export mode requires --cache and --file arguments")
        export_cache(args.cache_type, args.archive_file)
    elif args.restore:
        if not args.cache_type or not args.archive_file:
            parser.error("Restore mode requires --cache and --file arguments")
        restore_cache(args.cache_type, args.archive_file)
    elif args.count:
        if not args.cache_type:
            parser.error("Count mode requires --cache argument")
        count_cache(args.cache_type)
    elif args.count_queue:
        count_queue()
    elif args.clear_queue:
        clear_queue()
    elif args.delete_all:
        delete_all_caches()
    elif args.count_all:
        count_all_caches()
    elif args.remove_termination:
        if not args.cache_type:
            parser.error("Remove termination entries mode requires --cache argument")
        remove_termination_entries(args.cache_type)
    elif args.remove_large:
        if not args.cache_type or args.max_entries is None:
            parser.error("Remove large entries mode requires --cache and --max-entries arguments")
        remove_large_entries(args.cache_type, args.max_entries)
    else:
        parser.error(
            "Please specify a mode: --export, --copy, --delete, --restore, --count, --count_queue, --clear_queue, --delete-all, --count-all, --remove-termination, --remove-large"
        )

    # Cleanup

    if args.cache_type == "rocksdb" or args.cache_type == "rocksdb_bit" or args.to_cache_type == "rocksdb" or args.to_cache_type == "rocksdb_bit":
        cache = get_cache_handler(args.cache_type if args.cache_type is not None else args.to_cache_type)
        cache.compact()
        cache.close()


if __name__ == "__main__":
    main()

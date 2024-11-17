"""
CLI tool to read partition keys from the cache for a given query.
"""

import argparse
import sys

from logging import getLogger, WARNING

import dotenv

from partitioncache.cache_handler import get_cache_handler
from partitioncache.apply_cache import get_partition_keys

logger = getLogger("PartitionCache")

# set logger to warning
logger.setLevel(WARNING)

def main():
    parser = argparse.ArgumentParser(description="Read partition keys from cache for a given query")
    
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="SQL query to look up in cache"
    )
    
    parser.add_argument(
        "--cache-backend",
        type=str,
        default="rocksdb",
        help="Cache backend to use"
    )
    
    parser.add_argument(
        "--partition-key",
        type=str,
        default="partition_key",
        help="Name of the partition key column"
    )
    
    parser.add_argument(
        "--env-file",
        type=str,
        help="Path to environment file with cache configuration"
    )


    parser.add_argument(
        "--output-format",
        choices=['list', 'json', 'lines'],
        default='list',
        help="Output format for the partition keys, list is a simple comma separated list of partition keys, json is a json array and lines is one partition key per line"
    )

    args = parser.parse_args()

    # Load environment variables if specified
    if args.env_file:
        dotenv.load_dotenv(args.env_file)
    cache_handler = None
    
    try:
        # Initialize cache handler
        cache_handler = get_cache_handler(args.cache_backend)

        # Get partition keys
        partition_keys, num_subqueries, num_hits = get_partition_keys(
            query=args.query,
            cache_handler=cache_handler,
            partition_key=args.partition_key,
        )

        # Log cache hit statistics
        logger.info(f"Found {num_subqueries} subqueries")
        logger.info(f"Cache hits: {num_hits}")
        logger.info(f"Partition keys: {partition_keys}")

        if partition_keys is None:
            logger.warning("No partition keys found in cache")
            sys.exit(0)

        # Output results in the specified format
        if args.output_format == 'json':
            import json
            print(json.dumps(list(partition_keys)))
        
        elif args.output_format == 'lines':  # list format
            for key in partition_keys:
                sys.stdout.write(f"{key}\n")
        elif args.output_format == 'list':
            sys.stdout.write(",".join([str(x) for x in partition_keys]))

        # Print summary
        logger.info(f"Found {len(partition_keys)} partition keys")

    except Exception as e:
        logger.error(f"Error reading from cache: {str(e)}")
        sys.exit(1)
    finally:
        if cache_handler is not None:
            cache_handler.close()

if __name__ == "__main__":
    main()
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

def main(file=sys.stdout):
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
    
    parser.add_argument(
        "--output-file",
        type=str,
        help="Path to file to write the partition keys to, if not specified, the partition keys will be printed to the console"
    )    
    

    args = parser.parse_args()

    # Load environment variables if specified
    if args.env_file:
        dotenv.load_dotenv(args.env_file)
    cache_handler = None
    
    if args.output_file:
        with open(args.output_file, 'w') as f:
            file = f
    else:
        file = sys.stdout
    
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

        try:
            # Output results in the specified format
            if args.output_format == 'json':
                import json
                print(json.dumps(sorted(list(partition_keys))), file=file)
            
            elif args.output_format == 'lines':
                for key in sorted(partition_keys):
                    print(str(key), file=file)
            else:  # list format
                print(",".join(str(x) for x in sorted(partition_keys)), file=file)

            # Print summary
            logger.info(f"Found {len(partition_keys)} partition keys")
            sys.exit(0)

        except Exception as e:
            logger.error(f"Error formatting output: {str(e)}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error reading from cache: {str(e)}")
        sys.exit(1)
    finally:
        if cache_handler is not None:
            cache_handler.close()

if __name__ == "__main__":
    main()
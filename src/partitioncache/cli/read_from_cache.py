"""
CLI tool to read partition keys from the cache for a given query.
"""

import argparse
import sys
from logging import getLogger

import partitioncache
from partitioncache.cli.common_args import (
    add_cache_args,
    add_environment_args,
    add_output_args,
    add_verbosity_args,
    configure_logging,
    load_environment_with_validation,
    resolve_cache_backend,
)

logger = getLogger("PartitionCache")


def main(file=sys.stdout):
    parser = argparse.ArgumentParser(description="Read partition keys from cache for a given query")

    # Query configuration
    parser.add_argument("--query", type=str, required=True, help="SQL query to look up in cache")

    # Add common argument groups
    add_cache_args(parser, require_partition_key=False)
    add_environment_args(parser)
    add_output_args(parser)
    add_verbosity_args(parser)

    # Set default partition key if not provided
    parser.set_defaults(partition_key="partition_key", partition_datatype="integer")

    args = parser.parse_args()

    # Configure logging based on verbosity
    configure_logging(args)

    # Load environment variables
    load_environment_with_validation(args.env_file)

    # Resolve cache backend and determine output file
    cache_backend = resolve_cache_backend(args)

    # Determine output file
    if args.output_file:
        output_file = open(args.output_file, "w")
    else:
        output_file = sys.stdout

    cache = None
    try:
        # Initialize cache handler using API
        cache = partitioncache.create_cache_helper(cache_backend, args.partition_key, args.partition_datatype)

        # Get partition keys # TODO use cache.get_partition_keys() etc instead
        partition_keys, num_subqueries, num_hits = partitioncache.get_partition_keys(
            query=args.query,
            cache_handler=cache.underlying_handler,
            partition_key=args.partition_key,
        )

        # Log cache hit statistics
        logger.debug(f"Found {num_subqueries} subqueries")
        logger.debug(f"Cache hits: {num_hits}")
        logger.debug(f"Partition keys: {partition_keys}")

        if partition_keys is None:
            logger.warning("No partition keys found in cache")
            sys.exit(0)

        try:
            # Output results in the specified format
            if args.output_format == "json":
                import json

                print(json.dumps(sorted(partition_keys)), file=output_file)

            elif args.output_format == "lines":
                for key in sorted(partition_keys):
                    print(str(key), file=output_file)
            else:  # list format
                print(",".join(str(x) for x in sorted(partition_keys)), file=output_file)

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
        if cache is not None:
            cache.close()
        if args.output_file and output_file != sys.stdout:
            output_file.close()


if __name__ == "__main__":
    main()

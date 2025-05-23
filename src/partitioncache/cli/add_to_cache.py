"""
CLI tool to add queries to the cache either directly or via the queue.
"""

import argparse
import os
from logging import getLogger

import dotenv

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Query
from partitioncache.db_handler import get_db_handler
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.query_processor import clean_query, generate_all_query_hash_pairs, hash_query
from partitioncache.queue import push_to_original_query_queue, push_to_query_fragment_queue

logger = getLogger("PartitionCache")


def main():
    parser = argparse.ArgumentParser(description="Add queries to the partition cache either directly or via the queue")

    # Query configuration
    parser.add_argument("--query", type=str, help="SQL query to cache")

    parser.add_argument("--query-file", type=str, help="Path to file containing a SQL query to add to cache")

    parser.add_argument("--no-recompose", action="store_true", help="Do not recompose the query before adding to cache, the query is added as is to the cache or fragment queue")

    parser.add_argument("--partition-key", type=str, required=True, help="Name of the partition key column")

    # Queue configuration
    parser.add_argument("--queue", action="store_true", help="Add query to fragment queue instead of executing directly")
    
    parser.add_argument("--queue-original", action="store_true", help="Add query to original query queue instead of fragment queue")

    parser.add_argument("--queue-provider", type=str, default="postgresql", help="Queue provider to use")

    # Direct execution configuration
    parser.add_argument("--direct", action="store_true", help="Calculate fragments directly instead of using the incoming queue")

    parser.add_argument("--db-backend", type=str, default="postgresql", help="Database backend (currently only postgresql supported)")

    parser.add_argument("--db-name", type=str, help="Database name, if not specified, the database name will be read from the environment file")

    parser.add_argument("--env", type=str, default=".env", help="Path to environment file with database credentials")

    # Cache configuration
    parser.add_argument("--cache-backend", type=str, default="postgresql_bit", help="Cache backend to use")

    args = parser.parse_args()

    # Load environment variables
    dotenv.load_dotenv(args.env)

    # Validate mutually exclusive options
    queue_options = [args.queue, args.direct]
    if sum(queue_options) != 1:
        logger.error("Only one of --queue or --direct can be specified")
        exit(1)

    query_options = [args.query is not None, args.query_file is not None]
    if sum(query_options) != 1:
        logger.error("Only one of --query or --query-file can be provided")
        exit(1)

    if args.query_file:
        with open(args.query_file, "r") as f:
            query = f.read()
    else:
        query = args.query

    if args.queue:  # Add to queue for async processing
        if args.queue_original:
            success = push_to_original_query_queue(query, args.partition_key)
        else:
            if not args.no_recompose:  # Add to query fragment queue for async processing
                query_hash_pairs = generate_all_query_hash_pairs(
                    query,
                    args.partition_key,
                    min_component_size=1,
                    follow_graph=True,
                    keep_all_attributes=True,
                )
                success = push_to_query_fragment_queue(query_hash_pairs, args.partition_key)
            else:
                query = clean_query(query)
                query_hash_pairs = [(query, hash_query(query))]
                success = push_to_query_fragment_queue(query_hash_pairs, args.partition_key)
        if success:
            logger.info("Query successfully added to queue")
        else:
            logger.error("Failed to add query to queue")
            exit(1)

    elif args.direct:  # Execute directly
        if not args.no_recompose:
            logger.warning("Direct mode with recomposing may take a long time on large queries, consider using the queue instead")

        if args.db_name is None:
            db_name = os.getenv("DB_NAME")
            if db_name is None:
                logger.error("--db-name is required")
                exit(1)
            args.db_name = db_name

        try:
            # Initialize cache handler
            cache_handler = get_cache_handler(args.cache_backend)

            # Get database handler
            db_handler: AbstractDBHandler
            if args.db_backend == "postgresql":
                db_handler = get_db_handler(
                    "postgres",
                    host=os.getenv("PG_DB_HOST", os.getenv("DB_HOST", "localhost")),
                    port=int(os.getenv("PG_DB_PORT", os.getenv("DB_PORT", 5432))),
                    user=os.getenv("PG_DB_USER", os.getenv("DB_USER", "postgres")),
                    password=os.getenv("PG_DB_PASSWORD", os.getenv("DB_PASSWORD", "postgres")),
                    dbname=args.db_name,
                )
            elif args.db_backend == "mysql":
                db_handler = get_db_handler(
                    "mysql",
                    host=os.getenv("MY_DB_HOST", os.getenv("DB_HOST", "localhost")),
                    port=int(os.getenv("MY_DB_PORT", os.getenv("DB_PORT", 3306))),
                    user=os.getenv("MY_DB_USER", os.getenv("DB_USER", "root")),
                    password=os.getenv("MY_DB_PASSWORD", os.getenv("DB_PASSWORD", "root")),
                    dbname=args.db_name,
                )
            elif args.db_backend == "sqlite":
                db_handler = get_db_handler("sqlite", db_path=args.db_dir)
            else:
                raise ValueError(f"Unsupported database backend: {args.db_backend}")

            if not args.no_recompose:
                logger.info("Recomposing query")

                # Generate query-hash pairs
                query_hash_pairs = generate_all_query_hash_pairs(
                    query,
                    args.partition_key,
                    min_component_size=1,
                    follow_graph=True,
                    keep_all_attributes=True,
                )

            else:
                query = clean_query(query)
                query_hash_pairs = [(query, hash_query(query))]

            logger.info(f"Found {len(query_hash_pairs)} subqueries to process")

            # Process each query-hash pair
            for query, hash_value in query_hash_pairs:
                if cache_handler.exists(hash_value):
                    logger.info(f"Query {hash_value} already in cache")
                    if isinstance(cache_handler, AbstractCacheHandler_Query):
                        cache_handler.set_query(hash_value, query, args.partition_key)
                    continue

                # Execute query and store results
                result = set(db_handler.execute(query))
                if result:
                    cache_handler.set_set(hash_value, result)
                    if isinstance(cache_handler, AbstractCacheHandler_Query):
                        cache_handler.set_query(hash_value, query, args.partition_key)
                    logger.info(f"Stored query {hash_value} with {len(result)} results")
                else:
                    logger.warning(f"Query {hash_value} returned no results")

            db_handler.close()
            cache_handler.close()

        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            exit(1)


if __name__ == "__main__":
    main()

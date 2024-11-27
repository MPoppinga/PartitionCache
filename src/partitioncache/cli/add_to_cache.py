"""
CLI tool to add queries to the cache either directly or via the queue.
"""

import argparse
import os
from logging import getLogger

import dotenv

from partitioncache.cache_handler import get_cache_handler
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.db_handler.mysql import MySQLDBHandler
from partitioncache.db_handler.postgres import PostgresDBHandler
from partitioncache.db_handler.sqlite import SQLiteDBHandler
from partitioncache.query_processor import clean_query, generate_all_query_hash_pairs, hash_query
from partitioncache.queue import push_to_queue

logger = getLogger("PartitionCache")


def main():
    parser = argparse.ArgumentParser(description="Add queries to the partition cache")

    parser.add_argument("--query", type=str, required=True, help="SQL query to add to cache")

    parser.add_argument("--queue", action="store_true", help="Add query to queue instead of executing directly")

    parser.add_argument("--no-recompose", action="store_true", help="Do not recompose the query before adding to cache, if true the query is added as is")

    # Database configuration
    parser.add_argument("--db-backend", type=str, default="postgresql", help="Database backend (currently only postgresql supported)")

    parser.add_argument("--db-name", type=str, required=True, help="Database name")

    parser.add_argument("--env-file", type=str, help="Path to environment file with database credentials")

    # Cache configuration
    parser.add_argument("--cache-backend", type=str, default="rocksdb", help="Cache backend to use")

    parser.add_argument("--partition-key", type=str, default="partition_key", help="Name of the partition key column")

    args = parser.parse_args()

    # Load environment variables
    if args.env_file:
        dotenv.load_dotenv(args.env_file)

    if args.queue:
        if args.no_recompose:
            logger.error("Queue mode does not support --no-recompose")
            exit(1)
        # Add to queue for async processing
        success = push_to_queue(args.query)
        if success:
            logger.info("Query successfully added to queue")
        else:
            logger.error("Failed to add query to queue")
            exit(1)
    else:  # Execute directly
        if not args.no_recompose:
            logger.warning("Direct mode with recomposing may take a long time on large queries, consider using the queue instead")

        try:
            # Initialize cache handler
            cache_handler = get_cache_handler(args.cache_backend)

            # Initialize database handler
            db_handler: AbstractDBHandler
            if args.db_backend == "postgresql":
                if args.db_env_file is None:
                    raise ValueError("db_env_file is required")

                db_handler = PostgresDBHandler(
                    host=os.getenv("PG_DB_HOST", os.getenv("DB_HOST", "localhost")),
                    port=int(os.getenv("PG_DB_PORT", os.getenv("DB_PORT", 5432))),
                    user=os.getenv("PG_DB_USER", os.getenv("DB_USER", "postgres")),
                    password=os.getenv("PG_DB_PASSWORD", os.getenv("DB_PASSWORD", "postgres")),
                    dbname=args.db_name,
                )
            elif args.db_backend == "mysql":
                db_handler = MySQLDBHandler(
                    host=os.getenv("MY_DB_HOST", os.getenv("DB_HOST", "localhost")),
                    port=int(os.getenv("MY_DB_PORT", os.getenv("DB_PORT", 3306))),
                    user=os.getenv("MY_DB_USER", os.getenv("DB_USER", "root")),
                    password=os.getenv("MY_DB_PASSWORD", os.getenv("DB_PASSWORD", "root")),
                    dbname=args.db_name,
                )
            elif args.db_backend == "sqlite":
                db_handler = SQLiteDBHandler(args.db_dir)
            else:
                raise AssertionError("No db backend specified, querying not possible")

            if not args.no_recompose:
                logger.info("Recomposing query")

                # Generate query-hash pairs
                query_hash_pairs = generate_all_query_hash_pairs(
                    args.query, args.partition_key, min_component_size=1, follow_graph=True, keep_all_attributes=True
                )

            else:
                query = clean_query(args.query)
                query_hash_pairs = [(query, hash_query(query))]

            logger.info(f"Found {len(query_hash_pairs)} subqueries to process")

            # Process each query-hash pair
            for query, hash_value in query_hash_pairs:
                if cache_handler.exists(hash_value):
                    logger.info(f"Query {hash_value} already in cache")
                    continue

                # Execute query and store results
                result = set(db_handler.execute(query))
                if result:
                    cache_handler.set_set(hash_value, result)
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

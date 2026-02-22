"""
CLI tool to add queries to the cache either directly or via the queue.
"""

import argparse
import os
from logging import getLogger

import partitioncache
from partitioncache.cli.common_args import (
    add_cache_args,
    add_database_args,
    add_environment_args,
    add_queue_args,
    add_spatial_args,
    add_variant_generation_args,
    add_verbosity_args,
    configure_logging,
    get_database_connection_params,
    load_environment_with_validation,
    parse_variant_generation_json_args,
    resolve_cache_backend,
)
from partitioncache.db_handler import get_db_handler
from partitioncache.query_processor import clean_query, generate_all_query_hash_pairs, hash_query

logger = getLogger("PartitionCache")


def main():
    parser = argparse.ArgumentParser(description="Add queries to the partition cache either directly or via the queue")

    # Query configuration
    query_group = parser.add_argument_group("query configuration")
    query_group.add_argument("--query", type=str, help="SQL query to cache")
    query_group.add_argument("--query-file", type=str, help="Path to file containing a SQL query to add to cache")
    query_group.add_argument(
        "--no-recompose",
        action="store_true",
        help="Do not recompose the query before adding to cache, the query is added as is to the cache or fragment queue"
    )


    # Execution mode configuration
    mode_group = parser.add_argument_group("execution mode")
    mode_group.add_argument("--queue", action="store_true", help="Add query to fragment queue instead of executing directly")
    mode_group.add_argument("--queue-original", action="store_true", help="Add query to original query queue instead of fragment queue")
    mode_group.add_argument("--direct", action="store_true", help="Calculate fragments directly instead of using the incoming queue")

    # Add common argument groups
    add_cache_args(parser, require_partition_key=True)
    add_database_args(parser)
    add_queue_args(parser)
    add_spatial_args(parser, include_buffer_distance=False)
    add_variant_generation_args(parser)
    add_environment_args(parser)
    add_verbosity_args(parser)

    args = parser.parse_args()

    # Configure logging based on verbosity
    configure_logging(args)

    # Load environment variables with validation
    load_environment_with_validation(args.env_file)

    # Parse JSON arguments for constraint modifications
    add_constraints, remove_constraints_all, remove_constraints_add = parse_variant_generation_json_args(args)

    # Validate mutually exclusive options for execution mode
    queue_options = [args.queue, args.direct, args.queue_original]
    if sum(queue_options) != 1:
        logger.error("Only one of --queue, --direct or --queue-original can be specified")
        exit(1)

    # Validate query source options
    query_options = [args.query is not None, args.query_file is not None]
    if sum(query_options) != 1:
        logger.error("Only one of --query or --query-file can be provided")
        exit(1)

    if args.query_file:
        with open(args.query_file) as f:
            query = f.read()
    else:
        query = args.query

    # Determine queue provider
    queue_provider = getattr(args, 'queue_provider', None) or os.getenv("QUERY_QUEUE_PROVIDER", "postgresql")

    if args.queue_original:
        success = partitioncache.push_to_original_query_queue(query, args.partition_key, args.partition_datatype, queue_provider)
        if success:
            logger.info("Query successfully added to original query queue")
        else:
            logger.error("Failed to add query to original query queue")
            exit(1)

    elif args.queue:  # Add to queue for async processing
        is_spatial = args.partition_datatype == "geometry"

        # Resolve cache backend to store with queue item (important for spatial routing)
        queue_cache_backend = resolve_cache_backend(args) if is_spatial else None

        if not args.no_recompose:  # Add to query fragment queue for async processing
            # Resolve geometry column for spatial datatypes
            geometry_column = None
            if is_spatial:
                geometry_column = args.geometry_column
                if not geometry_column:
                    # Try to get from cache handler config
                    try:
                        cache_handler = partitioncache.get_cache_handler(resolve_cache_backend(args), singleton=True)
                        geometry_column = getattr(cache_handler, "geometry_column", "geom")
                    except Exception:
                        geometry_column = "geom"

            query_hash_pairs = generate_all_query_hash_pairs(
                query,
                args.partition_key,
                min_component_size=args.min_component_size,
                follow_graph=args.follow_graph,
                keep_all_attributes=True,
                auto_detect_partition_join=not args.no_auto_detect_partition_join,
                max_component_size=args.max_component_size,
                partition_join_table=args.partition_join_table,
                warn_no_partition_key=not args.no_warn_partition_key,
                bucket_steps=args.bucket_steps,
                add_constraints=add_constraints,
                remove_constraints_all=remove_constraints_all,
                remove_constraints_add=remove_constraints_add,
                skip_partition_key_joins=is_spatial,
                geometry_column=geometry_column,
            )
            success = partitioncache.push_to_query_fragment_queue(query_hash_pairs, args.partition_key, args.partition_datatype, queue_provider, cache_backend=queue_cache_backend)
        else: # Compute fragments and add to fragment queue
            query = clean_query(query)
            query_hash_pairs = [(query, hash_query(query))]
            success = partitioncache.push_to_query_fragment_queue(query_hash_pairs, args.partition_key, args.partition_datatype, queue_provider, cache_backend=queue_cache_backend)
        if success:
            logger.info("Query successfully added to query fragment queue")
        else:
            logger.error("Failed to add query to query fragment queue")
            exit(1)

    elif args.direct:  # Execute directly
        is_spatial = args.partition_datatype == "geometry"

        if not args.no_recompose:
            logger.warning("Direct mode with recomposing may take a long time on large queries, consider using the queue instead")

        try:
            # Resolve cache backend and database connection
            cache_backend = resolve_cache_backend(args)

            # Initialize cache handler using API with proper bitsize handling
            cache_handler = partitioncache.get_cache_handler(cache_backend, singleton=True)
            kwargs = {}
            if hasattr(cache_handler, '_get_partition_bitsize'):
                existing_bitsize = cache_handler._get_partition_bitsize(args.partition_key)  # type: ignore[attr-defined]
                if args.bitsize is not None:
                    kwargs['bitsize'] = args.bitsize
                    if existing_bitsize is not None and existing_bitsize != args.bitsize:
                        logger.info(f"Updating bitsize from {existing_bitsize} to {args.bitsize} for partition '{args.partition_key}'")
                        cache_handler._set_partition_bitsize(args.partition_key, args.bitsize)  # type: ignore[attr-defined]
                elif existing_bitsize is not None:
                    kwargs['bitsize'] = existing_bitsize
            elif hasattr(args, 'bitsize') and args.bitsize is not None:
                kwargs['bitsize'] = args.bitsize
            cache = partitioncache.create_partitioncache_helper(cache_handler, args.partition_key, args.partition_datatype, **kwargs)

            # Resolve geometry column for spatial datatypes
            geometry_column = None
            if is_spatial:
                geometry_column = args.geometry_column or getattr(cache_handler, "geometry_column", "geom")

            if not args.no_recompose:
                logger.debug("Recomposing query")

                # Generate query-hash pairs
                query_hash_pairs = generate_all_query_hash_pairs(
                    query,
                    args.partition_key,
                    min_component_size=args.min_component_size,
                    follow_graph=args.follow_graph,
                    keep_all_attributes=True,
                    auto_detect_partition_join=not args.no_auto_detect_partition_join,
                    max_component_size=args.max_component_size,
                    partition_join_table=args.partition_join_table,
                    warn_no_partition_key=not args.no_warn_partition_key,
                    bucket_steps=args.bucket_steps,
                    add_constraints=add_constraints,
                    remove_constraints_all=remove_constraints_all,
                    remove_constraints_add=remove_constraints_add,
                    skip_partition_key_joins=is_spatial,
                    geometry_column=geometry_column,
                )

            else:
                query = clean_query(query)
                query_hash_pairs = [(query, hash_query(query))]

            logger.debug(f"Found {len(query_hash_pairs)} subqueries to process")

            if is_spatial:
                # Spatial mode: use lazy insertion (the handler wraps fragments with H3/BBox SQL)
                if not hasattr(cache_handler, "set_cache_lazy"):
                    logger.error(f"Spatial datatype '{args.partition_datatype}' requires a handler with set_cache_lazy support")
                    exit(1)

                for query, hash_value in query_hash_pairs:
                    if cache.exists(hash_value):
                        logger.debug(f"Query {hash_value} already in cache")
                        cache.set_query(hash_value, query)
                        continue

                    success = cache_handler.set_cache_lazy(hash_value, query, args.partition_key)
                    if success:
                        cache.set_query(hash_value, query)
                        logger.debug(f"Lazily stored spatial query {hash_value}")
                    else:
                        logger.warning(f"Failed to lazily store spatial query {hash_value}")

                cache.close()
            else:
                # Standard mode: execute fragments via db_handler and store results
                db_connection_params = get_database_connection_params(args)

                if args.db_backend == "postgresql":
                    db_handler = get_db_handler("postgres", **db_connection_params)
                elif args.db_backend == "mysql":
                    db_handler = get_db_handler("mysql", **db_connection_params)
                elif args.db_backend == "sqlite":
                    db_handler = get_db_handler("sqlite", **db_connection_params)
                else:
                    raise ValueError(f"Unsupported database backend: {args.db_backend}")

                # Process each query-hash pair
                for query, hash_value in query_hash_pairs:
                    if cache.exists(hash_value):
                        logger.debug(f"Query {hash_value} already in cache")
                        cache.set_query(hash_value, query)
                        continue

                    # Execute query and store results
                    result = set(db_handler.execute(query))
                    if result:
                        cache.set_cache(hash_value, result)
                        cache.set_query(hash_value, query)
                        logger.debug(f"Stored query {hash_value} with {len(result)} results")
                    else:
                        logger.warning(f"Query {hash_value} returned no results")

                db_handler.close()
                cache.close()

        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            exit(1)


if __name__ == "__main__":
    main()

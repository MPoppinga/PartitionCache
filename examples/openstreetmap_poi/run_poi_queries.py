import os
import time

import psycopg2
from dotenv import load_dotenv

import partitioncache
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

# Load environment variables
load_dotenv(".env", override=True)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Choose your cache backend (see README for options)
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "postgresql_array")


# TODO allow option to add query to queue


def run_query(conn, query, params=None):
    with conn.cursor() as cur:
        start = time.perf_counter()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
    return rows, elapsed


def test_partition_key(conn, partition_key: str, datatype: str = "integer"):
    """Test queries with a specific partition key."""
    print(f"\n{'=' * 60}")
    print(f"Running with partition key: {partition_key} (datatype: {datatype})")
    print(f"Cache backend: {CACHE_BACKEND}")
    print(f"{'=' * 60}")

    queries = []
    for file in sorted(os.listdir(os.path.join("testqueries_examples", partition_key))):
        with open(os.path.join("testqueries_examples", partition_key, file)) as f:
            queries.append((file, f.read()))

    # Create cache handler using the API with context manager
    try:
        with partitioncache.create_cache_helper(CACHE_BACKEND, partition_key, datatype) as cache:
            for description, sql_query in queries:
                print(f"\n---\nQuery: {description}\n")

                # Run without cache
                rows, elapsed = run_query(conn, sql_query)
                print(f"Without PartitionCache: {len(rows)} results in {elapsed:.3f} seconds")

                start_cache = time.perf_counter()
                # Use PartitionCache to get partition keys for this query
                partition_keys, num_subqueries, num_hits = partitioncache.get_partition_keys(
                    query=sql_query,
                    cache_handler=cache.underlying_handler,
                    partition_key=partition_key,
                    min_component_size=1,
                )

                if partition_keys:
                    # Extend the query to restrict to cached partition keys
                    try:
                        sql_cached = partitioncache.extend_query_with_partition_keys(
                            sql_query,
                            partition_keys,
                            partition_key=partition_key,
                            method="IN",
                            p0_alias="p1",  # assumes p1 is the main table alias for partition key
                        )
                        elapsed_cache_get = time.perf_counter() - start_cache
                        rows_cached, elapsed_cached = run_query(conn, sql_cached)
                        print(
                            f"With PartitionCache: {len(rows_cached)} results in {elapsed_cache_get:.3f} + {elapsed_cached:.3f} seconds (cache hits: {num_hits}, partition keys: {len(partition_keys)})"
                        )
                    except Exception as e:
                        print(f"Skipping IN method (text values with special characters): {type(e).__name__}")
                else:
                    if num_hits > 0:
                        print(f"Cache entries found ({num_hits} hash matches) but no common partition keys after intersection.")
                    else:
                        print("No cache entries found for this query.")
                    print("Hint: Add queries to cache using:")
                    print(
                        f"  pcache-add --direct --query-file testqueries_examples/{partition_key}/{description} --partition-key {partition_key} --partition-datatype {datatype} --cache-backend {CACHE_BACKEND}"
                    )
                    print("  or use queue with postgresql queue processor (recommended):")
                    print("  pcache-postgresql-queue-processor setup && pcache-postgresql-queue-processor enable # Setup and enable postgresql queue processor")
                    print(f"  pcache-add --queue --query-file testqueries_examples/{partition_key}/{description} --partition-key {partition_key}")

                # Test lazy intersection if supported
                if isinstance(cache.underlying_handler, AbstractCacheHandler_Lazy):
                    start_cache = time.perf_counter()
                    lazy_cache_subquery, nr_generated_variants, nr_used_hashes = partitioncache.get_partition_keys_lazy(
                        query=sql_query,
                        cache_handler=cache.underlying_handler,
                        partition_key=partition_key,
                        min_component_size=1,
                    )
                    if lazy_cache_subquery is not None:
                        elapsed_cache_get = time.perf_counter() - start_cache
                        sql_cached = sql_query.strip().strip(";") + f" AND p1.{partition_key} IN (" + lazy_cache_subquery + ")"
                        rows_cached, elapsed_cached = run_query(conn, sql_cached)

                        print(
                            f"With PartitionCache (lazy): {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                            f"{elapsed_cached:.3f} seconds (cache hash matches: {nr_used_hashes})"
                        )

                        # LAZY optimized query with TMP and p0 table to improve performance of optimized for example better join ordering
                        tmp_query = f" CREATE TEMP TABLE tmp_cache_keys AS ({lazy_cache_subquery});"
                        tmp_query += f"CREATE INDEX ON tmp_cache_keys ({partition_key}); "
                        tmp_query += "ANALYZE tmp_cache_keys;"

                        sql_cached = tmp_query + f"{sql_query.strip().strip(';')} AND p1.{partition_key} IN (SELECT * FROM tmp_cache_keys);"
                        rows_cached, elapsed_cached = run_query(conn, sql_cached)
                        conn.cursor().execute("DROP TABLE tmp_cache_keys;")

                        print(
                            f"With PartitionCache (lazy optimized): {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                            f"{elapsed_cached:.3f} seconds (cache hash matches: {nr_used_hashes})"
                        )

                        # Test apply_cache_lazy (regular - no p0 table)
                        start_cache = time.perf_counter()
                        sql_cached, stats = partitioncache.apply_cache_lazy(
                            query=sql_query,
                            cache_handler=cache.underlying_handler,
                            partition_key=partition_key,
                            method="TMP_TABLE_IN",
                            p0_alias="p1",  # Specify which table to apply cache restrictions to
                            min_component_size=1,
                        )
                        elapsed_cache_get = time.perf_counter() - start_cache
                        rows_cached, elapsed_cached = run_query(conn, sql_cached)

                        print(
                            f"With apply_cache_lazy: {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                            f"{elapsed_cached:.3f} seconds (generated: {stats['generated_variants']}, "
                            f"hits: {stats['cache_hits']}, enhanced: {stats['enhanced']})"
                        )

                        # Test apply_cache_lazy with p0 table (star-schema pattern)
                        # Cache restrictions automatically target the p0 table when use_p0_table=True
                        start_cache = time.perf_counter()
                        sql_cached, stats = partitioncache.apply_cache_lazy(
                            query=sql_query,
                            cache_handler=cache.underlying_handler,
                            partition_key=partition_key,
                            method="TMP_TABLE_IN",
                            min_component_size=1,
                            use_p0_table=True,
                            p0_alias="p0",
                        )
                        print(sql_cached)
                        elapsed_cache_get = time.perf_counter() - start_cache
                        rows_cached, elapsed_cached = run_query(conn, sql_cached)

                        print(
                            f"With apply_cache_lazy + p0 table: {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                            f"{elapsed_cached:.3f} seconds (generated: {stats['generated_variants']}, "
                            f"hits: {stats['cache_hits']}, enhanced: {stats['enhanced']}, p0: {stats['p0_rewritten']})"
                        )

    except ValueError as e:
        print(f"Cache configuration error: {e}")
        return


def test_spatial_partition_key(conn, partition_key: str, cache_backend: str, geometry_column: str = "geom", buffer_distance: float = 500.0):
    """Test queries with a spatial partition key (geometry datatype)."""
    query_dir = os.path.join("testqueries_examples", "spatial")
    if not os.path.exists(query_dir):
        print(f"\nSkipping spatial partition key '{partition_key}' - no test queries found in {query_dir}")
        return

    print(f"\n{'=' * 60}")
    print(f"Running with spatial partition key: {partition_key} (datatype: geometry)")
    print(f"Cache backend: {cache_backend}, geometry_column: {geometry_column}, buffer_distance: {buffer_distance}m")
    print(f"{'=' * 60}")

    queries = []
    for file in sorted(os.listdir(query_dir)):
        if file.endswith(".sql"):
            with open(os.path.join(query_dir, file)) as f:
                queries.append((file, f.read()))

    try:
        cache_handler = partitioncache.get_cache_handler(cache_backend, singleton=True)

        for description, sql_query in queries:
            print(f"\n---\nQuery: {description}\n")

            # Run without cache
            rows, elapsed = run_query(conn, sql_query)
            print(f"Without PartitionCache: {len(rows)} results in {elapsed:.3f} seconds")

            # Test non-lazy apply_cache with spatial filter
            start_cache = time.perf_counter()
            sql_cached, stats = partitioncache.apply_cache(
                query=sql_query,
                cache_handler=cache_handler,
                partition_key=partition_key,
                geometry_column=geometry_column,
                buffer_distance=buffer_distance,
                min_component_size=1,
            )
            elapsed_cache_get = time.perf_counter() - start_cache

            if stats["enhanced"]:
                rows_cached, elapsed_cached = run_query(conn, sql_cached)
                print(
                    f"With apply_cache (spatial): {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                    f"{elapsed_cached:.3f} seconds (generated: {stats['generated_variants']}, "
                    f"hits: {stats['cache_hits']}, enhanced: {stats['enhanced']})"
                )
            else:
                if stats["cache_hits"] > 0:
                    print(f"Cache entries found ({stats['cache_hits']} hash matches) but query was not enhanced.")
                else:
                    print("No cache entries found for this query.")
                    print("Hint: Add queries to cache using:")
                    print(
                        f"  pcache-add --direct --query-file {query_dir}/{description} "
                        f"--partition-key {partition_key} --partition-datatype geometry "
                        f"--cache-backend {cache_backend} --geometry-column {geometry_column}"
                    )

            # Test lazy apply_cache_lazy with spatial filter
            start_cache = time.perf_counter()
            sql_cached, stats = partitioncache.apply_cache_lazy(
                query=sql_query,
                cache_handler=cache_handler,
                partition_key=partition_key,
                geometry_column=geometry_column,
                buffer_distance=buffer_distance,
                method="TMP_TABLE_IN",
                min_component_size=1,
            )
            elapsed_cache_get = time.perf_counter() - start_cache

            if stats["enhanced"]:
                rows_cached, elapsed_cached = run_query(conn, sql_cached)
                print(
                    f"With apply_cache_lazy (spatial): {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                    f"{elapsed_cached:.3f} seconds (generated: {stats['generated_variants']}, "
                    f"hits: {stats['cache_hits']}, enhanced: {stats['enhanced']})"
                )

    except ValueError as e:
        print(f"Cache configuration error: {e}")
        return


# Spatial cache backend (set via env or override)
SPATIAL_CACHE_BACKEND = os.getenv("SPATIAL_CACHE_BACKEND", "postgis_h3")
GEOMETRY_COLUMN = os.getenv("PARTITION_CACHE_GEOMETRY_COLUMN", "geom")
BUFFER_DISTANCE = float(os.getenv("PARTITION_CACHE_BUFFER_DISTANCE", "500"))


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

    print(f"Initializing PartitionCache backend: {CACHE_BACKEND}")

    # Test with zipcode partition key (integer)
    test_partition_key(conn, "zipcode", "integer")

    # Test with landkreis partition key (text)
    test_partition_key(conn, "landkreis", "text")

    # Test with spatial partition key
    test_spatial_partition_key(
        conn, "spatial", SPATIAL_CACHE_BACKEND,
        geometry_column=GEOMETRY_COLUMN, buffer_distance=BUFFER_DISTANCE,
    )

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

import os
import psycopg2
import time
from dotenv import load_dotenv

import partitioncache

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
        with open(os.path.join("testqueries_examples", partition_key, file), "r") as f:
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
                if hasattr(cache.underlying_handler, "get_intersected_lazy"):
                    start_cache = time.perf_counter()
                    lazy_cache_subquery, nr_used_hashes = partitioncache.get_partition_keys_lazy(
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

    except ValueError as e:
        print(f"Cache configuration error: {e}")
        return


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

    print(f"Initializing PartitionCache backend: {CACHE_BACKEND}")

    # Test with zipcode partition key (integer)
    test_partition_key(conn, "zipcode", "integer")

    # Test with landkreis partition key (text)
    test_partition_key(conn, "landkreis", "text")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

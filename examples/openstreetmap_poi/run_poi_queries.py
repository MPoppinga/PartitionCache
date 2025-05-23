import os
import psycopg2
import time
from dotenv import load_dotenv

from partitioncache.cache_handler import get_cache_handler
from partitioncache.apply_cache import get_partition_keys, extend_query_with_partition_keys, get_partition_keys_lazy

# Load environment variables
load_dotenv(".env.example", override=True)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Choose your cache backend (see README for options)
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "postgresql_bit")  # or "postgresql_array", "rocksdb", etc.
PARTITION_KEY = os.getenv("PARTITION_KEY", "zipcode")

QUERIES = []
for file in sorted(os.listdir("testqueries_examples")):
    with open(os.path.join("testqueries_examples", file), "r") as f:
        QUERIES.append((file, f.read()))


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


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

    print(f"Initializing PartitionCache backend: {CACHE_BACKEND}")
    cache_handler = get_cache_handler(CACHE_BACKEND)

    for description, sql_query in QUERIES:
        print(f"\n---\nQuery: {description}\n")
        # Run without cache
        rows, elapsed = run_query(conn, sql_query)
        print(f"Without PartitionCache: {len(rows)} results in {elapsed:.3f} seconds")

        start_cache = time.perf_counter()
        # Use PartitionCache to get partition keys for this query
        partition_keys, num_subqueries, num_hits = get_partition_keys(
            query=sql_query,
            cache_handler=cache_handler,
            partition_key=PARTITION_KEY,
            min_component_size=1,
        )
        if partition_keys:
            # Extend the query to restrict to cached partition keys
            sql_cached = extend_query_with_partition_keys(
                sql_query,
                partition_keys,
                partition_key=PARTITION_KEY,
                method="IN",
                p0_alias="p1",  # assumes p1 is the main table alias for partition key
            )
            elapsed_cache_get = time.perf_counter() - start_cache
            rows_cached, elapsed_cached = run_query(conn, sql_cached)
            print(
                f"With PartitionCache: {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                f"{elapsed_cached:.3f} seconds (cache hits: {num_hits})"
            )

        else:
            print("No Partiton Keys found. Hint: Add one of the queries to the cache to improve performance.")

        # Lazy get
        start_cache = time.perf_counter()
        lazy_cache_subquery, nr_used_hashes = get_partition_keys_lazy(
            query=sql_query,
            cache_handler=cache_handler,
            partition_key=PARTITION_KEY,
            min_component_size=1,
        )
        if lazy_cache_subquery is not None:
            elapsed_cache_get = time.perf_counter() - start_cache
            sql_cached = sql_query.strip().strip(";") + " AND p1.zipcode IN (" + lazy_cache_subquery + ")"
            rows_cached, elapsed_cached = run_query(conn, sql_cached)
            print(
                f"With PartitionCache: {len(rows_cached)} results in {elapsed_cache_get:.3f} + "
                f"{elapsed_cached:.3f} seconds (cache hits: {nr_used_hashes})"
            )
        else:
            print("No Partiton Keys found. Hint: Add one of the queries to the cache to improve performance.")

    cache_handler.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

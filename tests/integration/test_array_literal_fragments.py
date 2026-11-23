"""Fragments of queries with PostgreSQL array constructors run on PostgreSQL and keep every key of the query.

The fragment generator used to render ``ARRAY['x']`` as ``ARRAY('x')``, which PostgreSQL rejects, so such fragments
never reached the cache.
"""

import os

import psycopg
import pytest

from partitioncache.query_processor import generate_all_query_hash_pairs

CONN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "integration_user"),
    "password": os.getenv("PG_PASSWORD", "test_password"),
    "dbname": os.getenv("PG_DBNAME", "partitioncache_integration"),
}
ITEMS, TAGS = "pc_arr_items", "pc_arr_store_tags"
QUERY = (
    f"SELECT DISTINCT t1.store_id FROM {ITEMS} AS t1, {ITEMS} AS t2 "
    "WHERE t1.store_id = t2.store_id AND abs(t1.price - t2.price) <= 2 AND t1.id <> t2.id "
    f"AND t1.store_id IN (SELECT store_id FROM {TAGS} WHERE tag_id = 7 AND ({{condition}}))"
)
CONDITIONS = [
    "tag_value ILIKE ANY (ARRAY['%blue%'])",
    "tag_value LIKE ANY (ARRAY['%blue%', 'green%'])",
    "tag_id = ANY (ARRAY[7, 8])",
    "tag_value <> ALL (ARRAY['red'])",
]


@pytest.fixture(scope="module")
def conn():
    try:
        connection = psycopg.connect(**CONN, autocommit=True, connect_timeout=10)
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    with connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {ITEMS}, {TAGS}")
        cur.execute(f"CREATE TABLE {ITEMS} (id int, store_id int, price float)")
        cur.execute(f"CREATE TABLE {TAGS} (store_id int, tag_id int, tag_value text)")
        cur.execute(f"INSERT INTO {ITEMS} VALUES (1, 1, 0.0), (2, 1, 1.0), (3, 2, 5.0), (4, 2, 6.5), (5, 3, 1.0), (6, 3, 9.0)")
        cur.execute(f"INSERT INTO {TAGS} VALUES (1, 7, 'dark_blue'), (2, 7, 'green_light'), (3, 7, 'red')")
    yield connection
    with connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {ITEMS}, {TAGS}")
    connection.close()


def keys(conn, query: str) -> set[int]:
    with conn.cursor() as cur:
        cur.execute(query)
        return {row[0] for row in cur.fetchall()}


@pytest.mark.parametrize("condition", CONDITIONS)
def test_every_fragment_runs_and_keeps_the_keys_of_the_query(conn, condition: str) -> None:
    query = QUERY.format(condition=condition)
    expected = keys(conn, query)
    pairs = generate_all_query_hash_pairs(query, "store_id", warn_no_partition_key=False)
    for fragment, _ in pairs:
        assert keys(conn, fragment) >= expected, fragment
    assert any("ARRAY[" in fragment for fragment, _ in pairs), "the array condition should survive in some fragments"

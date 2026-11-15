"""
Eager key lists and key tables keep the type of the keys; float keys restrict only with IN.

As bare string literals, timestamp keys made ``IN (VALUES ...)`` a text column, which PostgreSQL and DuckDB do not
compare with a timestamp column; the temporary key table's ``INSERT ... (VALUES ...)`` was a syntax error in SQLite.
Float keys stay untyped string literals in IN, which take the column's type (exact for a float4 column too); VALUES
and the key tables refuse them, as a double precision key list would silently miss a float4 column. The SQLite and DuckDB tests always run; TestPostgreSQL runs with PARTITIONCACHE_TEST_PG_HOST/PORT/
USER/PASSWORD/DB and is skipped without them.
"""

import math
import os
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from partitioncache.apply_cache import _format_spliced_partition_key, extend_query_with_partition_keys

EAGER_METHODS = ["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]
UNLOGGED_METHODS = ["UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"]
ROWS = [
    (1, 2**40, 1.5, 0.1, Decimal("1.50"), datetime(2024, 1, 1, 12), datetime(2024, 1, 1, 12, tzinfo=UTC), "o'brien"),
    (2, 3, 2.5, 0.2, Decimal("2.25"), datetime(2024, 1, 2), datetime(2024, 1, 2, 3, tzinfo=timezone(timedelta(hours=2))), "back\\slash"),
    (3, 4, math.inf, 0.3, Decimal("3.00"), datetime(2024, 1, 3), datetime(2024, 1, 3, tzinfo=UTC), "plain"),
    (4, 5, math.nan, 0.4, Decimal("4.75"), datetime(2024, 1, 4), datetime(2024, 1, 4, tzinfo=UTC), "x"),
]
COLUMNS = "i, big, f, f4, d, ts, tstz, s"
# (column, keys, ids of the rows they select)
CASES = {
    "bigint": ("big", {2**40, 3}, {1, 2}),
    "float": ("f", {1.5, 2.5}, {1, 2}),
    "float_inf": ("f", {math.inf, 1.5}, {1, 3}),
    "float_nan": ("f", {math.nan, 2.5}, {2, 4}),
    "float4": ("f4", {0.1, 0.2}, {1, 2}),  # the doubles nearest to the texts psycopg reads back from a real column
    "decimal": ("d", {Decimal("1.50"), Decimal("4.75")}, {1, 4}),
    "timestamp": ("ts", {datetime(2024, 1, 1, 12), datetime(2024, 1, 2)}, {1, 2}),
    "timestamptz": ("tstz", {datetime(2024, 1, 1, 12, tzinfo=UTC), datetime(2024, 1, 2, 1, tzinfo=UTC)}, {1, 2}),
    "text": ("s", {"o'brien", "back\\slash"}, {1, 2}),
}


class TestLiterals:
    @pytest.mark.parametrize(
        ("key", "dialect", "literal"),
        [
            (2**40, "postgres", "1099511627776"),
            (Decimal("1.50"), "postgres", "1.50"),
            (Decimal("NaN"), "postgres", "'NaN'"),
            (1.5, "postgres", "'1.5'"),
            (math.inf, "postgres", "'inf'"),
            (math.nan, "duckdb", "'nan'"),
            (math.inf, "sqlite", "9e999"),
            (-math.inf, "sqlite", "-9e999"),
            (datetime(2024, 1, 2, 3, 4, 5), "postgres", "CAST('2024-01-02 03:04:05' AS TIMESTAMP)"),
            (datetime(2024, 1, 2, tzinfo=UTC), "duckdb", "CAST('2024-01-02 00:00:00+00:00' AS TIMESTAMPTZ)"),
            (datetime(2024, 1, 2, 3, 4, 5), "sqlite", "'2024-01-02 03:04:05'"),
            ("o'brien", "postgres", "'o''brien'"),
            ("back\\slash", "mysql", "'back\\\\slash'"),
        ],
    )
    def test_literal(self, key, dialect, literal):
        assert _format_spliced_partition_key(key, dialect) == literal

    def test_temporary_key_table_insert_is_plain_values(self):
        script = extend_query_with_partition_keys("SELECT p.i FROM t AS p", {1}, "k", method="TMP_TABLE_IN", p0_alias="p", sql_dialect="sqlite")
        assert "(partition_key) VALUES (1);" in script


def _sqlite():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE probe (i INTEGER, big INTEGER, f REAL, f4 REAL, d NUMERIC, ts TIMESTAMP, tstz TIMESTAMP, s TEXT)")
    conn.executemany(f"INSERT INTO probe ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [(*row[:4], str(row[4]), str(row[5]), str(row[6]), row[7]) for row in ROWS])

    def run(script: str) -> set:
        setup, _, select = script.rpartition(";")
        if setup.strip():
            conn.executescript(setup + ";")
        return {row[0] for row in conn.execute(select)}

    return run


def _duckdb():
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect()
    conn.execute("CREATE TABLE probe (i INTEGER, big BIGINT, f DOUBLE, f4 FLOAT, d DECIMAL(10, 2), ts TIMESTAMP, tstz TIMESTAMPTZ, s VARCHAR)")
    conn.executemany(f"INSERT INTO probe ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ROWS)
    return lambda script: {row[0] for row in conn.execute(script).fetchall()}


@pytest.mark.parametrize("method", EAGER_METHODS)
@pytest.mark.parametrize("case", list(CASES))
@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
def test_restriction_selects_the_rows_of_the_keys(dialect, case, method):
    column, keys, expected = CASES[case]
    if dialect == "sqlite" and case in ("float_nan", "timestamptz"):
        pytest.skip("SQLite stores NaN as NULL and has no time zones")
    run = _sqlite() if dialect == "sqlite" else _duckdb()
    _check(run, keys, column, method, expected, dialect)


def _check(run, keys, column, method, expected, dialect="postgres"):
    query = "SELECT p.i FROM probe AS p"
    if method != "IN" and any(isinstance(k, float) for k in keys):
        with pytest.raises(ValueError, match="use method IN"):
            extend_query_with_partition_keys(query, keys, column, method=method, p0_alias="p", sql_dialect=dialect)
        return
    assert run(extend_query_with_partition_keys(query, keys, column, method=method, p0_alias="p", sql_dialect=dialect)) == expected


class TestPostgreSQL:
    @pytest.fixture(scope="class")
    def run(self):
        psycopg = pytest.importorskip("psycopg")
        if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
            pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
        from partitioncache.db_handler.postgres import fetch_final_result_set

        params = {
            "host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
            "port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
            "user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
            "password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
            "dbname": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
        }
        table = f"typed_keys_{uuid.uuid4().hex[:8]}"
        conn = psycopg.connect(**params, autocommit=True)
        conn.execute(f"CREATE TABLE {table} (i int, big bigint, f double precision, f4 real, d numeric(10, 2), ts timestamp, tstz timestamptz, s text)")
        with conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {table} ({COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", ROWS)

        def run(script: str) -> set:
            with conn.cursor() as cur:
                cur.execute(script.replace("probe", table))
                return {row[0] for row in fetch_final_result_set(cur)}

        yield run
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()

    @pytest.mark.parametrize("method", EAGER_METHODS + UNLOGGED_METHODS)
    @pytest.mark.parametrize("case", list(CASES))
    def test_restriction_selects_the_rows_of_the_keys(self, run, case, method):
        column, keys, expected = CASES[case]
        _check(run, keys, column, method, expected)

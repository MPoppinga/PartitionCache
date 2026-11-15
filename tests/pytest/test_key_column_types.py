"""
Key columns whose comparison differs from the keys' Python type: char(n) and real (PostgreSQL).

A char(n) column reads back padded keys ('k1   '); compared as text with the column, they matched no row. A real
column reads back the nearest doubles; compared in double precision, they matched no row. The key tables are built
in the column's type; VALUES and the lazy IN_SUBQUERY, which cannot take it, raise instead of returning no rows.

Runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB and is skipped without them.
"""

import os
import uuid

import pytest

from partitioncache.apply_cache import extend_query_with_partition_keys, extend_query_with_partition_keys_lazy

TABLE_METHODS = ["TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"]
GUARD_MESSAGE = "needs method IN or a key-table method"
# (column, its type, the lazy cache datatype, the SQL array type a cache of that datatype returns)
COLUMNS = {
    "kc": ("char(5)", "text", "text[]"),
    "kr": ("real", "float", "float8[]"),
    "kt": ("text", "text", "text[]"),
    "kf": ("double precision", "float", "float8[]"),
}
QUERY = "SELECT p.id FROM ctk AS p WHERE p.id > 0"
EXPECTED = {1, 2}


@pytest.fixture(scope="module")
def pg():
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
    schema = f"coltype_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(**params, autocommit=True)
    conn.execute(f"CREATE SCHEMA {schema}")
    conn.execute(f"SET search_path TO {schema}")
    conn.execute("CREATE TABLE ctk (id int, kc char(5), kr real, kt text, kf double precision)")
    conn.execute("INSERT INTO ctk SELECT g, 'k' || g, g / 10.0, 't' || g, g / 10.0 FROM generate_series(1, 6) AS g")
    conn.execute("CREATE TABLE ctmeta (kv varchar, kc char(5), prop int)")
    conn.execute("INSERT INTO ctmeta SELECT 'k' || g, 'k' || g, g % 2 FROM generate_series(1, 6) AS g")
    conn.execute("ANALYZE")
    # the keys as the cache stores them: read back from the column (padded char, the double nearest the real)
    keys = {column: {row[0] for row in conn.execute(f"SELECT {column} FROM ctk WHERE id IN (1, 2)")} for column in COLUMNS}

    def run(script):
        with conn.cursor() as cur:
            cur.execute(script)
            return {row[0] for row in fetch_final_result_set(cur)}

    yield run, keys, psycopg
    conn.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


def _lazy_keys(keys: set, column: str) -> str:
    """A lazy cache subquery returning ``keys`` as a cache of the column's datatype does (text or float8)."""
    array_type = COLUMNS[column][2]
    literals = ", ".join("'" + str(k).replace("'", "''") + "'" for k in keys)
    return f"SELECT unnest(CAST(ARRAY[{literals}] AS {array_type})) AS {column}"


@pytest.mark.parametrize("method", ["IN", *TABLE_METHODS])
@pytest.mark.parametrize("column", list(COLUMNS))
def test_eager_keys_select_their_rows(pg, column, method):
    run, keys, _ = pg
    assert run(extend_query_with_partition_keys(QUERY, keys[column], column, method=method, p0_alias="p")) == EXPECTED


@pytest.mark.parametrize("method", TABLE_METHODS)
@pytest.mark.parametrize("column", list(COLUMNS))
def test_lazy_keys_select_their_rows(pg, column, method):
    run, keys, _ = pg
    datatype = COLUMNS[column][1]
    script = extend_query_with_partition_keys_lazy(QUERY, _lazy_keys(keys[column], column), column, method=method, p0_alias="p", partition_datatype=datatype)
    assert run(script) == EXPECTED


def test_values_raises_for_a_char_column_and_not_for_text(pg):
    run, keys, psycopg = pg
    with pytest.raises(psycopg.errors.InvalidTextRepresentation, match=GUARD_MESSAGE):
        run(extend_query_with_partition_keys(QUERY, keys["kc"], "kc", method="VALUES", p0_alias="p"))
    assert run(extend_query_with_partition_keys(QUERY, keys["kt"], "kt", method="VALUES", p0_alias="p")) == EXPECTED


@pytest.mark.parametrize(("column", "raises"), [("kc", True), ("kr", True), ("kt", False), ("kf", False)])
def test_lazy_in_subquery_raises_for_char_and_real_columns_only(pg, column, raises):
    run, keys, psycopg = pg
    script = extend_query_with_partition_keys_lazy(QUERY, _lazy_keys(keys[column], column), column, method="IN_SUBQUERY", p0_alias="p", partition_datatype=COLUMNS[column][1])
    if raises:
        with pytest.raises(psycopg.errors.InvalidTextRepresentation, match=GUARD_MESSAGE):
            run(script)
    else:
        assert run(script) == EXPECTED


def test_the_guard_does_not_fire_on_an_empty_scan(pg):
    run, keys, _ = pg
    query = "SELECT p.id FROM ctk AS p WHERE p.id > 100"
    assert run(extend_query_with_partition_keys(query, keys["kc"], "kc", method="VALUES", p0_alias="p")) == set()


@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
@pytest.mark.parametrize("method", TABLE_METHODS)
@pytest.mark.parametrize("meta_column", ["kv", "kc"])
def test_folding_compares_in_the_column_type(pg, method, lazy, meta_column):
    """Folded with the caller's word for text keys: the keys are compared in the char(n) column's own type."""
    run, keys, _ = pg
    query = f"SELECT p.id FROM ctk AS p WHERE p.kc IN (SELECT m.{meta_column} FROM ctmeta AS m WHERE m.prop = 1)"
    kwargs = {"method": method, "p0_alias": "p", "fold_key_subqueries": "any_key_type"}
    if lazy:
        script = extend_query_with_partition_keys_lazy(query, _lazy_keys(keys["kc"], "kc"), "kc", partition_datatype="text", **kwargs)
    else:
        script = extend_query_with_partition_keys(query, keys["kc"], "kc", **kwargs)
    assert "pcache_folded_1" in script and "pcache_stage_" in script
    assert run(script) == {1}  # key 1 has prop 1, key 2 has prop 0

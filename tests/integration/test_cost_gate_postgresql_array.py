"""PostgresDBHandler.estimate_cost against a real PostgreSQL: one statement only, caller's transaction untouched.

The module name contains postgresql_array so the CI job of that backend selects it (-k postgresql_array).
"""

import os

import psycopg
import pytest

from partitioncache.db_handler.postgres import PostgresDBHandler

CONN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "integration_user"),
    "password": os.getenv("PG_PASSWORD", "test_password"),
    "dbname": os.getenv("PG_DBNAME", "partitioncache_integration"),
}


@pytest.fixture
def handler():
    try:
        h = PostgresDBHandler(CONN["host"], CONN["port"], CONN["user"], CONN["password"], CONN["dbname"])
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    yield h
    h.conn.rollback()
    h.conn.execute("DROP TABLE IF EXISTS pc_cost_gate_probe")
    h.conn.commit()
    h.close()


def _table_exists(handler, name):
    with psycopg.connect(**CONN) as other:
        return other.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()[0]


def test_returns_a_cost_and_handles_percent_signs(handler):
    assert handler.estimate_cost("SELECT 'a%b' LIKE '%b'") > 0


def test_several_statements_are_rejected_and_not_run(handler):
    with pytest.raises(psycopg.Error):
        handler.estimate_cost("SELECT 1; CREATE TABLE pc_cost_gate_probe (a int)")
    assert not _table_exists(handler, "pc_cost_gate_probe")


def test_inside_a_transaction_block(handler):
    with handler.conn.transaction():
        handler.conn.execute("CREATE TEMP TABLE pc_cost_gate_tmp (a int)")
        with pytest.raises(psycopg.Error):
            handler.estimate_cost("SELECT * FROM pc_cost_gate_no_such_table")
        # The failed EXPLAIN only rolled back its savepoint; the caller's transaction goes on
        assert handler.conn.execute("SELECT count(*) FROM pc_cost_gate_tmp").fetchone()[0] == 0
        assert handler.estimate_cost("SELECT 1") > 0


def test_open_transaction_is_not_committed(handler):
    handler.conn.execute("CREATE TEMP TABLE pc_cost_gate_on_commit (a int) ON COMMIT DROP")
    handler.estimate_cost("SELECT 1")
    assert handler.conn.execute("SELECT to_regclass('pc_cost_gate_on_commit') IS NOT NULL").fetchone()[0]

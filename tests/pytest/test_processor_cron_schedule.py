"""
The queue processor's frequency is turned into a pg_cron schedule. pg_cron takes '[1-59] seconds' or a five-field
cron expression whose step stays within its field; any other frequency is rejected rather than changed.

The SQL cases run with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB; where pg_cron is installed they also
schedule every produced string once.
"""

import argparse
import os
import re

import pytest

from partitioncache.cli.postgresql_queue_processor import SQL_CRON_FILE, _parse_frequency_seconds_arg, processor_cron_schedule

VALID = {
    1: "1 seconds",
    30: "30 seconds",
    59: "59 seconds",
    60: "*/1 * * * *",
    300: "*/5 * * * *",
    3540: "*/59 * * * *",
    3600: "0 */1 * * *",
    7200: "0 */2 * * *",
    82800: "0 */23 * * *",
    86400: "0 0 * * *",
}
INVALID = [0, -5, 61, 90, 119, 3601, 3660, 5400, 86399, 90000, 172800]


@pytest.mark.parametrize(("seconds", "schedule"), VALID.items())
def test_schedulable_frequencies(seconds, schedule):
    assert processor_cron_schedule(seconds) == schedule


@pytest.mark.parametrize("seconds", INVALID)
def test_frequencies_pg_cron_cannot_schedule_are_rejected(seconds):
    with pytest.raises(ValueError, match="cannot be scheduled with pg_cron"):
        processor_cron_schedule(seconds)


@pytest.mark.parametrize(("value", "seconds"), [("30s", 30), ("5m", 300), ("2h", 7200), ("1d", 86400)])
def test_cli_accepts_schedulable_durations(value, seconds):
    assert _parse_frequency_seconds_arg(value) == seconds


@pytest.mark.parametrize("value", ["90", "90s", "61m", "2d"])
def test_cli_rejects_durations_pg_cron_cannot_schedule(value):
    with pytest.raises(argparse.ArgumentTypeError, match="cannot be scheduled with pg_cron"):
        _parse_frequency_seconds_arg(value)


@pytest.fixture
def pg_conn():
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    import psycopg

    conn = psycopg.connect(
        dbname=os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
        host=os.getenv("PARTITIONCACHE_TEST_PG_HOST"),
        user=os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        password=os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        port=int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
    )
    # Only this function of the cron SQL file, as a temporary function: the rest needs the pg_cron database
    definition = re.search(
        r"CREATE OR REPLACE FUNCTION partitioncache_processor_cron_schedule\(.*?\$\$ LANGUAGE plpgsql IMMUTABLE;", SQL_CRON_FILE.read_text(), re.S
    )
    assert definition is not None
    with conn.cursor() as cur:
        cur.execute(definition.group(0).replace("FUNCTION partitioncache_processor_cron_schedule", "FUNCTION pg_temp.partitioncache_processor_cron_schedule", 1))
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def _sql_schedule(conn, seconds):
    import psycopg

    with conn.cursor() as cur:
        cur.execute("SAVEPOINT s")
        try:
            cur.execute("SELECT pg_temp.partitioncache_processor_cron_schedule(%s)", (seconds,))
            row = cur.fetchone()
            return row[0] if row else None
        except psycopg.errors.InvalidParameterValue:
            cur.execute("ROLLBACK TO SAVEPOINT s")
            return None


def test_sql_and_python_schedule_the_same_frequencies(pg_conn):
    for seconds in [*range(-1, 3700), *range(3600, 90001, 60)]:
        try:
            expected = processor_cron_schedule(seconds)
        except ValueError:
            expected = None
        assert _sql_schedule(pg_conn, seconds) == expected, seconds


def _require_pg_cron(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
        if cur.fetchone() is None:
            if os.getenv("PARTITIONCACHE_TEST_REQUIRE_PG_CRON"):
                pytest.fail("PARTITIONCACHE_TEST_REQUIRE_PG_CRON is set but pg_cron is not installed in this database")
            pytest.skip("pg_cron is not installed in this database")


def test_pg_cron_accepts_every_produced_schedule(pg_conn):
    _require_pg_cron(pg_conn)
    with pg_conn.cursor() as cur:
        schedules = sorted({processor_cron_schedule(s) for s in [*range(1, 60), *range(60, 3600, 60), *range(3600, 86401, 3600)]})
        for schedule in schedules:
            cur.execute("SELECT cron.unschedule(cron.schedule('partitioncache_schedule_probe', %s, 'SELECT 1'))", (schedule,))
            assert cur.fetchone() == (True,), schedule


def test_config_trigger_rejects_or_schedules_the_frequency(pg_conn):
    """The config trigger refuses a frequency pg_cron cannot schedule and schedules a whole hour as an hour step."""
    import uuid

    import psycopg

    _require_pg_cron(pg_conn)
    queue_prefix = f"cron_schedule_{uuid.uuid4().hex[:8]}"
    config_table = f"{queue_prefix}_processor_config"
    insert = (
        f"INSERT INTO {config_table} (job_name, enabled, max_parallel_jobs, frequency_seconds, table_prefix, queue_prefix, cache_backend, target_database) "
        f"VALUES (%s, false, 1, %s, 'unused', '{queue_prefix}', 'array', current_database())"
    )
    with pg_conn.cursor() as cur:
        cur.execute(SQL_CRON_FILE.read_text())
        cur.execute("SELECT partitioncache_initialize_cron_config_table(%s)", (queue_prefix,))
        cur.execute("SELECT partitioncache_create_cron_config_trigger(%s)", (queue_prefix,))
        cur.execute("SAVEPOINT before_insert")
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="cannot be scheduled with pg_cron"):
            cur.execute(insert, (f"{queue_prefix}_every_90s", 90))
        cur.execute("ROLLBACK TO SAVEPOINT before_insert")

        cur.execute(insert, (f"{queue_prefix}_hourly", 3600))
        cur.execute(f"SELECT (SELECT array_agg(schedule) FROM cron.job WHERE jobid = ANY(c.job_ids)) FROM {config_table} c")
        assert cur.fetchone() == (["0 */1 * * *"],)
        cur.execute(f"DELETE FROM {config_table}")  # unschedules the job
    # pg_conn is rolled back by the fixture, which also drops the table, the trigger and the job

"""The bit caches share one bitsize trigger function, so dropping a cache's tables leaves no function behind.

Runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import os
import time
import uuid

import pytest

# The per-prefix trigger function as created before the shared one (v2: grows with USING, refuses to shrink)
OLD_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION "{function}"() RETURNS TRIGGER AS $t$
DECLARE v_cache_table TEXT;
BEGIN
    -- partitioncache bitsize trigger v2: grows with USING, refuses to shrink
    IF TG_OP = 'UPDATE' AND NEW.bitsize < OLD.bitsize THEN
        RAISE EXCEPTION 'cannot shrink';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.bitsize != NEW.bitsize THEN
        v_cache_table := '{prefix}_cache_' || NEW.partition_key;
        EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS partition_keys_count', v_cache_table);
        EXECUTE format('ALTER TABLE %I ALTER COLUMN partition_keys TYPE BIT(%s) USING partition_keys::BIT(%s)', v_cache_table, NEW.bitsize, NEW.bitsize);
        EXECUTE format('ALTER TABLE %I ADD COLUMN partition_keys_count INTEGER GENERATED ALWAYS AS '
                       '(length(replace(partition_keys::text, ''0'', ''''))) STORED', v_cache_table);
    END IF;
    RETURN NEW;
END;
$t$ LANGUAGE plpgsql
"""

# 41 + 9 = 50 characters: the trigger and metadata table names exceed 63 bytes and are stored truncated, the cache
# and queries tables are not
LONG = "pcache_shared_trigger_long_prefix_of_50ch"


def _params() -> dict:
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        "user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "dbname": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
    }


def _query(statement: str, params: tuple = ()) -> list[tuple]:
    import psycopg

    with psycopg.connect(**_params(), autocommit=True) as conn:
        cursor = conn.execute(statement, params or None)  # type: ignore[arg-type]
        return cursor.fetchall() if cursor.description else []


def _handler(prefix: str, bitsize: int = 16):
    from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

    params = _params()
    return PostgreSQLBitCacheHandler(
        db_name=params["dbname"],
        db_host=params["host"],
        db_user=params["user"],
        db_password=params["password"],
        db_port=params["port"],
        db_tableprefix=prefix,
        bitsize=bitsize,
    )


def _drop_cache(prefix: str) -> None:
    """Drop the prefix's tables (names as stored: truncated to 63 bytes)."""
    tables = _query(
        """SELECT tablename FROM pg_tables WHERE schemaname = 'public'
           AND (tablename IN ((%s || '_partition_metadata')::name, (%s || '_queries')::name)
                OR starts_with(tablename::text, (%s || '_cache_')::name::text))""",
        (prefix, prefix, prefix),
    )
    for (table,) in tables:
        _query(f'DROP TABLE IF EXISTS "{table}" CASCADE')


def _functions_of(prefix: str) -> list[str]:
    """The prefix's own trigger function, as stored (truncated to 63 bytes); the shared function replaces it."""
    return [row[0] for row in _query("SELECT proname::text FROM pg_proc WHERE proname = (%s || '_bitsize_trigger_func')::name", (prefix,))]


def _trigger(prefix: str) -> tuple[int, str, bytes] | None:
    """(trigger oid, called function, trigger arguments) of the prefix's bitsize trigger."""
    rows = _query(
        "SELECT oid::int, tgfoid::regproc::text, tgargs FROM pg_trigger WHERE tgname = %s AND tgrelid = to_regclass(quote_ident(%s)) AND NOT tgisinternal",
        (prefix + "_bitsize_trigger", prefix + "_partition_metadata"),
    )
    return rows[0] if rows else None  # type: ignore[return-value]


def _install_old_trigger(prefix: str, function: str | None = None) -> str:
    """Give the prefix the per-prefix trigger function and trigger it had before the shared function."""
    function = function or prefix + "_bitsize_trigger_func"
    _query(OLD_TRIGGER_FUNCTION.format(function=function, prefix=prefix))
    _query(f'DROP TRIGGER IF EXISTS "{prefix}_bitsize_trigger" ON "{prefix}_partition_metadata"')
    _query(f'CREATE TRIGGER "{prefix}_bitsize_trigger" AFTER UPDATE ON "{prefix}_partition_metadata" FOR EACH ROW EXECUTE FUNCTION "{function}"()')
    return function


@pytest.fixture(params=["short", "long"])
def prefix(request):
    """A fresh prefix; the long one (50 characters) gives truncated trigger, metadata table and function names."""
    suffix = uuid.uuid4().hex[:8]
    name = f"{LONG}_{suffix}" if request.param == "long" else f"pcache_shared_{suffix}"
    yield name
    _drop_cache(name)
    for function in _functions_of(name):
        _query(f'DROP FUNCTION IF EXISTS "{function}"() CASCADE')


def _store_and_grow(handler) -> None:
    handler.register_partition_key("pk", "integer")
    assert handler.set_cache("small", {1, 2}, "pk") is True
    assert handler.set_cache("large", {3, 40}, "pk") is True  # beyond bitsize 16: the trigger widens the table
    assert handler.get("large", "pk") == {3, 40}
    assert handler.get("small", "pk") == {1, 2}


def _old_cache(prefix: str) -> str:
    """A cache whose trigger calls the prefix's own function, as created before the shared function; returns its name."""
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
        assert handler.set_cache("small", {1, 2}, "pk") is True
    finally:
        handler.close()
    _install_old_trigger(prefix)
    old = _functions_of(prefix)[0]  # as stored, truncated for the long prefix
    assert _trigger(prefix)[1].strip('"') == old  # type: ignore[index]
    return old


def test_new_cache_uses_the_shared_function_and_grows(prefix):
    handler = _handler(prefix)
    try:
        _store_and_grow(handler)
    finally:
        handler.close()

    _, function, args = _trigger(prefix)  # type: ignore[misc]
    assert function == "partitioncache_bit_widen_trigger"
    assert args == prefix.encode() + b"\x00"
    assert _functions_of(prefix) == []


def test_dropping_a_cache_leaves_no_function_and_the_others_keep_growing(prefix):
    other = f"pcache_shared_other_{uuid.uuid4().hex[:8]}"
    first, second = _handler(prefix), _handler(other)
    try:
        first.register_partition_key("pk", "integer")
        second.register_partition_key("pk", "integer")
        assert first.set_cache("q", {1}, "pk") is True
    finally:
        first.close()
    _drop_cache(prefix)

    assert _functions_of(prefix) == []
    try:
        assert second.set_cache("large", {3, 40}, "pk") is True
        assert second.get("large", "pk") == {3, 40}
    finally:
        second.close()
        _drop_cache(other)


def test_a_cache_from_before_is_switched_when_its_handler_starts(prefix):
    _old_cache(prefix)

    handler = _handler(prefix)
    try:
        assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
        assert _functions_of(prefix) == []
        assert handler.set_cache("large", {3, 40}, "pk") is True
        assert handler.get("large", "pk") == {3, 40}
        assert handler.get("small", "pk") == {1, 2}
    finally:
        handler.close()


def test_the_processor_bootstrap_keeps_a_trigger_from_before(prefix):
    """Inside a job the switch would hold an exclusive lock on the metadata table until the job commits: no switch."""
    old = _old_cache(prefix)

    _query("SELECT partitioncache_bootstrap_partition(%s, 'pk', 'integer', 'bit', 64)", (prefix,))

    assert _trigger(prefix)[1].strip('"') == old  # type: ignore[index]
    assert _functions_of(prefix) == [old]
    assert _query(f'SELECT bitsize FROM "{prefix}_partition_metadata"') == [(64,)]  # grown through the old trigger


def test_the_switch_gives_up_on_a_busy_table_and_the_next_start_switches(prefix):
    """An open transaction that has read the metadata table: the handler starts anyway, with the old trigger."""
    import psycopg

    old = _old_cache(prefix)
    busy = psycopg.connect(**_params())
    try:
        busy.execute(f'SELECT * FROM "{prefix}_partition_metadata"')  # holds its lock until the transaction ends

        started = time.monotonic()
        handler = _handler(prefix)
        try:
            assert time.monotonic() - started < 10
            assert _trigger(prefix)[1].strip('"') == old  # type: ignore[index]
            with psycopg.connect(**_params(), autocommit=True) as reader:
                reader.execute("SET lock_timeout = '1s'")
                assert reader.execute(f'SELECT count(*) FROM "{prefix}_partition_metadata"').fetchone() == (1,)
            assert handler.set_cache("large", {3, 40}, "pk") is True  # the old trigger still widens the table
            assert handler.get("large", "pk") == {3, 40}
        finally:
            handler.close()
    finally:
        busy.rollback()
        busy.close()

    _handler(prefix).close()
    assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
    assert _functions_of(prefix) == []


def test_an_own_function_an_older_client_re_created_is_dropped_at_the_next_start(prefix):
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
    finally:
        handler.close()
    _query(OLD_TRIGGER_FUNCTION.format(function=prefix + "_bitsize_trigger_func", prefix=prefix))
    assert len(_functions_of(prefix)) == 1

    _handler(prefix).close()

    assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
    assert _functions_of(prefix) == []


def test_an_old_function_another_trigger_still_uses_is_kept(prefix):
    old = _old_cache(prefix)
    _query(f'CREATE TABLE "{prefix[:40]}_other" (partition_key TEXT, bitsize INT)')
    _query(f'CREATE TRIGGER "{prefix[:40]}_other_trigger" AFTER UPDATE ON "{prefix[:40]}_other" FOR EACH ROW EXECUTE FUNCTION "{old}"()')
    try:
        _query("SELECT partitioncache_switch_bitsize_trigger(%s)", (prefix,))

        assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
        assert _functions_of(prefix) == [old]
    finally:
        _query(f'DROP TABLE IF EXISTS "{prefix[:40]}_other"')


def test_a_current_trigger_is_left_alone(prefix):
    """A second call finds the trigger current (also under truncated names) and does no DDL."""
    handler = _handler(prefix)
    try:
        _store_and_grow(handler)
        oid = _trigger(prefix)[0]  # type: ignore[index]
        _query("SELECT partitioncache_create_bitsize_trigger(%s)", (prefix,))
        _query("SELECT partitioncache_switch_bitsize_trigger(%s)", (prefix,))
        _handler(prefix).close()
        assert _trigger(prefix)[0] == oid  # type: ignore[index]
    finally:
        handler.close()

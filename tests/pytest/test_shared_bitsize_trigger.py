"""The bit caches share one bitsize trigger function, so dropping a cache's tables leaves no function behind.

Runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import os
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


@pytest.fixture
def prefix():
    name = f"pcache_shared_{uuid.uuid4().hex[:8]}"
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


def test_new_cache_uses_the_shared_function_with_its_prefix(prefix):
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
    other = prefix + "_b"
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
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
        assert handler.set_cache("small", {1, 2}, "pk") is True
    finally:
        handler.close()
    old = _install_old_trigger(prefix)
    assert _trigger(prefix)[1] == old  # type: ignore[index]

    handler = _handler(prefix)
    try:
        assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
        assert _functions_of(prefix) == []
        assert handler.set_cache("large", {3, 40}, "pk") is True
        assert handler.get("large", "pk") == {3, 40}
        assert handler.get("small", "pk") == {1, 2}
    finally:
        handler.close()


def test_a_cache_from_before_is_switched_by_the_bootstrap_of_the_processor(prefix):
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
    finally:
        handler.close()
    _install_old_trigger(prefix)

    # The in-database processor bootstraps each partition it fills; for a bit cache that sets up the trigger
    _query("SELECT partitioncache_bootstrap_partition(%s, 'pk', 'integer', 'bit', 64)", (prefix,))

    assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
    assert _functions_of(prefix) == []
    assert _query(f'SELECT bitsize FROM "{prefix}_partition_metadata"') == [(64,)]


def test_an_old_function_another_trigger_still_uses_is_kept(prefix):
    handler = _handler(prefix)
    try:
        handler.register_partition_key("pk", "integer")
    finally:
        handler.close()
    old = _install_old_trigger(prefix)
    _query(f'CREATE TABLE "{prefix}_other" (partition_key TEXT, bitsize INT)')
    _query(f'CREATE TRIGGER "{prefix}_other_trigger" AFTER UPDATE ON "{prefix}_other" FOR EACH ROW EXECUTE FUNCTION "{old}"()')

    try:
        _query("SELECT partitioncache_create_bitsize_trigger(%s)", (prefix,))

        assert _trigger(prefix)[1] == "partitioncache_bit_widen_trigger"  # type: ignore[index]
        assert _functions_of(prefix) == [old]
    finally:
        _query(f'DROP TABLE IF EXISTS "{prefix}_other"')


@pytest.mark.parametrize("long_prefix", [False, True])
def test_a_current_trigger_is_left_alone(prefix, long_prefix):
    """A second call finds the trigger current (also under a truncated name) and does no DDL."""
    name = LONG + "_" + prefix[-8:] if long_prefix else prefix
    assert len(name) == (50 if long_prefix else len(prefix))
    handler = _handler(name)
    try:
        _store_and_grow(handler)
        oid = _trigger(name)[0]  # type: ignore[index]
        _query("SELECT partitioncache_create_bitsize_trigger(%s)", (name,))
        assert _trigger(name)[0] == oid
    finally:
        handler.close()
        _drop_cache(name)
        for function in _functions_of(name):
            _query(f'DROP FUNCTION IF EXISTS "{function}"() CASCADE')

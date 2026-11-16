"""Size priority in the fragment queue: the smaller fragments of all queued queries are popped first."""

import os
import uuid
from unittest.mock import MagicMock, patch

import pytest

from partitioncache.queue_handler.postgresql import PostgreSQLQueueHandler
from partitioncache.queue_handler.size_priority import SIZE_PRIORITY_ENV, fragment_size_priority, size_priority_enabled


def _fragment(tables: int, conditions: int = 1) -> str:
    """A fragment joining `tables` aliases of one table, with `conditions` conditions on the first."""
    aliases = [f"t{i}" for i in range(1, tables + 1)]
    joins = [f"{alias}.region_id = t1.region_id" for alias in aliases[1:]]
    own = [f"t1.x > {n}" for n in range(conditions)]
    return f"SELECT DISTINCT t1.region_id FROM {', '.join(f'readings AS {alias}' for alias in aliases)} WHERE {' AND '.join(joins + own)}"


class TestPriorityValue:
    def test_size_decides(self):
        assert fragment_size_priority(_fragment(1)) == 9001
        assert fragment_size_priority(_fragment(3)) == 7001
        assert fragment_size_priority(_fragment(10)) == 1

    def test_more_than_ten_tables_is_negative_and_still_ordered(self):
        assert fragment_size_priority(_fragment(11)) == -999
        assert fragment_size_priority(_fragment(12)) == -1999
        assert fragment_size_priority(_fragment(12)) < fragment_size_priority(_fragment(11)) < fragment_size_priority(_fragment(10))

    def test_conditions_and_subquery_tables_do_not_count(self):
        assert fragment_size_priority(_fragment(2, conditions=5)) == fragment_size_priority(_fragment(2, conditions=1))
        with_subquery = "SELECT DISTINCT t1.region_id FROM readings AS t1 WHERE t1.region_id IN (SELECT r.region_id FROM regions AS r, stores AS s)"
        assert fragment_size_priority(with_subquery) == 9001

    def test_not_a_select_keeps_priority_one(self):
        assert fragment_size_priority("SELECT a FROM x UNION SELECT a FROM y") == 1
        assert fragment_size_priority("this is not sql (") == 1

    def test_repeats_stay_below_one_size_band(self):
        # 998 re-pushes of a 3-table fragment stay below a new 2-table fragment
        assert fragment_size_priority(_fragment(3)) + 998 < fragment_size_priority(_fragment(2))


class TestSwitch:
    def test_default_is_on(self, monkeypatch):
        monkeypatch.delenv(SIZE_PRIORITY_ENV, raising=False)
        assert size_priority_enabled() is True

    @pytest.mark.parametrize("value,expected", [("false", False), ("0", False), ("no", False), ("true", True), ("1", True), ("YES", True)])
    def test_environment(self, monkeypatch, value, expected):
        monkeypatch.setenv(SIZE_PRIORITY_ENV, value)
        assert size_priority_enabled() is expected

    def test_explicit_setting_wins(self, monkeypatch):
        monkeypatch.setenv(SIZE_PRIORITY_ENV, "false")
        assert size_priority_enabled(True) is True
        monkeypatch.setenv(SIZE_PRIORITY_ENV, "true")
        assert size_priority_enabled(False) is False


def _mocked_handler():
    cursor = MagicMock()
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor
    handler = PostgreSQLQueueHandler.__new__(PostgreSQLQueueHandler)
    handler.fragment_queue_table = "test_query_fragment_queue"
    handler.original_queue_table = "test_original_query_queue"
    handler._get_connection = MagicMock(return_value=connection)  # type: ignore[method-assign]
    return handler, connection, cursor


class TestPushStatements:
    PAIRS = [(_fragment(3), "h3"), (_fragment(1), "h1"), (_fragment(2), "h2")]

    def test_off_runs_only_the_batch_with_priority_one(self):
        handler, connection, cursor = _mocked_handler()
        cursor.fetchall.return_value = [(1, "h3", "pk", "inserted"), (2, "h1", "pk", "inserted"), (3, "h2", "pk", "inserted")]

        assert handler.push_to_query_fragment_queue(self.PAIRS, "pk", "integer", size_priority=False)

        assert cursor.execute.call_count == 1
        statement, params = cursor.execute.call_args.args
        assert "non_blocking_fragment_queue_batch_upsert" in statement
        assert params[4] == [1, 1, 1]
        connection.commit.assert_called_once_with()

    def test_on_lifts_only_the_fragments_this_push_created(self):
        handler, _, cursor = _mocked_handler()
        cursor.fetchall.return_value = [(1, "h3", "pk", "inserted"), (2, "h1", "pk", "updated"), (3, "h2", "pk", "skipped_locked")]

        assert handler.push_to_query_fragment_queue(self.PAIRS, "pk", "integer", size_priority=True)

        batch, lift = cursor.execute.call_args_list
        assert batch.args[1][4] == [1, 1, 1], "the batch itself is the same as without size priority"
        statement, params = lift.args
        assert "priority = q.priority - 1 + s.priority" in statement.as_string(None)
        assert "q.created_at = CURRENT_TIMESTAMP" in statement.as_string(None)
        assert params == (["h3"], [7001], "pk")

    def test_on_is_the_default(self, monkeypatch):
        monkeypatch.delenv(SIZE_PRIORITY_ENV, raising=False)
        handler, _, cursor = _mocked_handler()
        cursor.fetchall.return_value = [(1, "h1", "pk", "inserted")]

        assert handler.push_to_query_fragment_queue([(_fragment(1), "h1")], "pk", "integer")

        assert cursor.execute.call_count == 2
        assert cursor.execute.call_args.args[1] == (["h1"], [9001], "pk")

    def test_environment_turns_it_off(self, monkeypatch):
        monkeypatch.setenv(SIZE_PRIORITY_ENV, "false")
        handler, _, cursor = _mocked_handler()
        cursor.fetchall.return_value = [(1, "h1", "pk", "inserted")]

        assert handler.push_to_query_fragment_queue([(_fragment(1), "h1")], "pk", "integer")

        assert cursor.execute.call_count == 1

    @pytest.mark.parametrize("size_priority,expected", [(True, [7001, 9001]), (False, [1, 1])])
    def test_fallback_inserts_with_the_size_priority_and_adds_one_on_conflict(self, size_priority, expected):
        handler, _, cursor = _mocked_handler()
        cursor.execute.side_effect = [RuntimeError("helper unavailable"), None, None]

        assert handler.push_to_query_fragment_queue(self.PAIRS[:2], "pk", "integer", size_priority=size_priority)

        inserts = cursor.execute.call_args_list[1:]
        assert [call.args[1][5] for call in inserts] == expected
        assert all('priority = "test_query_fragment_queue".priority + 1' in call.args[0].as_string(None) for call in inserts)

    def test_explicit_priority_is_used_as_given(self):
        handler, _, cursor = _mocked_handler()
        cursor.fetchall.return_value = [(1, "h3", "pk", "inserted")]

        assert handler.push_to_query_fragment_queue_with_priority([(_fragment(3), "h3")], "pk", 5, "integer")

        assert cursor.execute.call_count == 1
        assert cursor.execute.call_args.args[1][4] == [5]


class TestPublicPush:
    @patch("partitioncache.queue._get_queue_handler")
    def test_unset_keeps_the_handler_call_unchanged(self, get_handler):
        from partitioncache.queue import push_to_query_fragment_queue

        handler = get_handler.return_value
        push_to_query_fragment_queue([("SELECT 1", "h")], "pk", "integer")
        handler.push_to_query_fragment_queue.assert_called_once_with([("SELECT 1", "h")], "pk", "integer", None)

    @patch("partitioncache.queue._get_queue_handler")
    def test_explicit_setting_is_passed_on(self, get_handler):
        from partitioncache.queue import push_to_query_fragment_queue

        handler = get_handler.return_value
        push_to_query_fragment_queue([("SELECT 1", "h")], "pk", "integer", size_priority=False)
        handler.push_to_query_fragment_queue.assert_called_once_with([("SELECT 1", "h")], "pk", "integer", None, size_priority=False)

    def test_redis_accepts_and_ignores_it(self):
        from partitioncache.queue_handler.redis import RedisQueueHandler

        handler = RedisQueueHandler.__new__(RedisQueueHandler)
        connection = MagicMock()
        handler._get_redis_connection = MagicMock(return_value=connection)  # type: ignore[method-assign]
        handler._get_queue_key = MagicMock(return_value="q")  # type: ignore[method-assign]

        assert handler.push_to_query_fragment_queue([("SELECT 1", "h1"), ("SELECT 2", "h2")], "pk", "integer", size_priority=True)

        pushed = [call.args for call in connection.pipeline.return_value.rpush.call_args_list]
        assert [args[0] for args in pushed] == ["q", "q"]
        assert ['"h1"' in args[1] for args in pushed] == [True, False]


class TestCommandLine:
    def _parse(self, *argv: str):
        import argparse

        from partitioncache.cli.common_args import add_queue_args

        parser = argparse.ArgumentParser()
        add_queue_args(parser)
        return parser.parse_args(list(argv))

    def test_flag(self):
        assert self._parse().size_priority is None
        assert self._parse("--size-priority").size_priority is True
        assert self._parse("--no-size-priority").size_priority is False


def _pg_params() -> dict:
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        "user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "dbname": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
    }


def _query(statement: str) -> list[tuple]:
    import psycopg

    with psycopg.connect(**_pg_params(), autocommit=True) as conn:
        cursor = conn.execute(statement)  # type: ignore[arg-type]
        return cursor.fetchall() if cursor.description else []


class _WithoutHelper:
    """A connection whose cursors fail the batch helper call, as on a database where the helper is missing."""

    def __init__(self, connection):
        self._connection = connection

    def cursor(self, *args, **kwargs):
        return _WithoutHelperCursor(self._connection.cursor(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _WithoutHelperCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, statement, *args, **kwargs):
        if "non_blocking_fragment_queue_batch_upsert" in str(statement):
            raise RuntimeError("helper unavailable")
        return self._cursor.execute(statement, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class TestAgainstPostgreSQL:
    """Pop order of the PostgreSQL fragment queue, with the batch helper and with the fallback statements."""

    @pytest.fixture
    def queue(self):
        handler = PostgreSQLQueueHandler(**_pg_params(), table_prefix=f"pcache_sizeprio_{uuid.uuid4().hex[:8]}")
        yield handler
        handler.close()
        _query(f'DROP TABLE IF EXISTS "{handler.fragment_queue_table}", "{handler.original_queue_table}" CASCADE')

    # Two queries: query A has fragments of 3, 2 and 1 tables, query B (pushed later) of 2 and 1 tables
    QUERY_A = [(_fragment(1), "a1"), (_fragment(2, 2), "a2"), (_fragment(3, 3), "a3")]
    QUERY_B = [(_fragment(1, 4), "b1"), (_fragment(2, 5), "b2")]

    def _pop_all(self, queue) -> list[str]:
        popped = []
        while (entry := queue.pop_from_query_fragment_queue()) is not None:
            popped.append(entry[1])
        return popped

    def _priorities(self, queue) -> dict[str, int]:
        return dict(_query(f'SELECT hash, priority FROM "{queue.fragment_queue_table}"'))  # type: ignore[arg-type]

    @pytest.fixture(params=["helper", "fallback"])
    def push_path(self, request):
        """The batch helper, or the per-row fallback statements used where the helper is missing."""
        if request.param == "helper":
            yield
            return
        original = PostgreSQLQueueHandler._get_connection
        with patch.object(PostgreSQLQueueHandler, "_get_connection", lambda handler: _WithoutHelper(original(handler))):
            yield

    def test_on_pops_the_smaller_fragments_of_both_queries_first(self, queue, push_path):
        assert queue.push_to_query_fragment_queue(self.QUERY_A, "region_id", "integer", size_priority=True)
        assert queue.push_to_query_fragment_queue(self.QUERY_B, "region_id", "integer", size_priority=True)

        assert self._priorities(queue) == {"a1": 9001, "b1": 9001, "a2": 8001, "b2": 8001, "a3": 7001}
        assert self._pop_all(queue) == ["a1", "b1", "a2", "b2", "a3"]

    def test_off_pops_in_push_order(self, queue, push_path):
        assert queue.push_to_query_fragment_queue(self.QUERY_A, "region_id", "integer", size_priority=False)
        assert queue.push_to_query_fragment_queue(self.QUERY_B, "region_id", "integer", size_priority=False)

        assert set(self._priorities(queue).values()) == {1}
        assert self._pop_all(queue) == ["a1", "a2", "a3", "b1", "b2"]

    def test_a_repush_adds_one_and_wins_only_within_its_size(self, queue, push_path):
        assert queue.push_to_query_fragment_queue(self.QUERY_A, "region_id", "integer", size_priority=True)
        assert queue.push_to_query_fragment_queue(self.QUERY_B, "region_id", "integer", size_priority=True)
        # Query B again: its fragments are waiting, so each gets +1
        assert queue.push_to_query_fragment_queue(self.QUERY_B, "region_id", "integer", size_priority=True)

        assert self._priorities(queue) == {"a1": 9001, "b1": 9002, "a2": 8001, "b2": 8002, "a3": 7001}
        assert self._pop_all(queue) == ["b1", "a1", "b2", "a2", "a3"]

    def test_a_repush_within_one_push_adds_one(self, queue, push_path):
        assert queue.push_to_query_fragment_queue([(_fragment(2), "x"), (_fragment(2), "x")], "region_id", "integer", size_priority=True)

        assert self._priorities(queue) == {"x": 8002}

    def test_more_than_ten_tables_pops_last(self, queue, push_path):
        large = [(_fragment(12), "t12"), (_fragment(11), "t11")]
        assert queue.push_to_query_fragment_queue(large + self.QUERY_A, "region_id", "integer", size_priority=True)

        assert self._priorities(queue)["t12"] == -1999
        assert self._pop_all(queue) == ["a1", "a2", "a3", "t11", "t12"]

    def test_explicit_priority_is_kept(self, queue, push_path):
        assert queue.push_to_query_fragment_queue(self.QUERY_A, "region_id", "integer", size_priority=True)
        assert queue.push_to_query_fragment_queue_with_priority([(_fragment(3), "urgent")], "region_id", 20000, "integer")

        assert self._priorities(queue)["urgent"] == 20000
        assert self._pop_all(queue)[0] == "urgent"

    def test_a_fragment_another_session_created_is_not_lifted(self, queue):
        """The helper reports an insert that met another session's committed row through ON CONFLICT as inserted."""
        assert queue.push_to_query_fragment_queue([(_fragment(2), "x")], "region_id", "integer", size_priority=False)

        conn = queue._get_connection()
        queue._lift_created_fragments(conn.cursor(), [(_fragment(2), "x")], "region_id", [(1, "x", "region_id", "inserted")])
        conn.commit()
        conn.close()

        assert self._priorities(queue) == {"x": 1}

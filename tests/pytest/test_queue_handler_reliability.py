"""Queue handlers: blocking pops honour their timeout, a missing datatype pops as None, a push with failed rows is False."""

import json
import logging
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from partitioncache.queue_handler.postgresql import PostgreSQLQueueHandler
from partitioncache.queue_handler.redis import RedisQueueHandler

FRAGMENT_ITEM = ("SELECT DISTINCT t1.pk FROM t AS t1", "h1", "pk", "integer", None)
ORIGINAL_ITEM = ("SELECT * FROM t", "pk", "integer")


def _handler_with_connection():
    cursor = MagicMock()
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor
    handler = PostgreSQLQueueHandler.__new__(PostgreSQLQueueHandler)
    handler.host, handler.port, handler.user, handler.password, handler.dbname = "localhost", 5432, "u", "p", "db"
    handler.fragment_queue_table = "test_query_fragment_queue"
    handler.original_queue_table = "test_original_query_queue"
    handler._get_connection = MagicMock(return_value=connection)  # type: ignore[method-assign]
    return handler, connection, cursor


class _FakePGconn:
    """The libpq level of the LISTEN connection: consume_input reads the socket, notifies returns one notification."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.received: list[str] = []

    def consume_input(self) -> None:
        self.sock.setblocking(False)
        try:
            self.received.extend("notify" for _ in self.sock.recv(1024))
        except BlockingIOError:
            pass

    def notifies(self):
        return self.received.pop() if self.received else None


class _ListenConnection:
    """A LISTEN connection on one end of a socket pair; its notifies() waits like psycopg's without a timeout."""

    def __init__(self):
        self.sock, self.peer = socket.socketpair()
        self.pgconn = _FakePGconn(self.sock)
        self.release = threading.Event()
        self.autocommit = False

    def fileno(self) -> int:
        return self.sock.fileno()

    def cursor(self):
        return MagicMock()

    def notifies(self, **_kwargs):
        self.release.wait(60)
        return iter(())

    def close(self) -> None:
        self.release.set()
        self.sock.close()
        self.peer.close()


@pytest.fixture
def listen_connection():
    conn = _ListenConnection()
    with patch("partitioncache.queue_handler.postgresql.psycopg.connect", return_value=conn):
        yield conn
    conn.close()


def _call_with_deadline(fn, deadline: float):
    """Run fn in a thread; (finished, result, seconds) after at most deadline seconds."""
    outcome: dict = {}

    def run():
        start = time.monotonic()
        outcome["result"] = fn()
        outcome["seconds"] = time.monotonic() - start

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(deadline)
    return not thread.is_alive(), outcome.get("result"), outcome.get("seconds")


BLOCKING_POPS = [
    ("pop_from_query_fragment_queue_blocking", "pop_from_query_fragment_queue", FRAGMENT_ITEM),
    ("pop_from_original_query_queue_blocking", "pop_from_original_query_queue", ORIGINAL_ITEM),
]


class TestBlockingPopTimeout:
    @pytest.mark.parametrize("blocking_pop,pop,_item", BLOCKING_POPS)
    def test_empty_queue_returns_none_after_the_timeout(self, listen_connection, blocking_pop, pop, _item):
        handler, _, _ = _handler_with_connection()
        setattr(handler, pop, MagicMock(return_value=None))

        finished, result, seconds = _call_with_deadline(lambda: getattr(handler, blocking_pop)(timeout=1), deadline=5)

        assert finished, "the blocking pop waited past its timeout"
        assert result is None
        assert 0.9 <= seconds < 3

    @pytest.mark.parametrize("blocking_pop,pop,item", BLOCKING_POPS)
    def test_a_notification_pops_the_new_item(self, listen_connection, blocking_pop, pop, item):
        handler, _, _ = _handler_with_connection()
        setattr(handler, pop, MagicMock(side_effect=[None, item]))
        threading.Timer(0.2, listen_connection.peer.send, args=(b"n",)).start()

        finished, result, seconds = _call_with_deadline(lambda: getattr(handler, blocking_pop)(timeout=4), deadline=6)

        assert finished
        assert result == item
        assert seconds < 3, "the item was popped on the notification, not after the wait"


class TestMissingDatatype:
    @pytest.mark.parametrize("stored", [None, ""])
    def test_postgresql_original_pop(self, stored):
        handler, _, cursor = _handler_with_connection()
        cursor.fetchone.return_value = (1, "SELECT 1", "pk", stored)

        assert handler.pop_from_original_query_queue() == ("SELECT 1", "pk", None)

    @pytest.mark.parametrize("stored", [None, ""])
    def test_postgresql_fragment_pop(self, stored):
        handler, _, cursor = _handler_with_connection()
        cursor.fetchone.return_value = (1, "SELECT 1", "h1", "pk", stored, None)

        assert handler.pop_from_query_fragment_queue() == ("SELECT 1", "h1", "pk", None, None)

    def test_postgresql_pop_keeps_a_given_datatype(self):
        handler, _, cursor = _handler_with_connection()
        cursor.fetchone.return_value = (1, "SELECT 1", "h1", "pk", "text", None)

        assert handler.pop_from_query_fragment_queue() == ("SELECT 1", "h1", "pk", "text", None)

    @pytest.mark.parametrize("payload", [{"partition_datatype": None}, {}, {"partition_datatype": ""}])
    @pytest.mark.parametrize("blocking", [False, True])
    def test_redis_pops(self, payload, blocking):
        handler = RedisQueueHandler("localhost", 6379, 0)
        client = MagicMock()
        handler._redis_client = client
        original = json.dumps({"query": "SELECT 1", "partition_key": "pk", **payload}).encode()
        fragment = json.dumps({"query": "SELECT 1", "hash": "h1", "partition_key": "pk", "cache_backend": None, **payload}).encode()
        client.blpop.side_effect = [(b"k", original), (b"k", fragment)]

        if blocking:
            popped = (handler.pop_from_original_query_queue_blocking(timeout=1), handler.pop_from_query_fragment_queue_blocking(timeout=1))
        else:
            popped = (handler.pop_from_original_query_queue(), handler.pop_from_query_fragment_queue())

        assert popped == (("SELECT 1", "pk", None), ("SELECT 1", "h1", "pk", None, None))


PAIRS = [("SELECT 1", "h1"), ("SELECT 2", "h2"), ("SELECT 3", "h3")]


def _push_fragments(handler, with_priority: bool):
    if with_priority:
        return handler.push_to_query_fragment_queue_with_priority(PAIRS, "pk", 2, "integer")
    return handler.push_to_query_fragment_queue(PAIRS, "pk", "integer", size_priority=False)


def _push_original(handler, with_priority: bool):
    if with_priority:
        return handler.push_to_original_query_queue_with_priority("SELECT 1", "pk", 2, "integer")
    return handler.push_to_original_query_queue("SELECT 1", "pk", "integer")


class TestPushStatus:
    @pytest.mark.parametrize("with_priority", [False, True])
    def test_fragment_push_with_an_error_row_is_false(self, with_priority, caplog):
        handler, connection, cursor = _handler_with_connection()
        cursor.fetchall.return_value = [(1, "h1", "pk", "inserted"), (2, "h2", "pk", "error"), (3, "h3", "pk", "error")]

        with caplog.at_level(logging.ERROR, logger="PartitionCache"):
            assert _push_fragments(handler, with_priority) is False

        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "2 of 3" in errors[0] and "h2" in errors[0] and "'error'" in errors[0]
        connection.commit.assert_called_once_with()  # the rows that were queued stay queued

    @pytest.mark.parametrize("with_priority", [False, True])
    @pytest.mark.parametrize("status", ["inserted", "updated", "skipped_locked", "skipped_concurrent"])
    def test_fragment_push_of_queued_rows_is_true(self, with_priority, status):
        handler, _, cursor = _handler_with_connection()
        cursor.fetchall.return_value = [(1, "h1", "pk", "inserted"), (2, "h2", "pk", status), (3, "h3", "pk", "updated")]

        assert _push_fragments(handler, with_priority) is True

    @pytest.mark.parametrize("with_priority", [False, True])
    def test_original_push_with_error_status_is_false(self, with_priority, caplog):
        handler, _, cursor = _handler_with_connection()
        cursor.fetchone.return_value = ("error",)

        with caplog.at_level(logging.ERROR, logger="PartitionCache"):
            assert _push_original(handler, with_priority) is False

        assert any("'error'" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)

    @pytest.mark.parametrize("with_priority", [False, True])
    @pytest.mark.parametrize("status", ["inserted", "updated", "skipped_locked", "skipped_concurrent"])
    def test_original_push_of_a_queued_row_is_true(self, with_priority, status):
        handler, _, cursor = _handler_with_connection()
        cursor.fetchone.return_value = (status,)

        assert _push_original(handler, with_priority) is True


@pytest.mark.parametrize("mode", ["--queue", "--queue-original"])
def test_pcache_add_exits_non_zero_when_a_row_was_not_queued(mode, monkeypatch):
    import sys

    from partitioncache import queue
    from partitioncache.cli import add_to_cache

    handler, _, cursor = _handler_with_connection()
    cursor.fetchall.side_effect = lambda: [(i + 1, f"h{i + 1}", "pk", "error") for i in range(len(cursor.execute.call_args.args[1][0]))]
    cursor.fetchone.return_value = ("error",)
    monkeypatch.setattr(queue, "_queue_handler", handler)
    argv = ["pcache-add", mode, "--query", "SELECT * FROM orders AS o WHERE o.total > 10", "--partition-key", "pk", "--partition-datatype", "integer"]
    with (
        patch.object(sys, "argv", argv),
        patch.object(add_to_cache, "load_environment_with_validation"),
        pytest.raises(SystemExit) as exit_info,
    ):
        add_to_cache.main()
    assert exit_info.value.code == 1

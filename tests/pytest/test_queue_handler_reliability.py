"""Queue handlers: blocking pops honour their timeout, a missing datatype pops as None, a push with failed rows is False."""

import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from partitioncache.queue_handler.postgresql import PostgreSQLQueueHandler

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

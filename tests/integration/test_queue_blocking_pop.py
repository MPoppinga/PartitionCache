"""Blocking pops of the PostgreSQL queue against a server: an empty queue times out, a push wakes the pop by NOTIFY."""

import threading
import time

import pytest

from partitioncache.queue_handler import get_queue_handler

TIMEOUT = 2  # seconds the empty-queue pop may wait
PUSH_DELAY = 1.0
# The pop waits up to 5 s between two polls of the queue; an item pushed at 1 s and popped before 4 s was announced
# by its notification, not found by a poll
WOKEN_BEFORE = 4.0


def _push_fragment(handler) -> bool:
    return handler.push_to_query_fragment_queue([("SELECT DISTINCT t.zipcode FROM test_locations AS t", "blocking_pop_hash")], "zipcode", "integer")


def _push_original(handler) -> bool:
    return handler.push_to_original_query_queue("SELECT * FROM test_locations AS t WHERE t.population > 1", "zipcode", "integer")


# queue -> (blocking pop, push, index of the partition key in the popped tuple)
QUEUES = {
    "fragment": ("pop_from_query_fragment_queue_blocking", _push_fragment, 2),
    "original": ("pop_from_original_query_queue_blocking", _push_original, 1),
}


# CI selects the queue tests with -k "queue and postgresql_array". The queue uses no cache backend; the parameter
# only places these tests in that job.
@pytest.fixture(params=["postgresql_array"])
def queue_handler(request, db_connection):
    handler = get_queue_handler("postgresql")
    handler.clear_all_queues()
    yield handler
    handler.clear_all_queues()
    handler.close()


def _pop_in_thread(pop, timeout: int, deadline: float):
    """Run pop(timeout=...) in a thread and wait at most deadline seconds: (finished, result, seconds)."""
    outcome: dict = {}

    def run():
        start = time.monotonic()
        outcome["result"] = pop(timeout=timeout)
        outcome["seconds"] = time.monotonic() - start

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(deadline)
    return not thread.is_alive(), outcome.get("result"), outcome.get("seconds")


@pytest.mark.parametrize("queue", sorted(QUEUES))
def test_pop_from_an_empty_queue_returns_none_after_its_timeout(queue_handler, queue):
    pop_name, _, _ = QUEUES[queue]

    finished, result, seconds = _pop_in_thread(getattr(queue_handler, pop_name), TIMEOUT, deadline=TIMEOUT + 20)

    assert finished, f"the {queue} pop did not return within {TIMEOUT + 20} s of its {TIMEOUT} s timeout"
    assert result is None
    assert TIMEOUT - 0.5 <= seconds < TIMEOUT + 5


@pytest.mark.parametrize("queue", sorted(QUEUES))
def test_push_from_another_connection_wakes_the_waiting_pop(queue_handler, queue):
    pop_name, push, key_index = QUEUES[queue]
    pusher = get_queue_handler("postgresql")  # a second handler; every push opens its own connection
    pushed: list[bool] = []
    timer = threading.Timer(PUSH_DELAY, lambda: pushed.append(push(pusher)))
    timer.start()
    try:
        finished, result, seconds = _pop_in_thread(getattr(queue_handler, pop_name), timeout=30, deadline=40)
    finally:
        timer.cancel()
        pusher.close()

    assert pushed == [True]
    assert finished, f"the {queue} pop did not return within 40 s"
    assert result is not None and result[key_index] == "zipcode"
    assert seconds < WOKEN_BEFORE, f"popped after {seconds:.2f} s: found by a poll, not woken by the notification"

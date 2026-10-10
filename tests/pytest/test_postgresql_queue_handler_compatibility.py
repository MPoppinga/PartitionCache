"""Compatibility tests for PostgreSQL queue helper upgrades."""

from unittest.mock import MagicMock

import pytest

from partitioncache.queue_handler.postgresql import PostgreSQLQueueHandler


def _handler_with_connection():
    cursor = MagicMock()
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    handler = PostgreSQLQueueHandler.__new__(PostgreSQLQueueHandler)
    handler.fragment_queue_table = "test_query_fragment_queue"
    handler.original_queue_table = "test_original_query_queue"
    handler._get_connection = MagicMock(return_value=connection)
    return handler, connection, cursor


def test_default_backend_uses_legacy_compatible_batch_signature():
    handler, connection, cursor = _handler_with_connection()
    cursor.fetchall.return_value = [(1, "hash-1", "pkey", "inserted")]

    assert handler.push_to_query_fragment_queue(
        [("SELECT 1", "hash-1")], "pkey", "integer"
    )

    statement, params = cursor.execute.call_args.args
    assert statement.count("%s") == 6
    assert len(params) == 6
    connection.rollback.assert_not_called()


def test_explicit_backend_uses_extended_batch_signature():
    handler, _, cursor = _handler_with_connection()
    cursor.fetchall.return_value = [(1, "hash-1", "geom", "inserted")]

    assert handler.push_to_query_fragment_queue(
        [("SELECT 1", "hash-1")], "geom", "geometry", cache_backend="postgis_bbox"
    )

    statement, params = cursor.execute.call_args.args
    assert statement.count("%s") == 7
    assert len(params) == 7
    assert params[-1] == ["postgis_bbox"]


@pytest.mark.parametrize("with_priority", [False, True])
def test_fragment_fallback_rolls_back_failed_helper_transaction(with_priority):
    handler, connection, cursor = _handler_with_connection()
    cursor.execute.side_effect = [RuntimeError("helper signature mismatch"), None]

    if with_priority:
        success = handler.push_to_query_fragment_queue_with_priority(
            [("SELECT 1", "hash-1")], "pkey", 3, "integer"
        )
    else:
        success = handler.push_to_query_fragment_queue(
            [("SELECT 1", "hash-1")], "pkey", "integer"
        )

    assert success
    connection.rollback.assert_called_once_with()
    assert cursor.execute.call_count == 2


def test_original_queue_fallback_rolls_back_failed_helper_transaction():
    handler, connection, cursor = _handler_with_connection()
    cursor.execute.side_effect = [RuntimeError("helper unavailable"), None]

    assert handler.push_to_original_query_queue("SELECT 1", "pkey", "integer")

    connection.rollback.assert_called_once_with()
    assert cursor.execute.call_count == 2

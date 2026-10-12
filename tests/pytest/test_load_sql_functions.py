"""Tests for PostgreSQLAbstractCacheHandler._load_sql_functions.

The loader usually runs inside _serialized_ddl, whose advisory lock lives as long as the
transaction. A failed load must not end that transaction, or the DDL after it runs
without the lock.
"""

from unittest.mock import MagicMock

import pytest
from psycopg.pq import TransactionStatus

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

HANDLER_CLASSES = [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler]


def _handler(handler_cls, transaction_status, fail_load):
    handler = handler_cls.__new__(handler_cls)
    handler.db = MagicMock()
    handler.db.info.transaction_status = transaction_status
    handler.cursor = MagicMock()

    def execute(statement, *args):
        is_control = isinstance(statement, str) and statement.split()[0] in ("SAVEPOINT", "RELEASE", "ROLLBACK")
        if fail_load and not is_control:
            raise RuntimeError("function load failed")

    handler.cursor.execute.side_effect = execute
    return handler


def _statements(handler):
    return [call.args[0] for call in handler.cursor.execute.call_args_list]


@pytest.mark.parametrize("handler_cls", HANDLER_CLASSES)
def test_failed_load_inside_transaction_rolls_back_to_savepoint_only(handler_cls):
    handler = _handler(handler_cls, TransactionStatus.INTRANS, fail_load=True)

    handler._load_sql_functions()

    statements = _statements(handler)
    assert statements[0] == "SAVEPOINT partitioncache_load_sql"
    assert statements[-1] == "ROLLBACK TO SAVEPOINT partitioncache_load_sql"
    # A full rollback would release the advisory lock held by the transaction
    handler.db.rollback.assert_not_called()


@pytest.mark.parametrize("handler_cls", HANDLER_CLASSES)
def test_successful_load_inside_transaction_releases_savepoint(handler_cls):
    handler = _handler(handler_cls, TransactionStatus.INTRANS, fail_load=False)

    handler._load_sql_functions()

    statements = _statements(handler)
    assert statements[0] == "SAVEPOINT partitioncache_load_sql"
    assert statements[-1] == "RELEASE SAVEPOINT partitioncache_load_sql"
    assert len(statements) == 3
    handler.db.rollback.assert_not_called()


def test_failed_load_outside_transaction_rolls_back():
    handler = _handler(PostgreSQLArrayCacheHandler, TransactionStatus.IDLE, fail_load=True)

    handler._load_sql_functions()

    assert len(_statements(handler)) == 1  # no savepoint without a transaction to protect
    handler.db.rollback.assert_called_once_with()


@pytest.mark.parametrize("handler_cls", HANDLER_CLASSES)
def test_lost_savepoint_resets_the_aborted_transaction(handler_cls):
    """Another thread committed on the shared connection between SAVEPOINT and RELEASE."""
    handler = _handler(handler_cls, TransactionStatus.INTRANS, fail_load=False)

    def execute(statement, *args):
        if isinstance(statement, str) and statement.startswith(("RELEASE", "ROLLBACK TO")):
            handler.db.info.transaction_status = TransactionStatus.INERROR
            raise RuntimeError('savepoint "partitioncache_load_sql" does not exist')

    handler.cursor.execute.side_effect = execute

    handler._load_sql_functions()

    assert _statements(handler)[-1] == "ROLLBACK TO SAVEPOINT partitioncache_load_sql"
    # Returning with an aborted transaction would make every later write on the connection fail
    handler.db.rollback.assert_called_once_with()

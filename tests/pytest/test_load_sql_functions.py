"""Tests for PostgreSQLAbstractCacheHandler._load_sql_functions.

The loader usually runs inside _serialized_ddl, whose advisory lock lives as long as the
transaction. A failed load must leave the connection usable and the lock held, without
any extra statements on the success path (handlers may share a connection across threads).
"""

from unittest.mock import MagicMock

import pytest
from psycopg.pq import TransactionStatus

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext(%s))"
HANDLER_CLASSES = [PostgreSQLArrayCacheHandler, PostgreSQLBitCacheHandler, PostgreSQLRoaringBitCacheHandler]


def _handler(handler_cls, transaction_status, fail_load):
    handler = handler_cls.__new__(handler_cls)
    handler.db = MagicMock()
    handler.db.info.transaction_status = transaction_status
    handler.cursor = MagicMock()

    def execute(statement, *args):
        if fail_load and statement != LOCK_SQL:
            raise RuntimeError("function load failed")

    handler.cursor.execute.side_effect = execute
    return handler


def _statements(handler):
    return [call.args[0] for call in handler.cursor.execute.call_args_list]


@pytest.mark.parametrize("handler_cls", HANDLER_CLASSES)
def test_failed_load_inside_transaction_retakes_the_ddl_lock(handler_cls):
    handler = _handler(handler_cls, TransactionStatus.INTRANS, fail_load=True)
    handler.tableprefix = "pc"

    handler._load_sql_functions()

    handler.db.rollback.assert_called_once_with()
    # The rollback released the caller's advisory lock; the DDL after the load needs it again
    assert handler.cursor.execute.call_args_list[-1].args == (LOCK_SQL, ("pc",))
    assert len(_statements(handler)) == 2


@pytest.mark.parametrize("handler_cls", HANDLER_CLASSES)
def test_successful_load_inside_transaction_runs_only_the_file(handler_cls):
    handler = _handler(handler_cls, TransactionStatus.INTRANS, fail_load=False)

    handler._load_sql_functions()

    # No savepoint: another thread's commit on a shared connection would destroy it
    assert len(_statements(handler)) == 1
    handler.db.rollback.assert_not_called()


def test_failed_load_outside_transaction_rolls_back():
    handler = _handler(PostgreSQLArrayCacheHandler, TransactionStatus.IDLE, fail_load=True)

    handler._load_sql_functions()

    assert len(_statements(handler)) == 1  # no lock to retake outside a transaction
    handler.db.rollback.assert_called_once_with()


@pytest.mark.parametrize("handler_cls", HANDLER_CLASSES)
def test_serialized_ddl_and_retake_use_the_same_lock(handler_cls):
    handler = _handler(handler_cls, TransactionStatus.IDLE, fail_load=False)
    handler.tableprefix = "pc"

    with handler._serialized_ddl():
        pass

    assert handler.cursor.execute.call_args_list[-1].args == (LOCK_SQL, ("pc",))

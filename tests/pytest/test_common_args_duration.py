from unittest.mock import Mock

import pytest

from partitioncache.cli.common_args import (
    ensure_role_exists,
    function_exists,
    parse_duration_to_seconds,
    table_exists,
)

# ---------------------------------------------------------------------------
# parse_duration_to_seconds
# ---------------------------------------------------------------------------


def test_parse_duration_to_seconds_basic_values():
    assert parse_duration_to_seconds(30) == 30
    assert parse_duration_to_seconds("30") == 30
    assert parse_duration_to_seconds("30s") == 30
    assert parse_duration_to_seconds("5m") == 300
    assert parse_duration_to_seconds("2h") == 7200
    assert parse_duration_to_seconds("1d") == 86400


def test_parse_duration_to_seconds_invalid_values():
    with pytest.raises(ValueError):
        parse_duration_to_seconds("-1")

    with pytest.raises(ValueError):
        parse_duration_to_seconds("abc")

    with pytest.raises(ValueError):
        parse_duration_to_seconds("5w")


def test_parse_duration_to_seconds_zero():
    assert parse_duration_to_seconds(0) == 0
    assert parse_duration_to_seconds("0") == 0
    assert parse_duration_to_seconds("0s") == 0
    assert parse_duration_to_seconds("0m") == 0


def test_parse_duration_to_seconds_case_insensitive():
    assert parse_duration_to_seconds("5M") == 300
    assert parse_duration_to_seconds("2H") == 7200
    assert parse_duration_to_seconds("1D") == 86400
    assert parse_duration_to_seconds("30S") == 30


def test_parse_duration_to_seconds_whitespace():
    assert parse_duration_to_seconds(" 5m ") == 300
    assert parse_duration_to_seconds("  30  ") == 30
    assert parse_duration_to_seconds(" 2h ") == 7200


def test_parse_duration_to_seconds_negative_int():
    with pytest.raises(ValueError):
        parse_duration_to_seconds(-1)


# ---------------------------------------------------------------------------
# table_exists / function_exists
# ---------------------------------------------------------------------------


class TestTableExists:
    def test_table_exists_true(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (True,)

        assert table_exists(mock_conn, "my_table") is True
        mock_cursor.execute.assert_called_once()
        assert "information_schema.tables" in mock_cursor.execute.call_args[0][0]

    def test_table_exists_false(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (False,)

        assert table_exists(mock_conn, "missing_table") is False


class TestFunctionExists:
    def test_function_exists_true(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (True,)

        assert function_exists(mock_conn, "my_func") is True
        mock_cursor.execute.assert_called_once()
        assert "pg_proc" in mock_cursor.execute.call_args[0][0]

    def test_function_exists_false(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (False,)

        assert function_exists(mock_conn, "missing_func") is False


# ---------------------------------------------------------------------------
# ensure_role_exists
# ---------------------------------------------------------------------------


class TestEnsureRoleExists:
    def test_role_already_exists(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = (1,)

        ok, msg = ensure_role_exists(mock_conn, "existing_role")
        assert ok is True
        assert "already exists" in msg

    def test_role_missing_no_create(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_cursor.fetchone.return_value = None

        ok, msg = ensure_role_exists(mock_conn, "missing_role", create_if_missing=False)
        assert ok is False
        assert "does not exist" in msg

    def test_role_create_success(self):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        # First call: SELECT returns None (role missing); second call: CREATE succeeds
        mock_cursor.fetchone.return_value = None

        ok, msg = ensure_role_exists(mock_conn, "new_role", create_if_missing=True)
        assert ok is True
        assert "created successfully" in msg
        mock_conn.commit.assert_called()

    def test_role_create_permission_failure(self):
        mock_conn = Mock()
        # First cursor context: SELECT (role missing)
        mock_cursor_select = Mock()
        mock_cursor_select.fetchone.return_value = None

        # Second cursor context: CREATE raises permission error
        mock_cursor_create = Mock()
        mock_cursor_create.execute.side_effect = Exception("permission denied")

        # Make cursor() return different mocks on successive calls
        cursor_contexts = iter([mock_cursor_select, mock_cursor_create])

        def make_cursor_cm():
            cur = next(cursor_contexts)
            cm = Mock()
            cm.__enter__ = Mock(return_value=cur)
            cm.__exit__ = Mock(return_value=None)
            return cm

        mock_conn.cursor.side_effect = make_cursor_cm

        ok, msg = ensure_role_exists(mock_conn, "forbidden_role", create_if_missing=True)
        assert ok is False
        assert "Failed to create role" in msg
        mock_conn.rollback.assert_called()

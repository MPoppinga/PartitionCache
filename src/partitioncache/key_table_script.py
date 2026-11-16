"""Split a key-table restriction script into its setup, its SELECT and its teardown, for lazy insertion.

The key-table methods (``TMP_TABLE_*``, ``UNLOGGED_TABLE_*``) restrict a query with a script: statements that create,
fill, index and analyze a key table, the query's SELECT, and for an unlogged table a DROP of it. Lazy insertion
stores the keys of a single SELECT without moving them through Python. To store a restricted fragment the same way,
the cache handler runs the setup, stores the SELECT's keys and runs the teardown, all in one transaction.

Only the statements these methods emit for PostgreSQL are accepted around the SELECT: ``SET LOCAL`` of the
collapse limits, and CREATE / INSERT INTO / CREATE INDEX / ANALYZE / DROP TABLE of the library's key tables
(``tmp_cache_keys_<n>``, ``partitioncache_unl_<hex>``, ``pcache_stage_<hex>``). Every other statement, a DELETE
anywhere, and a DROP other than of a key table (``ON COMMIT DROP`` is a clause, not a statement) refuse the script,
as the guard of a single query refuses DELETE and DROP.
"""

import re

_KEY_TABLE = r"(?:tmp_cache_keys_\d+|partitioncache_unl_[0-9a-f]+|pcache_stage_[0-9a-f]+)"
_SETUP_STATEMENTS = (
    re.compile(r"SET\s+LOCAL\s+(?:join_collapse_limit|from_collapse_limit)\s*=\s*\d+", re.IGNORECASE),
    re.compile(rf"CREATE\s+(?:TEMPORARY|UNLOGGED)\s+TABLE\s+{_KEY_TABLE}\b.*", re.IGNORECASE | re.DOTALL),
    re.compile(rf"INSERT\s+INTO\s+{_KEY_TABLE}\b.*", re.IGNORECASE | re.DOTALL),
    # The folded fill: WITH pcache_k_<hex> AS MATERIALIZED (<keys>) INSERT INTO <key table> ...
    re.compile(rf"WITH\s+pcache_k_[0-9a-f]+\b.*\bINSERT\s+INTO\s+{_KEY_TABLE}\b.*", re.IGNORECASE | re.DOTALL),
    re.compile(rf"CREATE\s+INDEX\s+{_KEY_TABLE}_idx\s+ON\s+{_KEY_TABLE}\s*\(.*", re.IGNORECASE | re.DOTALL),
    re.compile(rf"ANALYZE\s+{_KEY_TABLE}", re.IGNORECASE),
)
_DROP_KEY_TABLE = re.compile(rf"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?{_KEY_TABLE}", re.IGNORECASE)
_QUERY = re.compile(r"(?:SELECT|WITH)\b", re.IGNORECASE)
_ON_COMMIT_DROP = re.compile(r"ON\s+COMMIT\s+DROP\b", re.IGNORECASE)


def _guarded(statement: str, allow_on_commit_drop: bool = False) -> bool:
    """The guard of a single lazy query: no DELETE and no DROP (except the ON COMMIT DROP clause of a setup)."""
    text = _ON_COMMIT_DROP.sub("", statement) if allow_on_commit_drop else statement
    return "DELETE " not in text.upper() and "DROP " not in text.upper()


def _statements(script: str) -> list[tuple[int, int]] | None:
    """(start, end) offsets of the script's statements, split at top-level semicolons; None if it cannot be tokenized."""
    try:
        from sqlglot.dialects.postgres import Postgres
        from sqlglot.tokens import TokenType

        tokens = Postgres().tokenize(script)
    except Exception:
        return None
    spans = []
    start = 0
    for token in tokens:
        if token.token_type == TokenType.SEMICOLON:
            spans.append((start, token.start))
            start = token.end + 1
    spans.append((start, len(script)))
    return [(begin, end) for begin, end in spans if script[begin:end].strip()]


def lazy_insertion_parts(query: str) -> tuple[str, str, str]:
    """(setup, select, teardown) of a query for lazy insertion; setup and teardown are '' for a single statement.

    Args:
        query: A single query, or a key-table restriction script (see the module docstring).

    Returns:
        The setup statements (to run before), the SELECT whose keys are stored, and the teardown statements (to run
        after), each as a substring of ``query``.

    Raises:
        ValueError: For a query with DELETE or DROP, or a script with any statement the key-table methods do not
            emit (the message names it).
    """
    spans = _statements(query)
    if spans is None or len(spans) <= 1:
        if not _guarded(query):
            raise ValueError("Query contains DELETE or DROP")
        return "", query, ""

    statements = [query[begin:end].strip() for begin, end in spans]
    teardown_start = len(statements)
    while teardown_start > 1 and _DROP_KEY_TABLE.fullmatch(statements[teardown_start - 1]):
        teardown_start -= 1
    select_index = teardown_start - 1
    select = statements[select_index]
    if not _QUERY.match(select) or not _guarded(select):
        raise ValueError(f"The script's last statement before its teardown is not a SELECT without DELETE or DROP: {select[:80]}")
    for statement in statements[:select_index]:
        if _DROP_KEY_TABLE.fullmatch(statement):
            continue  # a staging table, dropped once the key table is filled
        if not any(pattern.fullmatch(statement) for pattern in _SETUP_STATEMENTS) or not _guarded(statement, allow_on_commit_drop=True):
            raise ValueError(f"The script holds a statement a key-table method does not emit: {statement[:80]}")

    select_begin, select_end = spans[select_index]
    teardown = query[select_end:].lstrip()
    return query[:select_begin], query[select_begin:select_end], teardown[1:] if teardown.startswith(";") else teardown

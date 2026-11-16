"""CLI commands whose work partly failed exit non-zero after doing the rest, and count only real successes."""

import logging
import os
import pickle
import sqlite3
import sys
from unittest.mock import MagicMock, patch

import pytest

from partitioncache.cli import add_to_cache, manage_cache, postgresql_queue_processor
from partitioncache.query_processor import generate_all_query_hash_pairs

QUERY = "SELECT t.pk FROM t, d WHERE t.k = d.k AND d.x = 7"


@pytest.fixture
def empty_env_file(tmp_path):
    path = tmp_path / "empty.env"
    path.write_text("")
    return path


def _exit_code(main, argv):
    """The exit status of main() run with ``argv``; 0 when it returns."""
    with patch.object(sys, "argv", argv):
        try:
            main()
        except SystemExit as e:
            return 0 if e.code is None else e.code
    return 0


def _errors(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


# ---------------------------------------------------------------- pcache-add --direct


@pytest.fixture
def sqlite_db(tmp_path):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript("CREATE TABLE t (pk INTEGER, k INTEGER); CREATE TABLE d (pk INTEGER, k INTEGER, x INTEGER);")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(pk, pk % 3) for pk in range(10)])
    conn.executemany("INSERT INTO d VALUES (?, ?, ?)", [(pk, pk % 3, pk % 3) for pk in range(10)])
    conn.commit()
    conn.close()
    return db_path


def _add_direct_argv(db_path, env_file):
    return ["pcache-add", "--direct", "--query", QUERY, "--partition-key", "pk", "--partition-datatype", "integer",
            "--db-backend", "sqlite", "--db-dir", str(db_path), "--env-file", str(env_file), "--min-component-size", "1"]


@pytest.fixture
def rocksdict_cache(tmp_path, monkeypatch):
    """CACHE_BACKEND=rocksdict in tmp_path; skipped where rocksdict has no wheel (e.g. Python 3.15)."""
    pytest.importorskip("rocksdict")
    monkeypatch.setenv("CACHE_BACKEND", "rocksdict")
    monkeypatch.setenv("ROCKSDB_DICT_PATH", str(tmp_path / "cache"))


def test_add_direct_exits_1_when_a_variant_cannot_be_stored(rocksdict_cache, sqlite_db, empty_env_file, caplog):
    from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

    variants = generate_all_query_hash_pairs(QUERY, "pk", min_component_size=1, keep_all_attributes=True, warn_no_partition_key=False)
    real_set_cache = RocksDictCacheHandler.set_cache
    calls = []

    def set_cache_failing_once(self, key, *args, **kwargs):
        calls.append(key)
        return False if len(calls) == 1 else real_set_cache(self, key, *args, **kwargs)

    with patch.object(RocksDictCacheHandler, "set_cache", set_cache_failing_once):
        assert _exit_code(add_to_cache.main, _add_direct_argv(sqlite_db, empty_env_file)) == 1

    assert len(calls) == len(variants)  # the other variants are still stored
    assert any(f"1 of {len(variants)}" in message for message in _errors(caplog))


def test_add_direct_exits_0_when_every_variant_is_stored(rocksdict_cache, sqlite_db, empty_env_file):
    assert _exit_code(add_to_cache.main, _add_direct_argv(sqlite_db, empty_env_file)) == 0


def test_add_direct_without_variants_warns(rocksdict_cache, sqlite_db, empty_env_file, caplog):
    with patch.object(add_to_cache, "generate_all_query_hash_pairs", return_value=[]), caplog.at_level(logging.WARNING, logger="PartitionCache"):
        assert _exit_code(add_to_cache.main, _add_direct_argv(sqlite_db, empty_env_file)) == 0
    assert any("no query variants" in r.getMessage().lower() for r in caplog.records if r.levelno == logging.WARNING)


def test_add_direct_spatial_exits_1_when_lazy_store_fails(empty_env_file, caplog):
    handler = MagicMock(spec=["set_cache_lazy", "set_query", "exists", "close"])
    handler.db = MagicMock()
    handler.db.info.host, handler.db.info.port, handler.db.info.dbname = "db.example", 5432, "gis"
    handler.set_cache_lazy.side_effect = [False, True]
    helper = MagicMock()
    helper.exists.return_value = False
    argv = ["pcache-add", "--direct", "--query", "SELECT p1.geom FROM pois p1, pois p2 WHERE ST_DWithin(p1.geom, p2.geom, 100) AND p2.kind = 'x'",
            "--partition-key", "area", "--partition-datatype", "geometry", "--cache-backend", "postgis_bbox", "--db-name", "gis",
            "--env-file", str(empty_env_file), "--geometry-column", "geom"]
    pairs = [("SELECT 1", "h1"), ("SELECT 2", "h2")]
    with (
        patch.dict(os.environ, {"DB_HOST": "db.example", "DB_PORT": "5432"}),
        patch.object(add_to_cache.partitioncache, "get_cache_handler", return_value=handler),
        patch.object(add_to_cache.partitioncache, "create_partitioncache_helper", return_value=helper),
        patch.object(add_to_cache, "generate_all_query_hash_pairs", return_value=pairs),
    ):
        os.environ.pop("PG_DB_HOST", None)
        os.environ.pop("PG_DB_PORT", None)
        assert _exit_code(add_to_cache.main, argv) == 1
    assert handler.set_cache_lazy.call_count == 2
    assert any("1 of 2" in message for message in _errors(caplog))


# ---------------------------------------------------------------- pcache-manage cache copy / export / import


def _source(keys=("a", "b"), partitions=(("pk", "integer"),)):
    source = MagicMock()
    source.get_partition_keys.return_value = list(partitions)
    source.get_all_keys.return_value = list(keys)
    source.get.return_value = {1, 2}
    source.get_all_queries.return_value = [(key, f"SELECT {key}") for key in keys]
    return source


def _target(set_cache_results):
    target = MagicMock()
    target.get_datatype.return_value = "integer"
    target.exists.return_value = False
    target.set_cache.side_effect = list(set_cache_results)
    target.set_query.return_value = True
    return target


def _manage(*argv):
    return _exit_code(manage_cache.main, ["pcache-manage", *argv])


def test_copy_counts_only_stored_entries_and_exits_1(caplog):
    source, target = _source(), _target([True, False])
    with patch.object(manage_cache, "get_cache_handler", side_effect=[source, target]), caplog.at_level(logging.INFO, logger="PartitionCache"):
        assert _manage("cache", "copy", "--from", "src", "--to", "dst") == 1
    assert any("1 keys copied" in r.getMessage() for r in caplog.records)
    assert any("1 keys" in message and "failed" in message for message in _errors(caplog))


def test_copy_without_failures_exits_0():
    source, target = _source(), _target([True, True])
    with patch.object(manage_cache, "get_cache_handler", side_effect=[source, target]):
        assert _manage("cache", "copy", "--from", "src", "--to", "dst") == 0


def test_copy_of_a_partition_error_exits_1():
    source, target = _source(), _target([])
    source.get_all_keys.side_effect = RuntimeError("connection lost")
    with patch.object(manage_cache, "get_cache_handler", side_effect=[source, target]):
        assert _manage("cache", "copy", "--from", "src", "--to", "dst") == 1


def test_copy_of_a_missing_partition_key_exits_1():
    with patch.object(manage_cache, "get_cache_handler", side_effect=[_source(), _target([])]):
        assert _manage("cache", "copy", "--from", "src", "--to", "dst", "--partition-key", "absent") == 1


def test_export_of_a_missing_partition_key_exits_1(tmp_path):
    with patch.object(manage_cache, "get_cache_handler", return_value=_source()):
        assert _manage("cache", "export", "--type", "src", "--file", str(tmp_path / "a.pkl"), "--partition-key", "absent") == 1


def test_export_with_a_partition_error_exits_1(tmp_path):
    source = _source(partitions=(("pk", "integer"), ("pk2", "integer")))
    source.get_all_keys.side_effect = [["a"], RuntimeError("connection lost")]
    with patch.object(manage_cache, "get_cache_handler", return_value=source):
        assert _manage("cache", "export", "--type", "src", "--file", str(tmp_path / "a.pkl")) == 1


def test_export_without_failures_exits_0(tmp_path):
    with patch.object(manage_cache, "get_cache_handler", return_value=_source()):
        assert _manage("cache", "export", "--type", "src", "--file", str(tmp_path / "a.pkl")) == 0


def _archive(tmp_path, keys=("a", "b")):
    path = tmp_path / "archive.pkl"
    with open(path, "wb") as file:
        pickle.dump({"__PARTITION_METADATA__": [{"partition_key": "pk", "datatype": "integer"}]}, file)
        for key in keys:
            pickle.dump({"key": key, "value": {1}, "partition_key": "pk"}, file)
    return path


def test_import_counts_only_stored_entries_and_exits_1(tmp_path, caplog):
    target = _target([False, True])
    with patch.object(manage_cache, "get_cache_handler", return_value=target), caplog.at_level(logging.INFO, logger="PartitionCache"):
        assert _manage("cache", "import", "--type", "dst", "--file", str(_archive(tmp_path))) == 1
    assert any("1 keys restored" in r.getMessage() for r in caplog.records)
    assert _errors(caplog)


def test_import_entry_error_exits_1(tmp_path):
    target = _target([RuntimeError("disk full"), True])
    with patch.object(manage_cache, "get_cache_handler", return_value=target):
        assert _manage("cache", "import", "--type", "dst", "--file", str(_archive(tmp_path))) == 1


def test_import_without_failures_exits_0(tmp_path):
    with patch.object(manage_cache, "get_cache_handler", return_value=_target([True, True])):
        assert _manage("cache", "import", "--type", "dst", "--file", str(_archive(tmp_path))) == 0


# ---------------------------------------------------------------- pcache-manage maintenance prune / queue clear


def test_prune_of_one_type_error_exits_1():
    handler = MagicMock()
    handler.prune_old_queries.side_effect = RuntimeError("relation does not exist")
    with patch.object(manage_cache, "get_cache_handler", return_value=handler):
        assert _manage("maintenance", "prune", "--days", "7", "--type", "postgresql_array") == 1


def test_prune_of_all_types_error_exits_1():
    handler = MagicMock()
    handler.prune_old_queries.side_effect = [3, RuntimeError("relation does not exist")]

    def get(cache_type):
        if cache_type in ("postgresql_array", "postgresql_bit"):
            return handler
        raise ValueError(f"{cache_type} not configured")

    with patch.object(manage_cache, "get_cache_handler", side_effect=get):
        assert _manage("maintenance", "prune", "--days", "7") == 1


def test_prune_of_unconfigured_types_exits_0():
    with patch.object(manage_cache, "get_cache_handler", side_effect=ValueError("not configured")):
        assert _manage("maintenance", "prune", "--days", "7") == 0


@pytest.mark.parametrize(
    "flags, function",
    [((), "clear_all_queues"), (("--original",), "clear_original_query_queue"), (("--fragment",), "clear_query_fragment_queue")],
)
def test_queue_clear_error_exits_1(flags, function):
    with patch(f"partitioncache.queue.{function}", side_effect=RuntimeError("queue unreachable")):
        assert _manage("queue", "clear", *flags) == 1


# ---------------------------------------------------------------- pcache-postgresql-queue-processor check-permissions


@pytest.mark.parametrize("granted, expected", [(False, 1), (True, 0)])
def test_check_permissions_exit_status(granted, expected, empty_env_file, caplog):
    argv = ["pcache-postgresql-queue-processor", "--env", str(empty_env_file), "check-permissions"]
    with (
        patch.object(postgresql_queue_processor, "validate_environment", return_value=(True, "")),
        patch.object(postgresql_queue_processor, "check_and_grant_pg_cron_permissions", return_value=(granted, "permission denied for schema cron")),
    ):
        assert _exit_code(postgresql_queue_processor.main, argv) == expected
    assert bool(_errors(caplog)) is not granted

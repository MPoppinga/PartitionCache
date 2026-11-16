"""Lazy insertion runs a fragment on the cache handler's own connection; it is used only when that is the data database.

pcache-monitor and pcache-add --direct execute fragments on the data connection (--db-backend, --db-name, PG_DB_HOST /
DB_HOST, ...). Lazy insertion (set_entry_lazy / set_cache_lazy) executes them on the cache handler's connection
instead (DB_NAME, DB_HOST, ... or the handler's own variables). With two databases that have the same table names the
fragment would be computed on the wrong dataset and stored under a hash that depends only on the query text.
"""

import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import partitioncache.cli.monitor_cache_queue as mcq_module
from partitioncache.cli import add_to_cache
from partitioncache.cli.common_args import cache_connection_target, data_connection_target, lazy_insertion_refusal

SF1 = ("db.example", 5432, "bench_sf1")


def _handler(host="db.example", port=5432, dbname="bench_sf1", attribute="db"):
    """A cache handler whose psycopg connection reports these resolved values (connection.info)."""
    handler = Mock(spec=["set_cache_lazy", "set_entry_lazy", "set_cache", "set_query", "set_query_status", "register_partition_key", "exists", "close"])
    setattr(handler, attribute, SimpleNamespace(info=SimpleNamespace(host=host, port=port, dbname=dbname)))
    return handler


class TestLazyInsertionRefusal:
    def test_same_server_and_database(self):
        assert lazy_insertion_refusal(SF1, SF1) is None

    def test_host_compared_case_insensitively(self):
        assert lazy_insertion_refusal(("DB.Example", 5432, "bench_sf1"), SF1) is None

    @pytest.mark.parametrize(
        "cache_target",
        [
            ("db.example", 5432, "bench_sf10"),  # other database on the same server
            ("db.example", 5433, "bench_sf1"),  # other server on the same host
            ("other.example", 5432, "bench_sf1"),
            ("127.0.0.1", 5432, "bench_sf1"),  # not resolved: when in doubt, not lazy
            ("/var/run/postgresql", 5432, "bench_sf1"),  # Unix socket against a TCP host
        ],
    )
    def test_different_target_is_refused(self, cache_target):
        reason = lazy_insertion_refusal(SF1, cache_target)
        assert reason is not None and cache_target[2] in reason

    def test_localhost_and_loopback_address_are_not_assumed_equal(self):
        assert lazy_insertion_refusal(("localhost", 5432, "bench_sf1"), ("127.0.0.1", 5432, "bench_sf1")) is not None

    def test_unknown_targets_are_refused(self):
        assert lazy_insertion_refusal(None, SF1) is not None
        assert lazy_insertion_refusal(SF1, None) is not None


class TestConnectionTargets:
    def test_cache_target_from_psycopg_connection_info(self):
        assert cache_connection_target(_handler()) == SF1
        assert cache_connection_target(_handler(attribute="pg_conn")) == SF1  # rocksdict_h3_grouped

    @pytest.mark.parametrize("info", [None, SimpleNamespace(host=None, port=5432, dbname="x"), SimpleNamespace(host="h", port=5432, dbname="")])
    def test_cache_target_unknown(self, info):
        handler = Mock(spec=["set_cache_lazy"])
        handler.db = SimpleNamespace(info=info) if info is not None else object()
        assert cache_connection_target(handler) is None

    def test_cache_target_of_handler_without_connection(self):
        assert cache_connection_target(Mock(spec=["set_cache_lazy", "set_entry_lazy"])) is None

    def test_data_target_postgresql(self):
        args = SimpleNamespace(db_backend="postgresql", db_name="bench_sf1")
        with patch.dict(os.environ, {"DB_HOST": "db.example", "DB_PORT": "5432"}):
            os.environ.pop("PG_DB_HOST", None)
            os.environ.pop("PG_DB_PORT", None)
            assert data_connection_target(args) == SF1

    def test_data_target_other_backend_or_no_database(self):
        assert data_connection_target(SimpleNamespace(db_backend="sqlite", db_name=None)) is None
        with patch.dict(os.environ):
            os.environ.pop("DB_NAME", None)
            assert data_connection_target(SimpleNamespace(db_backend="postgresql", db_name=None)) is None


@pytest.fixture
def monitor_args():
    args = SimpleNamespace(
        db_backend="postgresql",
        db_name="bench_sf1",
        enable_cache_optimization=False,
        disable_cache_optimization=True,
        bitsize=None,
        limit=None,
        force_recalculate=False,
        long_running_query_timeout="0",
        disable_lazy_insertion=False,
        enable_duckdb_acceleration=False,
        grid_cell_size={},
        cache_backend="postgresql_array",
    )
    original = getattr(mcq_module, "args", None)
    mcq_module.args = args
    with patch.dict(os.environ, {"DB_HOST": "db.example", "DB_PORT": "5432"}):
        os.environ.pop("PG_DB_HOST", None)
        os.environ.pop("PG_DB_PORT", None)
        mcq_module._lazy_insertion_refusals.clear()
        yield args
    if original is not None:
        mcq_module.args = original
    else:
        delattr(mcq_module, "args")


def _run_monitor(handler, datatype="integer"):
    db = Mock()
    db.execute.return_value = [1, 2]
    with patch.object(mcq_module, "get_cache_handler", return_value=handler), patch.object(mcq_module, "get_db_handler", return_value=db) as get_db:
        result = mcq_module.run_and_store_query("SELECT pk FROM t", "h1", "pk", datatype)
    return result, get_db, db


class TestMonitor:
    def test_lazy_insertion_on_the_data_database(self, monitor_args):
        handler = _handler()
        handler.set_entry_lazy.return_value = True
        result, get_db, _ = _run_monitor(handler)
        assert result is True
        handler.set_entry_lazy.assert_called_once()
        get_db.assert_not_called()

    def test_other_database_uses_the_data_connection(self, monitor_args, caplog):
        handler = _handler(dbname="bench_sf10")
        handler.set_cache.return_value = True
        handler.set_query.return_value = True
        with caplog.at_level("INFO", logger="PartitionCache"):
            result, get_db, db = _run_monitor(handler)
            _run_monitor(handler)
        assert result is True
        handler.set_entry_lazy.assert_not_called()
        db.execute.assert_called_once_with("SELECT pk FROM t")
        handler.set_cache.assert_called_with("h1", {1, 2}, "pk")
        reasons = [r for r in caplog.records if "lazy insertion" in r.getMessage().lower() and "bench_sf10" in r.getMessage()]
        assert len(reasons) == 1 and reasons[0].levelname == "INFO"  # logged once

    def test_non_postgresql_data_backend_is_not_lazy(self, monitor_args):
        monitor_args.db_backend = "sqlite"
        monitor_args.db_dir = "unused.sqlite"
        handler = _handler()
        handler.set_cache.return_value = True
        handler.set_query.return_value = True
        _run_monitor(handler)
        handler.set_entry_lazy.assert_not_called()

    def test_spatial_fragment_on_other_database_fails(self, monitor_args):
        handler = _handler(dbname="bench_sf10")
        result, get_db, _ = _run_monitor(handler, datatype="geometry")
        assert result is False
        handler.set_entry_lazy.assert_not_called()
        get_db.assert_not_called()


class TestAddDirectSpatial:
    def _main(self, tmp_path, handler, db_name):
        env_file = tmp_path / "empty.env"
        env_file.write_text("")
        argv = ["pcache-add", "--direct", "--no-recompose", "--query", "SELECT geom FROM pois", "--partition-key", "area",
                "--partition-datatype", "geometry", "--cache-backend", "postgis_bbox", "--db-name", db_name, "--env-file", str(env_file)]
        helper = Mock()
        helper.exists.return_value = False
        with (
            patch.dict(os.environ, {"DB_HOST": "db.example", "DB_PORT": "5432"}),
            patch.object(sys, "argv", argv),
            patch.object(add_to_cache.partitioncache, "get_cache_handler", return_value=handler),
            patch.object(add_to_cache.partitioncache, "create_partitioncache_helper", return_value=helper),
        ):
            os.environ.pop("PG_DB_HOST", None)
            os.environ.pop("PG_DB_PORT", None)
            add_to_cache.main()

    def test_same_database_stores_lazily(self, tmp_path):
        handler = _handler()
        handler.set_cache_lazy.return_value = True
        self._main(tmp_path, handler, "bench_sf1")
        handler.set_cache_lazy.assert_called_once()

    def test_other_database_exits_without_running_the_fragment(self, tmp_path):
        handler = _handler(dbname="bench_sf10")
        with pytest.raises(SystemExit) as exit_info:
            self._main(tmp_path, handler, "bench_sf1")
        assert exit_info.value.code == 1
        handler.set_cache_lazy.assert_not_called()

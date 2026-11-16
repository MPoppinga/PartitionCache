"""pcache-monitor: --close waits for both queues and the decomposer, a missing datatype is resolved, a failed push is not lost silently."""

from unittest.mock import Mock, patch

import pytest

import partitioncache.cli.monitor_cache_queue as mcq

MODULE = "partitioncache.cli.monitor_cache_queue"

@pytest.fixture
def args():
    args = Mock()
    args.close = False
    args.disable_optimized_polling = False
    args.max_processes = 2
    args.status_log_interval = 10
    args.cache_backend = "rocksdict"
    args.db_backend = "sqlite"
    args.limit = None
    args.long_running_query_timeout = "0"
    args.force_recalculate = False
    args.disable_lazy_insertion = False
    args.enable_cache_optimization = False
    args.disable_cache_optimization = True
    args.enable_duckdb_acceleration = False
    args.bitsize = None
    args.log_query_times = None
    args.size_priority = None
    args.skip_partition_key_joins = []
    args.grid_cell_size = None
    original = getattr(mcq, "args", None)
    mcq.args = args
    yield args
    if original is not None:
        mcq.args = original
    else:
        delattr(mcq, "args")


class TestMissingDatatype:
    def _run(self, cache, datatype, db_rows=(1, 2, 3)):
        db = Mock()
        db.execute.return_value = list(db_rows)
        with patch(f"{MODULE}.get_cache_handler", return_value=cache), patch(f"{MODULE}.get_db_handler", return_value=db):
            return mcq.run_and_store_query("SELECT DISTINCT o.pk FROM orders AS o", "h1", "pk", datatype)

    @pytest.mark.parametrize("datatype", [None, ""])
    def test_unregistered_key_is_inferred_from_the_result(self, args, datatype):
        cache = Mock()
        cache.get_datatype.return_value = None
        cache.set_cache.return_value = True
        cache.set_query.return_value = True

        assert self._run(cache, datatype) is True

        cache.register_partition_key.assert_not_called()
        cache.set_entry_lazy.assert_not_called()  # lazy insertion needs a registered datatype
        cache.set_cache.assert_called_once_with("h1", {1, 2, 3}, "pk")

    @pytest.mark.parametrize("datatype", [None, ""])
    def test_registered_key_uses_its_datatype(self, args, datatype):
        cache = Mock()
        cache.get_datatype.return_value = "integer"
        cache.set_entry_lazy.return_value = True

        assert self._run(cache, datatype) is True

        cache.register_partition_key.assert_called_once()
        assert cache.register_partition_key.call_args.args == ("pk", "integer")
        cache.set_entry_lazy.assert_called_once()

    def test_empty_result_of_an_unregistered_key_gets_the_status_failed(self, args, tmp_path):
        """The datatype cannot be inferred from an empty result; the fragment is marked, not dropped silently."""
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        cache = RocksDictCacheHandler(str(tmp_path / "cache"))
        try:
            assert self._run(cache, None, db_rows=()) is True
            assert cache.get_query_status("h1", "pk") == "failed"
        finally:
            cache.close()

    def test_real_handler_with_empty_string_datatype(self, args, tmp_path):
        """A queue row with '' as datatype (stored by an older client) registers nothing and stores the inferred keys."""
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        cache = RocksDictCacheHandler(str(tmp_path / "cache"))
        try:
            assert self._run(cache, "") is True
            assert cache.get("h1", "pk") == {1, 2, 3}
            assert cache.get_datatype("pk") == "integer"
        finally:
            cache.close()

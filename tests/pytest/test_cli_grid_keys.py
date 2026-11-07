"""CLI support for grid-cell spatial keys: --grid-cell-size in pcache-add and pcache-monitor, pcache-manage setup grid."""

from __future__ import annotations

import argparse
import sys
from unittest.mock import Mock, patch

import pytest

from partitioncache.cli.common_args import check_grid_key, grid_cell_size_for, parse_grid_cell_sizes
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs
from partitioncache.spatial_grid import GRID_GENERATION_KWARGS

STAR = (
    "SELECT p1.name, p2.name FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'school' AND p2.subtype = 'library' "
    "AND ST_DWithin(p1.geom, p2.geom, 500)"
)


# ----------------------------------------------------------------------------------- argument helpers


def test_parse_grid_cell_sizes() -> None:
    assert parse_grid_cell_sizes("cell200=200, cell400=400.5") == {"cell200": 200.0, "cell400": 400.5}
    for bad in ("cell200", "cell200=x", "cell200=0", "=200", "cell200=-5"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_grid_cell_sizes(bad)


def test_grid_cell_size_for_ignores_non_dict_attributes() -> None:
    assert grid_cell_size_for(argparse.Namespace(grid_cell_size={"cell200": 200.0}), "cell200") == 200.0
    assert grid_cell_size_for(argparse.Namespace(grid_cell_size={"cell200": 200.0}), "zipcode") is None
    assert grid_cell_size_for(argparse.Namespace(), "cell200") is None
    assert grid_cell_size_for(Mock(), "cell200") is None  # an unset Mock attribute is not a grid configuration


def test_check_grid_key() -> None:
    assert check_grid_key("cell200", "integer", "postgresql_array") is None
    assert check_grid_key("cell200", "integer", "postgresql_roaringbit") is None
    assert "integer" in str(check_grid_key("cell200", "geometry", "postgresql_array"))
    assert "bit backend" in str(check_grid_key("cell200", "integer", "postgresql_bit"))


def test_cli_generation_flags_give_the_lookup_hashes() -> None:
    """The flags the CLI forces for a grid key generate exactly the hashes the grid lookup searches for."""
    common = {"query": STAR, "partition_key": "cell200", "geometry_column": "geom", "grid_cell_size": 200, "min_component_size": 2}
    cli = {h for _, h in generate_all_query_hash_pairs(**common, keep_all_attributes=False, warn_no_partition_key=False, **GRID_GENERATION_KWARGS)}
    lookup = set(generate_all_hashes(**common, fix_attributes=False, **GRID_GENERATION_KWARGS))
    assert cli and cli == lookup


# ------------------------------------------------------------------------------------------- pcache-add


def _run_add(argv: list[str]):
    from partitioncache.cli import add_to_cache

    with patch.object(sys, "argv", ["pcache-add", *argv]), patch.object(add_to_cache, "generate_all_query_hash_pairs") as gen, patch(
        "partitioncache.push_to_query_fragment_queue", return_value=True
    ) as push, patch.object(add_to_cache, "load_environment_with_validation"):
        gen.return_value = [("SELECT DISTINCT v.c AS cell200 FROM x", "h1")]
        add_to_cache.main()
    return gen, push


def test_pcache_add_queue_forces_the_grid_flags(tmp_path) -> None:
    qf = tmp_path / "q.sql"
    qf.write_text(STAR)
    gen, push = _run_add(
        ["--queue", "--query-file", str(qf), "--partition-key", "cell200", "--partition-datatype", "integer", "--grid-cell-size", "cell200=200",
         "--cache-backend", "postgresql_array"]
    )
    kw = gen.call_args.kwargs
    assert kw["grid_cell_size"] == 200.0 and kw["geometry_column"] == "geom"
    assert kw["skip_partition_key_joins"] is True and kw["auto_detect_partition_join"] is False
    assert push.called


def test_pcache_add_leaves_other_keys_unchanged(tmp_path) -> None:
    qf = tmp_path / "q.sql"
    qf.write_text("SELECT * FROM t AS t1 WHERE t1.zipcode = 1")
    gen, _ = _run_add(
        ["--queue", "--query-file", str(qf), "--partition-key", "zipcode", "--partition-datatype", "integer", "--grid-cell-size", "cell200=200"]
    )
    assert "grid_cell_size" not in gen.call_args.kwargs


@pytest.mark.parametrize(
    "extra",
    [
        ["--partition-datatype", "geometry", "--cache-backend", "postgresql_array"],
        ["--partition-datatype", "integer", "--cache-backend", "postgresql_bit"],
        ["--partition-datatype", "integer", "--cache-backend", "postgresql_array", "--no-recompose"],
    ],
)
def test_pcache_add_rejects_unusable_grid_settings(tmp_path, extra: list[str]) -> None:
    qf = tmp_path / "q.sql"
    qf.write_text(STAR)
    with pytest.raises(SystemExit):
        _run_add(["--queue", "--query-file", str(qf), "--partition-key", "cell200", "--grid-cell-size", "cell200=200", *extra])


# --------------------------------------------------------------------------------------- pcache-monitor


@pytest.fixture
def monitor_args():
    args = Mock()
    args.min_component_size = 1
    args.follow_graph = True
    args.no_auto_detect_partition_join = False
    args.max_component_size = None
    args.partition_join_table = None
    args.no_warn_partition_key = False
    args.bucket_steps = 1.0
    args.skip_partition_key_joins = []
    args.max_conditions_removed = None
    args.disable_optimized_polling = False
    args.cache_backend = "postgresql_array"
    args.geometry_column = None
    args.grid_cell_size = {"cell200": 200.0}
    return args


@patch("partitioncache.cli.monitor_cache_queue.exit_event")
@patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
@patch("partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs")
@patch("partitioncache.cli.monitor_cache_queue.push_to_query_fragment_queue")
def test_monitor_fragment_thread_forces_grid_flags_per_key(mock_push, mock_generate, mock_pop, mock_exit, monitor_args) -> None:
    from partitioncache.cli.monitor_cache_queue import query_fragment_processor

    mock_generate.return_value = [("SELECT 1", "h1")]
    for key, grid in (("cell200", True), ("zipcode", False)):
        mock_exit.is_set.side_effect = [False, True]
        mock_pop.return_value = (STAR, key, "integer")
        query_fragment_processor(monitor_args, (None, None, None))
        kw = mock_generate.call_args.kwargs
        assert ("grid_cell_size" in kw) is grid
        if grid:
            assert kw["grid_cell_size"] == 200.0 and kw["geometry_column"] == "geom"
            assert kw["skip_partition_key_joins"] is True and kw["auto_detect_partition_join"] is False


@patch("partitioncache.cli.monitor_cache_queue.exit_event")
@patch("partitioncache.cli.monitor_cache_queue.pop_from_original_query_queue_blocking")
@patch("partitioncache.cli.monitor_cache_queue.generate_all_query_hash_pairs")
@patch("partitioncache.cli.monitor_cache_queue.push_to_query_fragment_queue")
def test_monitor_skips_grid_entries_with_a_bit_backend(mock_push, mock_generate, mock_pop, mock_exit, monitor_args) -> None:
    from partitioncache.cli.monitor_cache_queue import query_fragment_processor

    monitor_args.cache_backend = "postgresql_bit"
    mock_exit.is_set.side_effect = [False, True]
    mock_pop.return_value = (STAR, "cell200", "integer")
    query_fragment_processor(monitor_args, (None, None, None))
    assert not mock_generate.called and not mock_push.called


# ---------------------------------------------------------------------------------- pcache-manage setup


class TestSetupGrid:
    ENV = {"DB_HOST": "localhost", "DB_USER": "u", "DB_PASSWORD": "p", "DB_NAME": "d", "DB_PORT": "5432"}

    def _run(self, **kwargs):
        from partitioncache.cli.manage_cache import setup_grid

        with patch.dict("os.environ", self.ENV), patch("psycopg.connect") as mock_connect:
            cursor = mock_connect.return_value.cursor.return_value
            cursor.fetchone.return_value = (280_000.0, 5_236_000.0, 921_000.0, 6_101_000.0)
            setup_grid(**kwargs)
        return [c.args[0] if isinstance(c.args[0], str) else c.args[0].as_string(None) for c in cursor.execute.call_args_list]

    def test_installs_function_checks_extent_and_quotes_index_names(self) -> None:
        statements = self._run(table="pois; DROP TABLE users", geometry_column="geom", cell_sizes=[200.0], max_distance=1000,
                               create_indexes=True, index_prefix_column="subtype")
        assert "CREATE OR REPLACE FUNCTION public.partitioncache_grid_cell" in statements[0]
        assert 'FROM "pois; DROP TABLE users"' in statements[1]  # extent check
        assert statements[2] == (
            'CREATE INDEX IF NOT EXISTS "pois; DROP TABLE users_grid200_idx" ON "pois; DROP TABLE users" '
            '("partitioncache_grid_cell"("geom", 200))'
        )
        assert statements[3].startswith('CREATE INDEX IF NOT EXISTS "pois; DROP TABLE users_subtype_grid200_idx"')
        assert '("subtype", "partitioncache_grid_cell"("geom", 200))' in statements[3]
        assert statements[-1] == 'ANALYZE "pois; DROP TABLE users"'

    def test_rejects_a_table_outside_the_packing(self) -> None:
        from partitioncache.cli.manage_cache import setup_grid

        with patch.dict("os.environ", self.ENV), patch("psycopg.connect") as mock_connect:
            mock_connect.return_value.cursor.return_value.fetchone.return_value = (-10.0, 35.0, 30.0, 70.0)  # degrees, not metres
            with pytest.raises(ValueError, match="grid packing"):
                setup_grid(table="pois", geometry_column="geom", cell_sizes=[200.0], max_distance=1000)

    def test_cli_routes_setup_grid(self) -> None:
        from partitioncache.cli import manage_cache

        argv = ["pcache-manage", "setup", "grid", "--table", "pois", "--cell-size", "200", "--cell-size", "400", "--max-distance", "1000"]
        with patch.object(sys, "argv", argv), patch.object(manage_cache, "setup_grid") as mock_setup, patch.object(
            manage_cache, "load_environment_with_validation", create=True
        ):
            manage_cache.main()
        kw = mock_setup.call_args.kwargs
        assert kw["cell_sizes"] == [200.0, 400.0] and kw["max_distance"] == 1000.0 and kw["create_indexes"] is False

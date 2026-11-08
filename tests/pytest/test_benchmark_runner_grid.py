"""Grid-cell keys in the unified benchmark runner (examples/benchmark/run_benchmark.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("partitioncache.spatial_grid")  # the grid core; the research branch has it after the main sync

from partitioncache import spatial_grid  # noqa: E402
from partitioncache.query_processor import generate_all_hashes  # noqa: E402

RUNNER = Path(__file__).resolve().parents[2] / "examples" / "benchmark" / "run_benchmark.py"
QUERY = "SELECT p1.name, p2.name FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'school' AND p2.subtype = 'library' AND ST_DWithin(p1.geom, p2.geom, 500)"
GRID_KEY = {"datatype": "integer", "geometry_column": "geom", "grid_cell_size": 200, "grid_method": "CELL_AUTO"}


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("run_benchmark_under_test", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _config(runner, query_processing: dict | None = None):
    return SimpleNamespace(
        fact_table="pois",
        p0_alias="p1",
        partition_keys={"cell200": GRID_KEY, "zipcode": {"datatype": "integer"}},
        query_processing=query_processing or {"follow_graph": True, "population": {"max_conditions_removed": 1}, "lookup": {"max_conditions_removed": 2}},
        fact_stats={"table": "pois"},
        fragment_filter={},
    )


def test_grid_keys_are_recognised_and_skip_partition_key_joins(runner) -> None:
    assert runner._grid_cell_size(GRID_KEY) == 200.0
    assert runner._grid_cell_size({"datatype": "integer"}) is None
    assert runner._skip_pk_joins_for(GRID_KEY, {"skip_partition_key_joins": False}) is True


def test_population_projects_cells_with_the_lookup_hashes(runner) -> None:
    """The runner stores cell sets under exactly the hashes the grid lookup of apply_cache searches for."""
    config = _config(runner)
    handler = MagicMock()
    handler.underlying_handler.exists.return_value = False
    cache_manager = MagicMock()
    cache_manager.get_handler.return_value = handler
    executor = MagicMock()
    executor.execute_fragment.return_value = {1, 2, 3}

    stats = runner.populate_cache_for_query(executor, cache_manager, QUERY, ["cell200"], config)

    fragments = [c.args[0] for c in executor.execute_fragment.call_args_list]
    assert fragments and all("partitioncache_grid_cell(" in f for f in fragments)
    stored = {c.kwargs["key"] for c in handler.set_entry.call_args_list}
    lookup = set(
        generate_all_hashes(
            QUERY,
            "cell200",
            min_component_size=1,
            follow_graph=True,
            fix_attributes=False,
            max_conditions_removed=2,
            geometry_column="geom",
            grid_cell_size=200,
            partition_key_source_table="pois",
            **spatial_grid.GRID_GENERATION_KWARGS,
        )
    )
    assert stored and stored <= lookup
    assert stats["cell200"]["fragments_executed"] == len(fragments)
    assert stats["cell200"]["fragments_existing"] == stats["cell200"]["fragments_failed"] == stats["cell200"]["fragments_empty"] == 0


def test_population_counts_existing_failed_and_empty_fragments(runner) -> None:
    """The story measures reuse: a cached fragment, a failed one and an empty one must not all look like 'executed'."""
    config = _config(runner)
    handler = MagicMock()
    cache_manager = MagicMock()
    cache_manager.get_handler.return_value = handler
    seen: list[str] = []
    handler.underlying_handler.exists.side_effect = lambda h, partition_key: not seen and not seen.append(h)  # the first only
    calls = iter([RuntimeError("boom"), set()])

    def execute(sql):
        result = next(calls, {1, 2})
        if isinstance(result, Exception):
            raise result
        return result

    executor = MagicMock()
    executor.execute_fragment.side_effect = execute
    st = runner.populate_cache_for_query(executor, cache_manager, QUERY, ["cell200"], config)["cell200"]
    assert st["fragments_generated"] >= 3
    assert (st["fragments_existing"], st["fragments_failed"], st["fragments_empty"]) == (1, 1, 1)
    assert st["fragments_executed"] == st["fragments_generated"] - 2  # every fragment that ran without an error
    assert handler.set_entry.call_count == st["fragments_generated"] - 3  # the empty one is not stored


def test_eager_apply_routes_grid_keys_and_splits_the_star_setup(runner) -> None:
    config = _config(runner)
    cache_manager = MagicMock()
    star_sql = "CREATE TEMPORARY TABLE pc_nb_1 (c int, nc int) ON COMMIT DROP; INSERT INTO pc_nb_1 VALUES (1, 1); SELECT 1"
    stats = {"generated_variants": 3, "cache_hits": 2, "enhanced": 1, "grid_cells": 42, "grid_method": "CELL_STAR"}
    with (
        patch.object(runner.partitioncache, "apply_cache_lazy", return_value=(star_sql, stats)) as apply,
        patch.object(runner.partitioncache, "get_partition_keys") as get_keys,
    ):
        enhanced, pk_stats, _ = runner.apply_cache_to_query(cache_manager, QUERY, ["cell200"], config)

    kwargs = apply.call_args.kwargs
    assert kwargs["grid_cell_size"] == 200.0 and kwargs["grid_method"] == "CELL_AUTO" and kwargs["geometry_column"] == "geom"
    assert kwargs["p0_alias"] == "p1" and kwargs["max_conditions_removed"] == 2
    assert not get_keys.called  # its plain intersection would skip the widening
    assert enhanced == star_sql.replace("; ", ";")
    assert pk_stats["cell200"] == {"variants_checked": 3, "cache_hits": 2, "partition_keys_found": 42, "grid_method": "CELL_STAR"}


def test_grid_method_flag_overrides_the_key(runner) -> None:
    config = _config(runner)
    runner.GRID_METHOD = "CELL_ARRAY"
    try:
        assert runner._grid_apply_kwargs(GRID_KEY, config, 200.0)["grid_method"] == "CELL_ARRAY"
    finally:
        runner.GRID_METHOD = None
    assert runner._grid_apply_kwargs(GRID_KEY, config, 200.0)["grid_method"] == "CELL_AUTO"


def test_setup_rejects_bit_backends_and_checks_the_packing(runner) -> None:
    config = _config(runner)
    executor = MagicMock()
    with pytest.raises(SystemExit, match="sparse-set"):
        runner.setup_grid_keys(executor, config, "postgresql_bit")
    with patch.object(spatial_grid, "check_extent") as check:
        runner.setup_grid_keys(executor, config, "postgresql_array")
    cursor = executor.conn.cursor.return_value.__enter__.return_value
    assert cursor.execute.call_args_list[0].args[0] == spatial_grid.CELL_FUNCTION_DDL
    assert check.call_args.args[1:] == ("pois", "geom", 200.0, 2000.0)


def test_grid_kwargs_are_named_parameters_of_the_apply_function(runner) -> None:
    """The real signature, not a mock. Both apply functions take **kwargs, so binding proves nothing: apply_cache
    rejected max_conditions_removed at run time (handle_deprecated_kwargs). Every grid argument must be a named one."""
    import inspect

    import partitioncache

    def named(fn) -> set[str]:
        return {n for n, p in inspect.signature(fn).parameters.items() if p.kind is not p.VAR_KEYWORD}

    kwargs = set(runner._grid_apply_kwargs(GRID_KEY, _config(runner), 200.0))
    assert "max_conditions_removed" in kwargs
    assert kwargs <= named(partitioncache.apply_cache_lazy)
    assert not kwargs <= named(partitioncache.apply_cache)  # why the eager path goes through apply_cache_lazy


def test_population_counts_a_failed_store_as_failed(runner) -> None:
    config = _config(runner)
    handler = MagicMock()
    handler.underlying_handler.exists.return_value = False
    handler.set_entry.return_value = False
    cache_manager = MagicMock()
    cache_manager.get_handler.return_value = handler
    executor = MagicMock()
    executor.execute_fragment.return_value = {1, 2}
    st = runner.populate_cache_for_query(executor, cache_manager, QUERY, ["cell200"], config)["cell200"]
    assert st["fragments_failed"] == st["fragments_generated"] > 0

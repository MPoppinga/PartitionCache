"""Tests for the planner-cost gate (``min_query_cost``) in apply_cache / apply_cache_lazy and the
``estimate_cost`` implementations of the DB handlers."""

import json
import logging
import sys
from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

import pytest

from partitioncache.apply_cache import apply_cache, apply_cache_lazy
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.db_handler.postgres import PostgresDBHandler

# The package re-exports the function apply_cache, which shadows the submodule for string-based patch() on Python 3.10
_apply_cache_module = sys.modules["partitioncache.apply_cache"]

QUERY = "SELECT * FROM trips AS t WHERE t.fare > 10"


def _lazy_handler():
    handler = Mock(spec=AbstractCacheHandler_Lazy)
    handler.get_intersected_lazy.return_value = ("SELECT city_id FROM cache", 2)
    return handler


def _eager_handler():
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = ({1, 2, 3}, 2)
    return handler


@pytest.fixture(params=["lazy", "eager"])
def apply(request):
    """Run the lazy or the eager wrapper with a handler that always has cache hits."""
    if request.param == "lazy":
        handler = _lazy_handler()
        return lambda **kw: (apply_cache_lazy(QUERY, handler, "city_id", method="IN_SUBQUERY", p0_alias="t", **kw), handler)
    handler = _eager_handler()
    return lambda **kw: (apply_cache(QUERY, handler, "city_id", method="IN", p0_alias="t", **kw), handler)


class TestCostGate:
    def test_gate_off_by_default_adds_no_stats(self, apply):
        (enhanced, stats), _ = apply()
        assert enhanced != QUERY
        assert "cost_gated" not in stats and "query_cost" not in stats

    def test_estimator_without_threshold_is_not_called(self, apply):
        estimator = Mock(return_value=1.0)
        (enhanced, stats), _ = apply(cost_estimator=estimator)
        estimator.assert_not_called()
        assert enhanced != QUERY
        assert "cost_gated" not in stats

    def test_cheap_query_skips_cache_before_variant_generation(self, apply):
        estimator = Mock(return_value=42.4)
        with patch.object(_apply_cache_module, "generate_all_hashes") as gen:
            (enhanced, stats), handler = apply(cost_estimator=estimator, min_query_cost=1000)
        gen.assert_not_called()
        assert handler.method_calls == []
        estimator.assert_called_once_with(QUERY)
        assert enhanced == QUERY
        assert stats == {"generated_variants": 0, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, "cost_gated": 1, "query_cost": 42}

    def test_expensive_query_uses_cache_and_reports_cost(self, apply):
        (enhanced, stats), _ = apply(cost_estimator=lambda q: 5000.0, min_query_cost=1000)
        assert enhanced != QUERY
        assert stats["enhanced"] == 1
        assert stats["cost_gated"] == 0
        assert stats["query_cost"] == 5000

    def test_threshold_is_exclusive(self, apply):
        (enhanced, stats), _ = apply(cost_estimator=lambda q: 1000.0, min_query_cost=1000)
        assert stats["cost_gated"] == 0
        assert stats["enhanced"] == 1

    def test_estimator_returning_none_applies_cache(self, apply):
        (enhanced, stats), _ = apply(cost_estimator=lambda q: None, min_query_cost=1000)
        assert stats["enhanced"] == 1
        assert stats["cost_gated"] == 0
        assert "query_cost" not in stats

    def test_failing_estimator_applies_cache_and_warns(self, apply, caplog):
        def broken(query):
            raise RuntimeError("permission denied")

        with caplog.at_level(logging.WARNING, logger="PartitionCache"):
            (enhanced, stats), _ = apply(cost_estimator=broken, min_query_cost=1000)
        assert stats["enhanced"] == 1
        assert stats["cost_gated"] == 0
        assert "permission denied" in caplog.text

    def test_threshold_without_estimator_raises(self, apply):
        with pytest.raises(ValueError, match="cost_estimator"):
            apply(min_query_cost=1000)

    def test_gated_query_is_not_p0_rewritten(self, apply):
        (enhanced, stats), _ = apply(cost_estimator=lambda q: 1.0, min_query_cost=10, use_p0_table=True)
        assert enhanced == QUERY
        assert stats["p0_rewritten"] == 0

    @pytest.mark.parametrize("wrapper", [apply_cache, apply_cache_lazy])
    def test_spatial_path_is_gated_too(self, wrapper):
        handler = Mock()
        with patch.object(_apply_cache_module, "generate_all_hashes") as gen:
            enhanced, stats = wrapper(
                "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'",
                handler,
                "spatial_h3",
                geometry_column="geom",
                buffer_distance=500.0,
                cost_estimator=lambda q: 3.0,
                min_query_cost=10,
            )
        gen.assert_not_called()
        assert handler.method_calls == []
        assert stats["cost_gated"] == 1

    def test_gate_stats_survive_no_cache_hits(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "city_id", cost_estimator=lambda q: 500.0, min_query_cost=100)
        assert enhanced == QUERY
        assert stats["enhanced"] == 0
        assert stats["cost_gated"] == 0
        assert stats["query_cost"] == 500


class TestEstimatorResultValidation:
    def test_decimal_cost_is_used(self, apply):
        (enhanced, stats), _ = apply(cost_estimator=Mock(return_value=Decimal("42.4")), min_query_cost=1000)
        assert enhanced == QUERY
        assert stats["cost_gated"] == 1 and stats["query_cost"] == 42

    @pytest.mark.parametrize(
        "value", [float("nan"), float("inf"), "87.5", True, Decimal("sNaN"), 10**400], ids=["nan", "inf", "str", "bool", "snan", "huge_int"]
    )
    def test_non_finite_or_non_numeric_cost_applies_the_cache(self, apply, value):
        (enhanced, stats), _ = apply(cost_estimator=Mock(return_value=value), min_query_cost=1000)
        assert enhanced != QUERY
        assert stats["cost_gated"] == 0
        assert "query_cost" not in stats


class TestPostgresEstimateCost:
    def _handler(self, cursor):
        handler = PostgresDBHandler.__new__(PostgresDBHandler)
        handler.conn = MagicMock()
        handler.cur = cursor
        return handler

    def test_returns_total_cost_without_executing(self):
        handler = self._handler(MagicMock())
        cur = handler.conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ([{"Plan": {"Node Type": "Seq Scan", "Total Cost": 1234.56}}],)

        assert handler.estimate_cost(QUERY) == 1234.56
        # A binary cursor sends an unnamed extended-protocol statement (single statement only, nothing left
        # in psycopg's prepared-statement cache); the transaction is always rolled back
        handler.conn.cursor.assert_called_once_with(binary=True)
        cur.execute.assert_called_once_with(f"EXPLAIN (FORMAT JSON) {QUERY}")
        handler.conn.transaction.assert_called_once_with(force_rollback=True)
        handler.conn.commit.assert_not_called()
        handler.conn.rollback.assert_not_called()

    def test_failure_reraises_without_touching_the_callers_transaction(self):
        handler = self._handler(MagicMock())
        handler.conn.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError("syntax error")

        with pytest.raises(RuntimeError):
            handler.estimate_cost("SELEC 1")
        handler.conn.commit.assert_not_called()
        handler.conn.rollback.assert_not_called()


class TestMySQLEstimateCost:
    @pytest.fixture
    def handler_cls(self):
        mysql_handler = pytest.importorskip("partitioncache.db_handler.mysql")
        return mysql_handler.MySQLDBHandler

    def _handler(self, cls, plan):
        handler = cls.__new__(cls)
        handler.cur = MagicMock()
        handler.cur.fetchone.return_value = (json.dumps(plan),)
        return handler

    def test_returns_query_cost(self, handler_cls):
        handler = self._handler(handler_cls, {"query_block": {"select_id": 1, "cost_info": {"query_cost": "87.25"}}})
        assert handler.estimate_cost(QUERY) == 87.25
        handler.cur.execute.assert_called_once_with(f"EXPLAIN FORMAT=JSON {QUERY}")

    def test_mariadb_plan_without_cost_returns_none(self, handler_cls):
        handler = self._handler(handler_cls, {"query_block": {"select_id": 1, "table": {"table_name": "trips"}}})
        assert handler.estimate_cost(QUERY) is None


class TestBackendsWithoutCostModel:
    def test_sqlite_returns_none(self, tmp_path):
        from partitioncache.db_handler.sqlite import SQLiteDBHandler

        handler = SQLiteDBHandler(str(tmp_path / "db.sqlite"))
        try:
            assert handler.estimate_cost("SELECT 1") is None
        finally:
            handler.cur.close()
            handler.conn.close()

    def test_duckdb_returns_none(self):
        duckdb_handler = pytest.importorskip("partitioncache.db_handler.duckdb")
        handler = duckdb_handler.DuckDBHandler(":memory:")
        try:
            assert handler.estimate_cost("SELECT 1") is None
        finally:
            handler.close()

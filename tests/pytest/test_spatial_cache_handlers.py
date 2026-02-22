"""
Unit tests for spatial cache handlers (PostGIS H3 and PostGIS BBox).

Tests cover:
- query_processor changes: geometry_column and skip_partition_key_joins
- apply_cache changes: extend_query_with_spatial_filter_lazy, spatial mode in apply_cache_lazy
- apply_cache changes: extend_query_with_spatial_filter, spatial mode in apply_cache (non-lazy)
- Handler SQL generation: set_cache_lazy wrapping, get_intersected_sql, get_spatial_filter_lazy
- Handler WKB generation: get_spatial_filter (non-lazy)
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from partitioncache.apply_cache import (
    apply_cache,
    apply_cache_lazy,
    extend_query_with_spatial_filter,
    extend_query_with_spatial_filter_lazy,
)
from partitioncache.query_processor import (
    _build_select_clause,
    compute_buffer_distance,
    extract_distance_constraints,
    generate_all_hashes,
    generate_all_query_hash_pairs,
    generate_partial_queries,
)

# Get the actual module object (not the function) since partitioncache.__init__
# shadows the module attribute with `from partitioncache.apply_cache import apply_cache`
_apply_cache_module = sys.modules["partitioncache.apply_cache"]

# =============================================================================
# Tests for query_processor.py changes
# =============================================================================


class TestBuildSelectClauseGeometryColumn:
    """Test _build_select_clause with geometry_column parameter."""

    def test_geometry_column_used_instead_of_partition_key(self):
        """When geometry_column is set, SELECT should use it instead of partition_key."""
        result = _build_select_clause(
            strip_select=True,
            original_select_clause=None,
            table_aliases=["t1"],
            original_to_new_alias_mapping={},
            partition_key="spatial_h3",
            partition_join_alias=None,
            geometry_column="geom",
        )
        assert "geom" in result
        assert "spatial_h3" not in result
        assert result == "SELECT DISTINCT t1.geom"

    def test_geometry_column_none_uses_partition_key(self):
        """When geometry_column is None, SELECT should use partition_key as before."""
        result = _build_select_clause(
            strip_select=True,
            original_select_clause=None,
            table_aliases=["t1"],
            original_to_new_alias_mapping={},
            partition_key="pdb_id",
            partition_join_alias=None,
            geometry_column=None,
        )
        assert result == "SELECT DISTINCT t1.pdb_id"

    def test_geometry_column_with_partition_join(self):
        """When geometry_column is set with partition_join_alias, use partition join alias."""
        result = _build_select_clause(
            strip_select=True,
            original_select_clause=None,
            table_aliases=["t1"],
            original_to_new_alias_mapping={},
            partition_key="spatial_h3",
            partition_join_alias="p1",
            geometry_column="geom",
        )
        assert result == "SELECT DISTINCT p1.geom"

    def test_geometry_column_with_non_strip_select(self):
        """When strip_select=False and original_select_clause exists, geometry_column is ignored."""
        result = _build_select_clause(
            strip_select=False,
            original_select_clause="t1.name, t1.value",
            table_aliases=["t1"],
            original_to_new_alias_mapping={"orig": "t1"},
            partition_key="spatial_h3",
            partition_join_alias=None,
            geometry_column="geom",
        )
        assert "SELECT t1.name, t1.value" in result


class TestGeneratePartialQueriesSkipJoins:
    """Test generate_partial_queries with skip_partition_key_joins."""

    def test_skip_partition_key_joins_omits_equijoins(self):
        """When skip_partition_key_joins=True, no partition_key equijoins should be generated."""
        query = "SELECT DISTINCT t1.geom FROM points AS t1, lines AS t2 WHERE ST_DWithin(t1.geom, t2.geom, 100)"
        results = generate_partial_queries(
            query,
            partition_key="spatial_h3",
            min_component_size=1,
            follow_graph=True,
            skip_partition_key_joins=True,
            geometry_column="geom",
        )

        for r in results:
            # No equijoin on spatial_h3 should exist
            assert "spatial_h3 =" not in r.lower().replace(" ", "").replace("'", "")

    def test_skip_partition_key_joins_false_generates_equijoins(self):
        """When skip_partition_key_joins=False (default), equijoins are generated."""
        query = "SELECT DISTINCT t1.pdb_id FROM tab1 AS t1, tab2 AS t2 WHERE t1.x = t2.x"
        results = generate_partial_queries(
            query,
            partition_key="pdb_id",
            min_component_size=1,
            follow_graph=False,
            skip_partition_key_joins=False,
        )

        # Should have at least some results with partition_key join
        has_pk_join = any("pdb_id" in r for r in results if "=" in r)
        assert has_pk_join

    def test_geometry_column_in_select_clause(self):
        """When geometry_column is set, SELECT clause should use it."""
        query = "SELECT DISTINCT t1.geom FROM points AS t1 WHERE t1.size > 5"
        results = generate_partial_queries(
            query,
            partition_key="spatial_h3",
            min_component_size=1,
            follow_graph=True,
            geometry_column="geom",
        )

        assert len(results) > 0
        for r in results:
            # Should have geom in SELECT, not spatial_h3
            if "SELECT" in r.upper():
                assert "geom" in r.lower() or "size" in r.lower()

    def test_spatial_namespace_partition_key_no_false_matches(self):
        """A spatial namespace partition_key like 'spatial_h3' should not match any real column."""
        query = "SELECT * FROM poi AS t1, areas AS t2 WHERE ST_DWithin(t1.geom, t2.geom, 1000) AND t1.type = 'restaurant'"
        results = generate_partial_queries(
            query,
            partition_key="spatial_h3",
            min_component_size=1,
            follow_graph=True,
            skip_partition_key_joins=True,
            geometry_column="geom",
            warn_no_partition_key=False,
        )

        # Should produce valid query fragments
        assert len(results) > 0


class TestStarJoinSpatialReaddition:
    """Test partition-join table with spatial conditions are correctly re-added to fragments."""

    FLAT_SPATIAL_QUERY = (
        "SELECT t.trip_id "
        "FROM taxi_trips t, osm_pois p_start, osm_pois p_end "
        "WHERE t.duration_seconds > 2700 "
        "AND ST_DWithin(t.pickup_geom, p_start.geom, 200) "
        "AND p_start.poi_type = 'museum' "
        "AND ST_DWithin(t.dropoff_geom, p_end.geom, 200) "
        "AND p_end.poi_type = 'hotel'"
    )

    FLAT_SPATIAL_QUERY_COMPLEX = (
        "SELECT t.trip_id "
        "FROM taxi_trips t, osm_pois p_start, osm_pois p_end "
        "WHERE t.duration_seconds > 2700 "
        "AND t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3 "
        "AND t.pickup_hour BETWEEN 1 AND 4 "
        "AND ST_DWithin(t.pickup_geom, p_start.geom, 200) "
        "AND p_start.poi_type = 'museum' "
        "AND ST_DWithin(t.dropoff_geom, p_end.geom, 200) "
        "AND p_end.poi_type = 'hotel'"
    )

    TRIPLE_SPATIAL_QUERY = (
        "SELECT t.trip_id "
        "FROM taxi_trips t, osm_pois p_start, osm_pois p_end1, osm_pois p_end2 "
        "WHERE t.fare_amount / NULLIF(t.trip_distance, 0) > 8 "
        "AND t.pickup_hour BETWEEN 1 AND 4 "
        "AND ST_DWithin(t.pickup_geom, p_start.geom, 150) "
        "AND p_start.poi_type = 'bar' "
        "AND ST_DWithin(t.dropoff_geom, p_end1.geom, 150) "
        "AND p_end1.poi_type = 'bar' "
        "AND ST_DWithin(t.dropoff_geom, p_end2.geom, 200) "
        "AND p_end2.poi_type = 'hotel'"
    )

    def test_partition_join_generates_fragments(self):
        """Flat spatial query with partition_join_table generates fragments."""
        pairs = generate_all_query_hash_pairs(
            query=self.FLAT_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        assert len(pairs) > 0, "Should generate at least one fragment"
        # All fragments should SELECT trip_id from the partition-join table
        for fragment, _hash in pairs:
            assert "trip_id" in fragment.lower(), f"Fragment should select trip_id: {fragment}"

    def test_partition_join_fragments_contain_spatial_conditions(self):
        """Fragments should contain ST_DWithin spatial conditions, not PK equality joins."""
        pairs = generate_all_query_hash_pairs(
            query=self.FLAT_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        # Multi-table fragments should have ST_DWithin, not trip_id = trip_id joins
        multi_table_fragments = [f for f, _ in pairs if "osm_pois" in f.lower()]
        assert len(multi_table_fragments) > 0, "Should have multi-table fragments"
        for fragment in multi_table_fragments:
            assert "st_dwithin" in fragment.lower(), (
                f"Multi-table fragment should use ST_DWithin, not PK joins: {fragment}"
            )

    def test_partition_join_includes_single_table_conditions(self):
        """Partition-join fragments should include attribute conditions from the fact table."""
        pairs = generate_all_query_hash_pairs(
            query=self.FLAT_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        # At least one fragment should contain duration_seconds condition
        has_duration = any("duration_seconds" in f.lower() for f, _ in pairs)
        assert has_duration, "Should have at least one fragment with duration_seconds condition"

    def test_partition_join_complex_includes_other_functions(self):
        """Complex conditions like T_INDIRECT should be included in partition-join fragments."""
        pairs = generate_all_query_hash_pairs(
            query=self.FLAT_SPATIAL_QUERY_COMPLEX,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        # The T_INDIRECT condition (trip_distance * 1609.34 / NULLIF(ST_Distance(...))) should appear
        has_indirect = any("st_distance" in f.lower() for f, _ in pairs)
        assert has_indirect, (
            "Complex single-table conditions (other_functions) should be included in fragments"
        )

    def test_triple_spatial_generates_more_fragments(self):
        """A query with 3 spatial joins should generate more fragments than 2."""
        pairs_double = generate_all_query_hash_pairs(
            query=self.FLAT_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        pairs_triple = generate_all_query_hash_pairs(
            query=self.TRIPLE_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )

        assert len(pairs_triple) > len(pairs_double), (
            f"Triple spatial ({len(pairs_triple)}) should have more fragments than double ({len(pairs_double)})"
        )

    def test_hash_consistency_population_vs_lookup(self):
        """Hashes from generate_all_query_hash_pairs should match generate_all_hashes with same params."""
        pairs = generate_all_query_hash_pairs(
            query=self.FLAT_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )
        pair_hashes = {h for _, h in pairs}

        lookup_hashes = generate_all_hashes(
            query=self.FLAT_SPATIAL_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
            follow_graph=False,
        )
        lookup_hash_set = set(lookup_hashes)

        assert pair_hashes == lookup_hash_set, (
            f"Hash mismatch: population has {pair_hashes - lookup_hash_set} extra, "
            f"lookup has {lookup_hash_set - pair_hashes} extra"
        )


class TestGenerateAllHashesSpatialParams:
    """Test that generate_all_hashes passes through spatial params correctly."""

    def test_generates_hashes_with_geometry_column(self):
        """Verify hashes are generated when geometry_column is set."""
        query = "SELECT DISTINCT t1.geom FROM points AS t1 WHERE t1.size > 5"
        hashes = generate_all_hashes(
            query,
            partition_key="spatial_h3",
            geometry_column="geom",
            skip_partition_key_joins=True,
            warn_no_partition_key=False,
        )
        assert len(hashes) > 0

    def test_hash_pairs_with_spatial_params(self):
        """Verify hash pairs include spatial params."""
        query = "SELECT DISTINCT t1.geom FROM points AS t1 WHERE t1.size > 5"
        pairs = generate_all_query_hash_pairs(
            query,
            partition_key="spatial_h3",
            geometry_column="geom",
            skip_partition_key_joins=True,
            warn_no_partition_key=False,
        )
        assert len(pairs) > 0
        # Each pair is (query_text, hash)
        for query_text, query_hash in pairs:
            assert isinstance(query_text, str)
            assert isinstance(query_hash, str)
            assert len(query_hash) == 40  # SHA1 hex digest


# =============================================================================
# Tests for apply_cache.py changes
# =============================================================================


class TestExtendQueryWithSpatialFilterLazy:
    """Test the new extend_query_with_spatial_filter_lazy function."""

    def test_adds_st_dwithin_with_geography_cast(self):
        """Should add ST_DWithin with geography cast."""
        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'"
        spatial_sql = "SELECT ST_Union(geom) FROM cached_geoms"
        result = extend_query_with_spatial_filter_lazy(
            query=query,
            spatial_filter_sql=spatial_sql,
            geometry_column="geom",
            buffer_distance=500.0,
            p0_alias="p1",
        )
        result_upper = result.upper()
        assert "ST_DWITHIN" in result_upper
        assert "GEOGRAPHY" in result_upper
        assert "500.0" in result
        assert "geom" in result.lower()

    def test_empty_spatial_filter_returns_original(self):
        """Empty spatial filter should return original query."""
        query = "SELECT * FROM poi AS p1"
        result = extend_query_with_spatial_filter_lazy(
            query=query,
            spatial_filter_sql="",
            geometry_column="geom",
            buffer_distance=500.0,
        )
        assert result == query

    def test_auto_detects_table_alias(self):
        """When p0_alias is None, should auto-detect first table."""
        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        spatial_sql = "SELECT ST_Union(geom) FROM cached"
        result = extend_query_with_spatial_filter_lazy(
            query=query,
            spatial_filter_sql=spatial_sql,
            geometry_column="geom",
            buffer_distance=100.0,
        )
        result_upper = result.upper()
        assert "ST_DWITHIN" in result_upper
        assert "p1.geom" in result.lower()

    def test_no_table_raises_error(self):
        """Query without tables should raise ValueError."""
        with pytest.raises(ValueError, match="No table found"):
            extend_query_with_spatial_filter_lazy(
                query="SELECT 1",
                spatial_filter_sql="SELECT geom FROM x",
                geometry_column="geom",
                buffer_distance=100.0,
            )


class TestApplyCacheLazySpatialMode:
    """Test apply_cache_lazy with spatial parameters."""

    def _make_mock_handler(self, lazy_result=None, spatial_filter=None, existing_keys=None):
        """Create a mock cache handler with spatial support."""
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (lazy_result, 3 if lazy_result else 0)
        handler.get_spatial_filter_lazy = MagicMock(return_value=spatial_filter)
        if existing_keys is None:
            existing_keys = {"hash1"} if spatial_filter else set()
        handler.filter_existing_keys.return_value = existing_keys
        return handler

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_calls_get_spatial_filter_lazy(self, mock_hashes):
        """When geometry_column is set, should call get_spatial_filter_lazy."""
        mock_hashes.return_value = ["hash1", "hash2"]

        handler = self._make_mock_handler(
            lazy_result="SELECT unnest(pk) FROM cache",
            spatial_filter="SELECT ST_Union(geom) FROM buffered",
        )

        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        result, stats = apply_cache_lazy(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=500.0,
        )

        # Should have called get_spatial_filter_lazy
        handler.get_spatial_filter_lazy.assert_called_once()
        assert stats["enhanced"] == 1

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_no_cache_hits_returns_original(self, mock_hashes):
        """When no cache hits in spatial mode, return original query."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(lazy_result=None)

        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        result, stats = apply_cache_lazy(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=500.0,
        )

        assert result == query
        assert stats["enhanced"] == 0
        handler.get_spatial_filter_lazy.assert_not_called()

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_no_hits_skips_buffer_validation(self, mock_hashes):
        """When there are no cache hits, return original query even if buffer_distance is None."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(existing_keys=set())

        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        result, stats = apply_cache_lazy(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=None,
        )

        assert result == query
        assert stats["cache_hits"] == 0
        assert stats["enhanced"] == 0
        handler.get_spatial_filter_lazy.assert_not_called()

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_requires_buffer_distance(self, mock_hashes):
        """When geometry_column is set but buffer_distance is None, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(
            lazy_result="SELECT 1",
            spatial_filter="SELECT ST_Union(geom) FROM buffered",
            existing_keys={"hash1"},
        )

        with pytest.raises(ValueError, match="buffer_distance is required.*no distance constraints"):
            apply_cache_lazy(
                query="SELECT * FROM poi AS p1",
                cache_handler=handler,
                partition_key="spatial_h3",
                geometry_column="geom",
                buffer_distance=None,
            )

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_requires_spatial_handler(self, mock_hashes):
        """When handler lacks get_spatial_filter_lazy, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = ("SELECT 1", 1)
        handler.filter_existing_keys.return_value = {"hash1"}
        # Remove get_spatial_filter_lazy
        del handler.get_spatial_filter_lazy

        with pytest.raises(ValueError, match="does not support spatial filtering"):
            apply_cache_lazy(
                query="SELECT * FROM poi AS p1",
                cache_handler=handler,
                partition_key="spatial_h3",
                geometry_column="geom",
                buffer_distance=500.0,
            )

    def test_non_spatial_mode_unchanged(self):
        """When geometry_column is None, should behave as before."""
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)

        query = "SELECT * FROM table1 AS t1 WHERE t1.col > 5"
        result, stats = apply_cache_lazy(
            query=query,
            cache_handler=handler,
            partition_key="pdb_id",
        )

        assert result == query
        assert stats["enhanced"] == 0


# =============================================================================
# Tests for PostGIS H3 Cache Handler SQL generation
# =============================================================================


class TestPostGISH3SqlGeneration:
    """Test SQL generation for the H3 handler (without requiring actual PostGIS/h3-pg)."""

    def test_intersected_sql_single_key(self):
        """Test intersection SQL for a single key."""
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler

        # We can't instantiate without a DB, but we can test the static method logic
        # by calling _get_intersected_sql on a mock instance
        handler = MagicMock(spec=PostGISH3CacheHandler)
        handler.tableprefix = "test_prefix"
        handler._get_intersected_sql = PostGISH3CacheHandler._get_intersected_sql.__get__(handler)

        result = handler._get_intersected_sql({"hash1"}, "spatial_h3")
        result_str = result.as_string()
        assert "partition_keys" in result_str
        assert "hash1" in result_str

    def test_intersected_sql_multiple_keys(self):
        """Test intersection SQL for multiple keys."""
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler

        handler = MagicMock(spec=PostGISH3CacheHandler)
        handler.tableprefix = "test_prefix"
        handler._get_intersected_sql = PostGISH3CacheHandler._get_intersected_sql.__get__(handler)

        result = handler._get_intersected_sql({"hash1", "hash2"}, "spatial_h3")
        result_str = result.as_string()
        # Should use INTERSECT-based array intersection (intarray & only supports integer[], not bigint[])
        assert "INTERSECT" in result_str


# =============================================================================
# Tests for PostGIS BBox Cache Handler SQL generation
# =============================================================================


class TestPostGISBBoxSqlGeneration:
    """Test SQL generation for the BBox handler."""

    def test_intersected_sql_single_key(self):
        """Test intersection SQL for a single key."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

        handler = MagicMock(spec=PostGISBBoxCacheHandler)
        handler.tableprefix = "test_prefix"
        handler._get_intersected_sql = PostGISBBoxCacheHandler._get_intersected_sql.__get__(handler)

        result = handler._get_intersected_sql({"hash1"}, "spatial_bbox")
        result_str = result.as_string()
        assert "partition_keys" in result_str

    def test_intersected_sql_multiple_keys_uses_st_intersection(self):
        """Test that multiple keys use ST_Intersection chaining."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

        handler = MagicMock(spec=PostGISBBoxCacheHandler)
        handler.tableprefix = "test_prefix"
        handler._get_intersected_sql = PostGISBBoxCacheHandler._get_intersected_sql.__get__(handler)

        result = handler._get_intersected_sql({"hash1", "hash2"}, "spatial_bbox")
        result_str = result.as_string()
        assert "ST_Intersection" in result_str


# =============================================================================
# Tests for environment config
# =============================================================================


class TestEnvironmentConfig:
    """Test environment config for spatial handlers."""

    def test_postgis_h3_config_with_defaults(self, monkeypatch):
        """Test H3 config uses defaults for optional params."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_USER", "testuser")
        monkeypatch.setenv("DB_PASSWORD", "testpass")
        monkeypatch.setenv("DB_NAME", "testdb")

        config = EnvironmentConfigManager.get_postgis_h3_config()
        assert config["db_host"] == "localhost"
        assert config["db_port"] == 5432
        assert config["resolution"] == 9
        assert config["geometry_column"] == "geom"
        assert config["srid"] == 4326
        assert config["db_tableprefix"] == "partitioncache_h3"

    def test_postgis_h3_config_with_overrides(self, monkeypatch):
        """Test H3 config with explicit PG_H3_ overrides."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        monkeypatch.setenv("PG_H3_HOST", "h3host")
        monkeypatch.setenv("PG_H3_PORT", "5433")
        monkeypatch.setenv("PG_H3_USER", "h3user")
        monkeypatch.setenv("PG_H3_PASSWORD", "h3pass")
        monkeypatch.setenv("PG_H3_DB", "h3db")
        monkeypatch.setenv("PG_H3_RESOLUTION", "7")
        monkeypatch.setenv("PG_H3_GEOMETRY_COLUMN", "location")
        monkeypatch.setenv("PG_H3_SRID", "3857")
        monkeypatch.setenv("PG_H3_CACHE_TABLE_PREFIX", "custom_h3")

        config = EnvironmentConfigManager.get_postgis_h3_config()
        assert config["db_host"] == "h3host"
        assert config["db_port"] == 5433
        assert config["resolution"] == 7
        assert config["geometry_column"] == "location"
        assert config["srid"] == 3857
        assert config["db_tableprefix"] == "custom_h3"

    def test_postgis_h3_config_missing_host_raises(self, monkeypatch):
        """Test that missing host raises ValueError."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        # Clear all potential env vars
        for var in ["PG_H3_HOST", "DB_HOST", "PG_H3_PORT", "DB_PORT", "PG_H3_USER", "DB_USER", "PG_H3_PASSWORD", "DB_PASSWORD", "PG_H3_DB", "DB_NAME"]:
            monkeypatch.delenv(var, raising=False)

        with pytest.raises(ValueError, match="PG_H3_HOST or DB_HOST"):
            EnvironmentConfigManager.get_postgis_h3_config()

    def test_postgis_bbox_config_with_defaults(self, monkeypatch):
        """Test BBox config uses defaults for optional params."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_USER", "testuser")
        monkeypatch.setenv("DB_PASSWORD", "testpass")
        monkeypatch.setenv("DB_NAME", "testdb")

        config = EnvironmentConfigManager.get_postgis_bbox_config()
        assert config["db_host"] == "localhost"
        assert config["cell_size"] == 0.01
        assert config["geometry_column"] == "geom"
        assert config["srid"] == 4326
        assert config["db_tableprefix"] == "partitioncache_bbox"

    def test_postgis_bbox_config_with_overrides(self, monkeypatch):
        """Test BBox config with explicit PG_BBOX_ overrides."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        monkeypatch.setenv("PG_BBOX_HOST", "bboxhost")
        monkeypatch.setenv("PG_BBOX_PORT", "5434")
        monkeypatch.setenv("PG_BBOX_USER", "bboxuser")
        monkeypatch.setenv("PG_BBOX_PASSWORD", "bboxpass")
        monkeypatch.setenv("PG_BBOX_DB", "bboxdb")
        monkeypatch.setenv("PG_BBOX_CELL_SIZE", "0.05")
        monkeypatch.setenv("PG_BBOX_GEOMETRY_COLUMN", "the_geom")
        monkeypatch.setenv("PG_BBOX_SRID", "3857")
        monkeypatch.setenv("PG_BBOX_CACHE_TABLE_PREFIX", "custom_bbox")

        config = EnvironmentConfigManager.get_postgis_bbox_config()
        assert config["db_host"] == "bboxhost"
        assert config["db_port"] == 5434
        assert config["cell_size"] == 0.05
        assert config["geometry_column"] == "the_geom"
        assert config["srid"] == 3857
        assert config["db_tableprefix"] == "custom_bbox"

    def test_validate_environment_postgis_h3(self, monkeypatch):
        """Test validate_environment for postgis_h3."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_USER", "testuser")
        monkeypatch.setenv("DB_PASSWORD", "testpass")
        monkeypatch.setenv("DB_NAME", "testdb")

        assert EnvironmentConfigManager.validate_environment("postgis_h3") is True

    def test_validate_environment_postgis_bbox(self, monkeypatch):
        """Test validate_environment for postgis_bbox."""
        from partitioncache.cache_handler.environment_config import EnvironmentConfigManager

        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_USER", "testuser")
        monkeypatch.setenv("DB_PASSWORD", "testpass")
        monkeypatch.setenv("DB_NAME", "testdb")

        assert EnvironmentConfigManager.validate_environment("postgis_bbox") is True


# =============================================================================
# Tests for unified "geometry" datatype and abstract base class
# =============================================================================


class TestUnifiedGeometryDatatype:
    """Test that both spatial handlers use the unified 'geometry' datatype."""

    def test_h3_handler_supports_geometry_datatype(self):
        """PostGISH3CacheHandler.get_supported_datatypes() should return {'geometry'}."""
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler

        assert PostGISH3CacheHandler.get_supported_datatypes() == {"geometry"}

    def test_bbox_handler_supports_geometry_datatype(self):
        """PostGISBBoxCacheHandler.get_supported_datatypes() should return {'geometry'}."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

        assert PostGISBBoxCacheHandler.get_supported_datatypes() == {"geometry"}

    def test_h3_handler_is_spatial_abstract_subclass(self):
        """PostGISH3CacheHandler should be a subclass of PostGISSpatialAbstractCacheHandler."""
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler
        from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler

        assert issubclass(PostGISH3CacheHandler, PostGISSpatialAbstractCacheHandler)

    def test_bbox_handler_is_spatial_abstract_subclass(self):
        """PostGISBBoxCacheHandler should be a subclass of PostGISSpatialAbstractCacheHandler."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler
        from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler

        assert issubclass(PostGISBBoxCacheHandler, PostGISSpatialAbstractCacheHandler)

    def test_spatial_abstract_supports_geometry_datatype(self):
        """PostGISSpatialAbstractCacheHandler.get_supported_datatypes() should return {'geometry'}."""
        from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler

        assert PostGISSpatialAbstractCacheHandler.get_supported_datatypes() == {"geometry"}

    def test_both_handlers_share_same_datatype(self):
        """Both spatial handlers should report the same supported datatype."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler

        assert PostGISH3CacheHandler.get_supported_datatypes() == PostGISBBoxCacheHandler.get_supported_datatypes()


# =============================================================================
# Tests for non-lazy spatial functions
# =============================================================================


class TestExtendQueryWithSpatialFilter:
    """Test the non-lazy extend_query_with_spatial_filter function."""

    def test_adds_st_dwithin_with_wkb(self):
        """Should add ST_DWithin with WKB geometry literal."""
        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'"
        # Fake WKB bytes (just needs to be non-empty for the test)
        wkb = b"\x01\x02\x03\x04"
        result = extend_query_with_spatial_filter(
            query=query,
            spatial_filter_wkb=wkb,
            geometry_column="geom",
            buffer_distance=500.0,
            srid=4326,
            p0_alias="p1",
        )
        result_upper = result.upper()
        assert "ST_DWITHIN" in result_upper
        assert "GEOGRAPHY" in result_upper
        assert "500.0" in result
        assert "ST_GEOMFROMWKB" in result_upper
        assert "01020304" in result.lower()  # hex of WKB bytes

    def test_empty_wkb_returns_original(self):
        """Empty WKB should return original query."""
        query = "SELECT * FROM poi AS p1"
        result = extend_query_with_spatial_filter(
            query=query,
            spatial_filter_wkb=b"",
            geometry_column="geom",
            buffer_distance=500.0,
        )
        assert result == query

    def test_auto_detects_table_alias(self):
        """When p0_alias is None, should auto-detect first table."""
        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        wkb = b"\x01\x02"
        result = extend_query_with_spatial_filter(
            query=query,
            spatial_filter_wkb=wkb,
            geometry_column="geom",
            buffer_distance=100.0,
        )
        result_upper = result.upper()
        assert "ST_DWITHIN" in result_upper
        assert "p1.geom" in result.lower()

    def test_srid_in_geomfromwkb(self):
        """SRID should be passed to ST_GeomFromWKB."""
        query = "SELECT * FROM poi AS p1"
        wkb = b"\xAA\xBB"
        result = extend_query_with_spatial_filter(
            query=query,
            spatial_filter_wkb=wkb,
            geometry_column="geom",
            buffer_distance=100.0,
            srid=25832,
            p0_alias="p1",
        )
        assert "25832" in result

    def test_no_table_raises_error(self):
        """Query without tables should raise ValueError."""
        with pytest.raises(ValueError, match="No table found"):
            extend_query_with_spatial_filter(
                query="SELECT 1",
                spatial_filter_wkb=b"\x01",
                geometry_column="geom",
                buffer_distance=100.0,
            )


class TestApplyCacheSpatialMode:
    """Test apply_cache (non-lazy) with spatial parameters."""

    def _make_mock_handler(self, spatial_filter_wkb=None, srid=4326, existing_keys=None):
        """Create a mock cache handler with spatial support."""
        from partitioncache.cache_handler.abstract import AbstractCacheHandler

        handler = MagicMock(spec=AbstractCacheHandler)
        # get_spatial_filter now returns (bytes, srid) tuple or None
        if spatial_filter_wkb is not None:
            handler.get_spatial_filter = MagicMock(return_value=(spatial_filter_wkb, srid))
        else:
            handler.get_spatial_filter = MagicMock(return_value=None)
        handler.filter_existing_keys = MagicMock(return_value=existing_keys or set())
        handler.srid = srid
        return handler

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_calls_get_spatial_filter(self, mock_hashes):
        """When geometry_column is set, should call get_spatial_filter."""
        mock_hashes.return_value = ["hash1", "hash2"]

        handler = self._make_mock_handler(
            spatial_filter_wkb=b"\x01\x02\x03",
            existing_keys={"hash1", "hash2"},
        )

        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        result, stats = apply_cache(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=500.0,
        )

        handler.get_spatial_filter.assert_called_once()
        assert stats["enhanced"] == 1
        assert "ST_DWITHIN" in result.upper()

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_no_cache_hits_returns_original(self, mock_hashes):
        """When no spatial filter returned, return original query."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(spatial_filter_wkb=None)

        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        result, stats = apply_cache(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=500.0,
        )

        assert result == query
        assert stats["enhanced"] == 0

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_requires_buffer_distance(self, mock_hashes):
        """When geometry_column is set but buffer_distance is None, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler()

        with pytest.raises(ValueError, match="buffer_distance is required.*no distance constraints"):
            apply_cache(
                query="SELECT * FROM poi AS p1",
                cache_handler=handler,
                partition_key="spatial_h3",
                geometry_column="geom",
                buffer_distance=None,
            )

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_requires_spatial_handler(self, mock_hashes):
        """When handler lacks get_spatial_filter, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        from partitioncache.cache_handler.abstract import AbstractCacheHandler

        handler = MagicMock(spec=AbstractCacheHandler)
        del handler.get_spatial_filter

        with pytest.raises(ValueError, match="does not support spatial filtering"):
            apply_cache(
                query="SELECT * FROM poi AS p1",
                cache_handler=handler,
                partition_key="spatial_h3",
                geometry_column="geom",
                buffer_distance=500.0,
            )

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_spatial_mode_uses_srid_from_spatial_filter(self, mock_hashes):
        """Should use the SRID returned by get_spatial_filter in the generated query."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(
            spatial_filter_wkb=b"\x01\x02",
            srid=25832,
            existing_keys={"hash1"},
        )

        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'cafe'"
        result, stats = apply_cache(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=500.0,
        )

        assert "25832" in result
        assert stats["enhanced"] == 1


class TestGetSpatialFilterH3SqlGeneration:
    """Test get_spatial_filter SQL generation for H3 handler (mock-based)."""

    def test_get_spatial_filter_calls_lazy_and_executes(self):
        """get_spatial_filter should call get_spatial_filter_lazy, then execute SQL."""
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler

        handler = MagicMock(spec=PostGISH3CacheHandler)
        handler.get_spatial_filter_lazy = MagicMock(return_value="SELECT ST_Union(geom) FROM cells")
        handler.srid = 25832

        # Mock cursor to return WKB
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (b"\x01\x02\x03",)
        handler.cursor = mock_cursor

        # Bind the real method
        handler.get_spatial_filter = PostGISH3CacheHandler.get_spatial_filter.__get__(handler)

        result = handler.get_spatial_filter({"hash1"}, "spatial_h3", 500.0)
        assert result == (b"\x01\x02\x03", 25832)
        handler.get_spatial_filter_lazy.assert_called_once_with({"hash1"}, "spatial_h3", 500.0)

    def test_get_spatial_filter_returns_none_when_lazy_is_none(self):
        """get_spatial_filter should return None when lazy returns None."""
        from partitioncache.cache_handler.postgis_h3 import PostGISH3CacheHandler

        handler = MagicMock(spec=PostGISH3CacheHandler)
        handler.get_spatial_filter_lazy = MagicMock(return_value=None)
        handler.get_spatial_filter = PostGISH3CacheHandler.get_spatial_filter.__get__(handler)

        result = handler.get_spatial_filter({"hash1"}, "spatial_h3", 500.0)
        assert result is None


class TestGetSpatialFilterBBoxSqlGeneration:
    """Test get_spatial_filter SQL generation for BBox handler (mock-based)."""

    def test_get_spatial_filter_executes_intersect_sql(self):
        """get_spatial_filter should execute the intersection SQL and return WKB."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

        handler = MagicMock(spec=PostGISBBoxCacheHandler)
        handler.tableprefix = "test_prefix"
        handler.srid = 25832
        handler._get_partition_datatype = MagicMock(return_value="geometry")
        handler.filter_existing_keys = MagicMock(return_value={"hash1"})

        # Provide a real _get_intersected_sql to build real SQL
        handler._get_intersected_sql = PostGISBBoxCacheHandler._get_intersected_sql.__get__(handler)

        # Mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (b"\xAA\xBB\xCC",)
        handler.cursor = mock_cursor

        # Bind the real method
        handler.get_spatial_filter = PostGISBBoxCacheHandler.get_spatial_filter.__get__(handler)

        result = handler.get_spatial_filter({"hash1"}, "spatial_bbox", 0.0)
        assert result == (b"\xAA\xBB\xCC", 25832)
        mock_cursor.execute.assert_called_once()

    def test_get_spatial_filter_returns_none_when_no_keys(self):
        """get_spatial_filter should return None when no filtered keys."""
        from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

        handler = MagicMock(spec=PostGISBBoxCacheHandler)
        handler._get_partition_datatype = MagicMock(return_value="geometry")
        handler.filter_existing_keys = MagicMock(return_value=set())
        handler.get_spatial_filter = PostGISBBoxCacheHandler.get_spatial_filter.__get__(handler)

        result = handler.get_spatial_filter({"hash1"}, "spatial_bbox", 0.0)
        assert result is None


# ============================================================================
# Tests for extract_distance_constraints and compute_buffer_distance
# ============================================================================


class TestExtractDistanceConstraints:
    """Tests for extract_distance_constraints()."""

    def test_star_pattern_query(self):
        """Star pattern: all distances from p1."""
        query = (
            "SELECT * FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE p1.name LIKE '%Eis%' "
            "AND ST_DWithin(p1.geom, p3.geom, 300) "
            "AND ST_DWithin(p1.geom, p2.geom, 400)"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 2
        # Sorted by alias pair: (p1, p2, 400) and (p1, p3, 300)
        assert result[0] == ("p1", "p2", 400.0)
        assert result[1] == ("p1", "p3", 300.0)

    def test_chain_pattern_query(self):
        """Chain pattern: p1->p2->p3."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2, t3 AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p2.geom, p3.geom, 300)"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 2
        assert result[0] == ("p1", "p2", 400.0)
        assert result[1] == ("p2", "p3", 300.0)

    def test_no_st_dwithin(self):
        """Query with no distance constraints returns empty list."""
        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'"
        result = extract_distance_constraints(query)
        assert result == []

    def test_single_st_dwithin(self):
        """Single ST_DWithin call."""
        query = "SELECT * FROM t1 AS p1, t2 AS p2 WHERE ST_DWithin(p1.geom, p2.geom, 500)"
        result = extract_distance_constraints(query)
        assert len(result) == 1
        assert result[0] == ("p1", "p2", 500.0)

    def test_alias_sorting(self):
        """Aliases should be sorted within each tuple."""
        query = "SELECT * FROM t1 AS b, t2 AS a WHERE ST_DWithin(b.geom, a.geom, 100)"
        result = extract_distance_constraints(query)
        assert result[0] == ("a", "b", 100.0)

    def test_integer_distance(self):
        """Integer distances should be parsed as floats."""
        query = "SELECT * FROM t1 AS p1, t2 AS p2 WHERE ST_DWithin(p1.geom, p2.geom, 1000)"
        result = extract_distance_constraints(query)
        assert result[0][2] == 1000.0

    def test_float_distance(self):
        """Float distances should be parsed correctly."""
        query = "SELECT * FROM t1 AS p1, t2 AS p2 WHERE ST_DWithin(p1.geom, p2.geom, 123.45)"
        result = extract_distance_constraints(query)
        assert result[0][2] == 123.45

    # --- Comparison-based distance constraint tests ---

    def test_sqrt_power_less_than(self):
        """SQRT(POWER(...)) < value → extract upper bound."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2 "
            "WHERE SQRT(POWER(p1.x - p2.x, 2) + POWER(p1.y - p2.y, 2)) < 0.008"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 1
        assert result[0] == ("p1", "p2", 0.008)

    def test_abs_sqrt_less_equal(self):
        """ABS(SQRT(...)) <= value → extract upper bound."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2 "
            "WHERE ABS(SQRT(POWER(p1.x - p2.x, 2))) <= 0.1"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 1
        assert result[0] == ("p1", "p2", 0.1)

    def test_dist_between(self):
        """DIST(...) BETWEEN x AND y → extract y (upper bound)."""
        query = (
            "SELECT * FROM t1, t2 "
            "WHERE DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 1
        assert result[0] == ("t1", "t2", 3.6)

    def test_dist_between_mixed_case(self):
        """Mixed-case BETWEEN/AND keywords are handled correctly."""
        query = (
            "SELECT * FROM t1, t2 "
            "WHERE DIST(t1.g, t2.g) Between 1.6 And 3.6"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 1
        assert result[0] == ("t1", "t2", 3.6)

    def test_greater_than_skipped(self):
        """DIST(...) > value → lower bound only, should be skipped."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2 "
            "WHERE DIST(p1.g, p2.g) > 5"
        )
        result = extract_distance_constraints(query)
        assert result == []

    def test_mixed_st_dwithin_and_comparison(self):
        """Both ST_DWithin and comparison patterns → all edges extracted."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2, t3 AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND SQRT(POWER(p2.x - p3.x, 2) + POWER(p2.y - p3.y, 2)) <= 0.1"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 2
        assert result[0] == ("p1", "p2", 400.0)
        assert result[1] == ("p2", "p3", 0.1)

    def test_multiple_comparison_distances(self):
        """Multiple comparison-based distances → correct tuples for each pair."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2, t3 AS p3 "
            "WHERE SQRT(POWER(p1.x - p2.x, 2)) <= 0.1 "
            "AND SQRT(POWER(p2.x - p3.x, 2)) <= 0.2"
        )
        result = extract_distance_constraints(query)
        assert len(result) == 2
        assert result[0] == ("p1", "p2", 0.1)
        assert result[1] == ("p2", "p3", 0.2)


class TestComputeBufferDistance:
    """Tests for compute_buffer_distance()."""

    def test_star_pattern(self):
        """Star: ST_DWithin(p1,p2,400) + ST_DWithin(p1,p3,300) → diameter=700."""
        query = (
            "SELECT * FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p1.geom, p3.geom, 300)"
        )
        assert compute_buffer_distance(query) == 700.0

    def test_chain_pattern(self):
        """Chain: ST_DWithin(p1,p2,400) + ST_DWithin(p2,p3,300) → diameter=700."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2, t3 AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p2.geom, p3.geom, 300)"
        )
        assert compute_buffer_distance(query) == 700.0

    def test_single_st_dwithin(self):
        """Single edge → diameter equals that distance."""
        query = "SELECT * FROM t1 AS p1, t2 AS p2 WHERE ST_DWithin(p1.geom, p2.geom, 500)"
        assert compute_buffer_distance(query) == 500.0

    def test_no_st_dwithin(self):
        """No ST_DWithin → 0.0."""
        query = "SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'"
        assert compute_buffer_distance(query) == 0.0

    def test_equal_distances_star(self):
        """Star: ST_DWithin(p1,p2,400) + ST_DWithin(p1,p3,400) → diameter=800."""
        query = (
            "SELECT * FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p1.geom, p3.geom, 400)"
        )
        assert compute_buffer_distance(query) == 800.0

    def test_triangle_pattern(self):
        """Triangle: p1-p2=400, p2-p3=300, p1-p3=500 → diameter=500 (direct p1-p3)."""
        query = (
            "SELECT * FROM t AS p1, t AS p2, t AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p2.geom, p3.geom, 300) "
            "AND ST_DWithin(p1.geom, p3.geom, 500)"
        )
        assert compute_buffer_distance(query) == 500.0

    def test_real_query_q1(self):
        """Real q1 query: star pattern with 300 + 400 → diameter=700."""
        query = (
            "SELECT p1.name, p2.name, p3.name "
            "FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'pharmacy' "
            "AND p3.subtype = 'supermarket' AND p3.name LIKE '%ALDI%' "
            "AND ST_DWithin(p1.geom, p3.geom, 300) "
            "AND ST_DWithin(p1.geom, p2.geom, 400)"
        )
        assert compute_buffer_distance(query) == 700.0

    def test_real_query_q2(self):
        """Real q2 query: star pattern with 400 + 400 → diameter=800."""
        query = (
            "SELECT p1.name, p2.name, p3.name "
            "FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'swimming_pool' "
            "AND p3.subtype = 'supermarket' AND p3.name ILIKE '%Edeka%' "
            "AND ST_DWithin(p1.geom, p3.geom, 400) "
            "AND ST_DWithin(p1.geom, p2.geom, 400)"
        )
        assert compute_buffer_distance(query) == 800.0

    def test_arithmetic_distance_chain(self):
        """Chain with SQRT-based distances: 0.1 + 0.2 → diameter=0.3."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2, t3 AS p3 "
            "WHERE SQRT(POWER(p1.x - p2.x, 2)) <= 0.1 "
            "AND SQRT(POWER(p2.x - p3.x, 2)) <= 0.2"
        )
        assert compute_buffer_distance(query) == pytest.approx(0.3)

    def test_mixed_st_dwithin_and_arithmetic(self):
        """Mixed: ST_DWithin(p1,p2,400) + SQRT(p2,p3)<=0.1 → diameter=400.1."""
        query = (
            "SELECT * FROM t1 AS p1, t2 AS p2, t3 AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400) "
            "AND SQRT(POWER(p2.x - p3.x, 2) + POWER(p2.y - p3.y, 2)) <= 0.1"
        )
        assert compute_buffer_distance(query) == pytest.approx(400.1)


class TestAutoDerivBufferDistanceLazy:
    """Tests for auto-derivation of buffer_distance in apply_cache_lazy()."""

    def _make_mock_handler(self, spatial_filter=None):
        """Create a mock lazy cache handler with spatial support."""
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_spatial_filter_lazy = MagicMock(return_value=spatial_filter)
        handler.filter_existing_keys.return_value = {"hash1"} if spatial_filter else set()
        return handler

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_auto_derive_buffer_from_query(self, mock_hashes):
        """When buffer_distance=None and query has ST_DWithin, auto-derive from graph diameter."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(
            spatial_filter="SELECT ST_Union(geom) FROM cached",
        )

        query = (
            "SELECT * FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE p1.name LIKE '%Eis%' "
            "AND ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p1.geom, p3.geom, 300)"
        )
        result, stats = apply_cache_lazy(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=None,
        )

        # Should have called get_spatial_filter_lazy directly (no get_intersected_lazy)
        handler.get_spatial_filter_lazy.assert_called_once()
        handler.get_intersected_lazy.assert_not_called()
        # The result should be enhanced (not original query)
        assert "ST_DWITHIN" in result.upper() or "st_dwithin" in result.lower()

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_explicit_buffer_overrides_auto(self, mock_hashes):
        """When buffer_distance is explicitly set, it should be used instead of auto-derive."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(
            spatial_filter="SELECT ST_Union(geom) FROM cached",
        )

        query = (
            "SELECT * FROM pois AS p1, pois AS p2 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 400)"
        )
        result, stats = apply_cache_lazy(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=999.0,
        )

        # Should succeed and use the explicit buffer distance
        assert "999" in result


class TestAutoDerivBufferDistanceNonLazy:
    """Tests for auto-derivation of buffer_distance in apply_cache()."""

    @patch.object(_apply_cache_module, "generate_all_hashes")
    def test_auto_derive_buffer_from_query(self, mock_hashes):
        """When buffer_distance=None and query has ST_DWithin, auto-derive from graph diameter."""
        mock_hashes.return_value = ["hash1"]

        handler = MagicMock(spec=["get", "get_intersected", "exists", "filter_existing_keys", "get_spatial_filter"])
        handler.get_intersected.return_value = {1, 2, 3}
        handler.filter_existing_keys.return_value = {"hash1"}
        handler.get_spatial_filter.return_value = (b"\x00\x01\x02", 25832)

        query = (
            "SELECT * FROM pois AS p1, pois AS p2, pois AS p3 "
            "WHERE p1.name LIKE '%Eis%' "
            "AND ST_DWithin(p1.geom, p2.geom, 400) "
            "AND ST_DWithin(p1.geom, p3.geom, 300)"
        )
        result, stats = apply_cache(
            query=query,
            cache_handler=handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=None,
        )

        # Should have called get_spatial_filter (no error raised)
        handler.get_spatial_filter.assert_called_once()
        # The enhanced query should contain ST_DWithin with derived buffer 700
        assert "700" in result

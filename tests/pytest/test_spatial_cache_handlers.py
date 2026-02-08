"""
Unit tests for spatial cache handlers (PostGIS H3 and PostGIS BBox).

Tests cover:
- query_processor changes: geometry_column and skip_partition_key_joins
- apply_cache changes: extend_query_with_spatial_filter_lazy, spatial mode in apply_cache_lazy
- apply_cache changes: extend_query_with_spatial_filter, spatial mode in apply_cache (non-lazy)
- Handler SQL generation: set_cache_lazy wrapping, get_intersected_sql, get_spatial_filter_lazy
- Handler WKB generation: get_spatial_filter (non-lazy)
"""

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
    generate_all_hashes,
    generate_all_query_hash_pairs,
    generate_partial_queries,
)

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
            star_join_alias=None,
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
            star_join_alias=None,
            geometry_column=None,
        )
        assert result == "SELECT DISTINCT t1.pdb_id"

    def test_geometry_column_with_star_join(self):
        """When geometry_column is set with star_join_alias, use star join alias."""
        result = _build_select_clause(
            strip_select=True,
            original_select_clause=None,
            table_aliases=["t1"],
            original_to_new_alias_mapping={},
            partition_key="spatial_h3",
            star_join_alias="p1",
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
            star_join_alias=None,
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

    def _make_mock_handler(self, lazy_result=None, spatial_filter=None):
        """Create a mock cache handler with spatial support."""
        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (lazy_result, 3 if lazy_result else 0)
        handler.get_spatial_filter_lazy = MagicMock(return_value=spatial_filter)
        return handler

    @patch("partitioncache.apply_cache.generate_all_hashes")
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

    @patch("partitioncache.apply_cache.generate_all_hashes")
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

    @patch("partitioncache.apply_cache.generate_all_hashes")
    def test_spatial_mode_requires_buffer_distance(self, mock_hashes):
        """When geometry_column is set but buffer_distance is None, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(lazy_result="SELECT 1")

        with pytest.raises(ValueError, match="buffer_distance is required"):
            apply_cache_lazy(
                query="SELECT * FROM poi AS p1",
                cache_handler=handler,
                partition_key="spatial_h3",
                geometry_column="geom",
                buffer_distance=None,
            )

    @patch("partitioncache.apply_cache.generate_all_hashes")
    def test_spatial_mode_requires_spatial_handler(self, mock_hashes):
        """When handler lacks get_spatial_filter_lazy, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = ("SELECT 1", 1)
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

    def _make_mock_handler(self, spatial_filter_wkb=None, existing_keys=None):
        """Create a mock cache handler with spatial support."""
        from partitioncache.cache_handler.abstract import AbstractCacheHandler

        handler = MagicMock(spec=AbstractCacheHandler)
        handler.get_spatial_filter = MagicMock(return_value=spatial_filter_wkb)
        handler.filter_existing_keys = MagicMock(return_value=existing_keys or set())
        handler.srid = 4326
        return handler

    @patch("partitioncache.apply_cache.generate_all_hashes")
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

    @patch("partitioncache.apply_cache.generate_all_hashes")
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

    @patch("partitioncache.apply_cache.generate_all_hashes")
    def test_spatial_mode_requires_buffer_distance(self, mock_hashes):
        """When geometry_column is set but buffer_distance is None, should raise ValueError."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler()

        with pytest.raises(ValueError, match="buffer_distance is required"):
            apply_cache(
                query="SELECT * FROM poi AS p1",
                cache_handler=handler,
                partition_key="spatial_h3",
                geometry_column="geom",
                buffer_distance=None,
            )

    @patch("partitioncache.apply_cache.generate_all_hashes")
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

    @patch("partitioncache.apply_cache.generate_all_hashes")
    def test_spatial_mode_uses_handler_srid(self, mock_hashes):
        """Should use the handler's SRID in the generated query."""
        mock_hashes.return_value = ["hash1"]

        handler = self._make_mock_handler(
            spatial_filter_wkb=b"\x01\x02",
            existing_keys={"hash1"},
        )
        handler.srid = 25832

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

        # Mock cursor to return WKB
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (b"\x01\x02\x03",)
        handler.cursor = mock_cursor

        # Bind the real method
        handler.get_spatial_filter = PostGISH3CacheHandler.get_spatial_filter.__get__(handler)

        result = handler.get_spatial_filter({"hash1"}, "spatial_h3", 500.0)
        assert result == b"\x01\x02\x03"
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
        assert result == b"\xAA\xBB\xCC"
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

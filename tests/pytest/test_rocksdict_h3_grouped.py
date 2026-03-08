"""
Tests for grouped match set support in RocksDictCacheHandler and
the thin RocksDictH3GroupedCacheHandler spatial subclass.

Tests cover:
- _build_spatial_grouped_query generates correct multi-column SELECT
- _grouped_intersection connected-component algorithm
- RocksDictCacheHandler polymorphic set_cache/get_intersected for grouped data
- RocksDictH3GroupedCacheHandler spatial filter (mocked PostgreSQL)
"""

from unittest.mock import MagicMock, patch

import pytest

from partitioncache.cache_handler.rocks_dict import _grouped_intersection
from partitioncache.query_processor import (
    _build_spatial_grouped_query,
    generate_partial_queries,
)

# =============================================================================
# Tests for _build_spatial_grouped_query
# =============================================================================


class TestBuildSpatialGroupedQuery:
    """Test _build_spatial_grouped_query helper for multi-column geometry SELECT."""

    def test_single_alias_returns_select_distinct(self):
        """Single-alias fragment should return standard SELECT DISTINCT."""
        result = _build_spatial_grouped_query(
            table_aliases=["t1"],
            table_list_with_alias=["pois AS t1"],
            where_conditions=["t1.type = 'cafe'"],
            geometry_column="geom",
        )
        assert result.upper().startswith("SELECT DISTINCT")
        assert "t1.geom" in result
        # Should NOT have numbered columns for single alias
        assert "geom_1" not in result
        assert "geom_2" not in result

    def test_multi_alias_returns_separate_columns(self):
        """Multi-alias fragment should return separate geometry columns."""
        result = _build_spatial_grouped_query(
            table_aliases=["t1", "t2"],
            table_list_with_alias=["pois AS t1", "pois AS t2"],
            where_conditions=["ST_DWithin(t1.geom, t2.geom, 500)"],
            geometry_column="geom",
        )
        assert "t1.geom AS geom_1" in result
        assert "t2.geom AS geom_2" in result
        assert "UNION ALL" not in result.upper()
        # Single SELECT with both columns
        assert result.upper().count("SELECT") == 1

    def test_three_aliases_three_columns(self):
        """Three-alias fragment should produce three geometry columns."""
        result = _build_spatial_grouped_query(
            table_aliases=["t1", "t2", "t3"],
            table_list_with_alias=["pois AS t1", "pois AS t2", "pois AS t3"],
            where_conditions=["ST_DWithin(t1.geom, t2.geom, 500)", "ST_DWithin(t2.geom, t3.geom, 300)"],
            geometry_column="geom",
        )
        assert "t1.geom AS geom_1" in result
        assert "t2.geom AS geom_2" in result
        assert "t3.geom AS geom_3" in result
        assert "UNION ALL" not in result.upper()

    def test_no_where_conditions(self):
        """Multi-alias without WHERE conditions should omit WHERE clause."""
        result = _build_spatial_grouped_query(
            table_aliases=["t1", "t2"],
            table_list_with_alias=["pois AS t1", "pois AS t2"],
            where_conditions=[],
            geometry_column="geom",
        )
        assert "geom_1" in result
        assert "geom_2" in result
        assert "WHERE" not in result.upper()


class TestSpatialGroupedInFragments:
    """Test that generate_partial_queries produces multi-column SELECT for spatial fragments."""

    SELF_JOIN_QUERY = (
        "SELECT * FROM pois AS p1, pois AS p2 "
        "WHERE ST_DWithin(p1.geom, p2.geom, 500) "
        "AND p1.type = 'cafe' AND p2.type = 'restaurant'"
    )

    SINGLE_TABLE_QUERY = "SELECT * FROM pois AS p1 WHERE p1.type = 'cafe'"

    def test_multi_alias_spatial_has_separate_columns(self):
        """Multi-alias spatial fragments should have separate geometry columns."""
        results = generate_partial_queries(
            self.SELF_JOIN_QUERY,
            partition_key="spatial_h3",
            min_component_size=2,
            follow_graph=True,
            skip_partition_key_joins=True,
            geometry_column="geom",
            warn_no_partition_key=False,
        )
        multi_col = [r for r in results if "geom_1" in r and "geom_2" in r]
        assert len(multi_col) > 0, f"Should have multi-column fragments, got: {results}"
        for frag in multi_col:
            assert "UNION ALL" not in frag.upper()

    def test_single_alias_spatial_no_numbered_columns(self):
        """Single-alias spatial fragments should NOT have numbered columns."""
        results = generate_partial_queries(
            self.SINGLE_TABLE_QUERY,
            partition_key="spatial_h3",
            min_component_size=1,
            follow_graph=True,
            skip_partition_key_joins=True,
            geometry_column="geom",
            warn_no_partition_key=False,
        )
        for r in results:
            assert "geom_1" not in r, f"Single-alias should not have numbered columns: {r}"

    def test_non_spatial_no_grouped_columns(self):
        """Non-spatial fragments should never have grouped geometry columns."""
        query = "SELECT * FROM tab1 AS t1, tab2 AS t2 WHERE t1.x = t2.x AND t1.val > 5"
        results = generate_partial_queries(
            query,
            partition_key="pdb_id",
            min_component_size=1,
            follow_graph=True,
            skip_partition_key_joins=False,
        )
        for r in results:
            assert "geom_1" not in r, f"Non-spatial should not have geometry columns: {r}"


# =============================================================================
# Tests for _grouped_intersection algorithm
# =============================================================================


class TestGroupedIntersection:
    """Test the connected-component grouped intersection algorithm."""

    def test_single_fragment_returns_all_cells(self):
        """Single fragment: return union of all cells."""
        groups = [[frozenset({1, 2}), frozenset({3, 4})]]
        result = _grouped_intersection(groups)
        assert result == {1, 2, 3, 4}

    def test_two_fragments_overlapping(self):
        """Two fragments with overlapping groups should survive."""
        # Fragment 0: group with cells {1, 2}
        # Fragment 1: group with cells {2, 3}
        # They share cell 2, so both survive
        groups = [
            [frozenset({1, 2})],
            [frozenset({2, 3})],
        ]
        result = _grouped_intersection(groups)
        assert result == {1, 2, 3}

    def test_two_fragments_no_overlap(self):
        """Two fragments with no overlapping cells should return empty."""
        groups = [
            [frozenset({1, 2})],
            [frozenset({3, 4})],
        ]
        result = _grouped_intersection(groups)
        assert result == set()

    def test_three_fragments_transitive_connection(self):
        """Three fragments connected transitively should survive."""
        # Fragment 0: {1, 2}
        # Fragment 1: {2, 3}  -- shares cell 2 with fragment 0
        # Fragment 2: {3, 4}  -- shares cell 3 with fragment 1
        # All connected transitively
        groups = [
            [frozenset({1, 2})],
            [frozenset({2, 3})],
            [frozenset({3, 4})],
        ]
        result = _grouped_intersection(groups)
        assert result == {1, 2, 3, 4}

    def test_three_fragments_partial_connection(self):
        """Component spanning only 2 of 3 fragments should NOT survive."""
        # Fragment 0: {1, 2}
        # Fragment 1: {2, 3}  -- connected to fragment 0
        # Fragment 2: {5, 6}  -- disconnected from both
        groups = [
            [frozenset({1, 2})],
            [frozenset({2, 3})],
            [frozenset({5, 6})],
        ]
        result = _grouped_intersection(groups)
        assert result == set()

    def test_multiple_groups_per_fragment(self):
        """Multiple groups per fragment: only connected ones survive."""
        # Fragment 0: two groups — {1, 2} and {10, 11}
        # Fragment 1: one group — {2, 3}
        # Only {1,2} from F0 connects to {2,3} from F1 via cell 2
        # {10, 11} from F0 has no match in F1
        groups = [
            [frozenset({1, 2}), frozenset({10, 11})],
            [frozenset({2, 3})],
        ]
        result = _grouped_intersection(groups)
        assert result == {1, 2, 3}
        assert 10 not in result
        assert 11 not in result

    def test_empty_fragments(self):
        """Empty input should return empty set."""
        assert _grouped_intersection([]) == set()

    def test_empty_groups_in_fragment(self):
        """Fragment with no groups: nothing can connect."""
        groups = [
            [frozenset({1, 2})],
            [],
        ]
        result = _grouped_intersection(groups)
        assert result == set()

    def test_identical_groups_across_fragments(self):
        """Same cells in both fragments: trivially connected."""
        groups = [
            [frozenset({5, 6, 7})],
            [frozenset({5, 6, 7})],
        ]
        result = _grouped_intersection(groups)
        assert result == {5, 6, 7}

    def test_multiple_surviving_components(self):
        """Two independent connected components, both spanning all fragments."""
        # Component A: F0:{1,2} - F1:{2,3}  (share cell 2)
        # Component B: F0:{10,11} - F1:{11,12}  (share cell 11)
        groups = [
            [frozenset({1, 2}), frozenset({10, 11})],
            [frozenset({2, 3}), frozenset({11, 12})],
        ]
        result = _grouped_intersection(groups)
        assert result == {1, 2, 3, 10, 11, 12}


# =============================================================================
# Tests for RocksDictCacheHandler with grouped data
# =============================================================================


class TestRocksDictGroupedStorage:
    """Test RocksDictCacheHandler with list[frozenset[int]] storage."""

    @pytest.fixture
    def handler(self, tmp_path):
        """Create a RocksDictCacheHandler with a temp directory."""
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        db_path = str(tmp_path / "test_grouped.rocksdb")
        h = RocksDictCacheHandler(db_path)
        h.register_partition_key("spatial_h3", "geometry")
        yield h
        h.close()

    def test_set_and_get_grouped(self, handler):
        """Store and retrieve grouped match sets."""
        groups = [frozenset({1, 2}), frozenset({3, 4})]
        assert handler.set_cache("hash1", groups, "spatial_h3")
        result = handler.get("hash1", "spatial_h3")
        assert result == groups

    def test_set_and_get_flat(self, handler):
        """Flat sets still work normally."""
        handler.register_partition_key("pk_int", "integer")
        flat = {10, 20, 30}
        assert handler.set_cache("hash2", flat, "pk_int")
        result = handler.get("hash2", "pk_int")
        assert result == flat

    def test_intersect_grouped(self, handler):
        """Grouped intersection uses connected-component algorithm."""
        handler.set_cache("h1", [frozenset({1, 2}), frozenset({10, 11})], "spatial_h3")
        handler.set_cache("h2", [frozenset({2, 3})], "spatial_h3")

        result, count = handler.get_intersected({"h1", "h2"}, "spatial_h3")
        assert count == 2
        # Only {1,2} from h1 connects to {2,3} from h2 via cell 2
        assert result == {1, 2, 3}

    def test_intersect_flat(self, handler):
        """Flat intersection still works for set[int]."""
        handler.register_partition_key("pk_int", "integer")
        handler.set_cache("a", {1, 2, 3, 4}, "pk_int")
        handler.set_cache("b", {2, 3, 5}, "pk_int")

        result, count = handler.get_intersected({"a", "b"}, "pk_int")
        assert count == 2
        assert result == {2, 3}

    def test_intersect_grouped_no_overlap(self, handler):
        """Grouped with no overlap returns empty set."""
        handler.set_cache("h1", [frozenset({1, 2})], "spatial_h3")
        handler.set_cache("h2", [frozenset({3, 4})], "spatial_h3")

        result, count = handler.get_intersected({"h1", "h2"}, "spatial_h3")
        assert count == 2
        assert result == set()

    def test_intersect_single_key(self, handler):
        """Single key: return all cells from all groups."""
        handler.set_cache("h1", [frozenset({1, 2}), frozenset({5, 6})], "spatial_h3")

        result, count = handler.get_intersected({"h1"}, "spatial_h3")
        assert count == 1
        assert result == {1, 2, 5, 6}

    def test_auto_registers_geometry_datatype(self, tmp_path):
        """Storing grouped data auto-registers geometry datatype."""
        from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

        db_path = str(tmp_path / "test_auto_dt.rocksdb")
        h = RocksDictCacheHandler(db_path)
        # Don't pre-register — let set_cache auto-detect
        h.set_cache("h1", [frozenset({1, 2})], "auto_pk")
        assert h.get_datatype("auto_pk") == "geometry"
        h.close()


# =============================================================================
# Tests for RocksDictH3GroupedCacheHandler
# =============================================================================


class TestRocksDictH3GroupedHandler:
    """Test the thin spatial subclass (mocked PostgreSQL)."""

    @pytest.fixture
    def handler(self, tmp_path):
        """Create handler with mocked PostgreSQL connection."""
        with patch("partitioncache.cache_handler.rocksdict_h3_grouped.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_psycopg.connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor

            from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler

            db_path = str(tmp_path / "test_h3.rocksdb")
            h = RocksDictH3GroupedCacheHandler(
                db_path=db_path,
                db_name="test",
                db_host="localhost",
                db_user="test",
                db_password="test",
                db_port=5432,
                resolution=9,
                srid=4326,
            )
            h.register_partition_key("spatial_h3", "geometry")
            h._mock_cursor = mock_cursor  # expose for test assertions
            yield h
            h.db.close()  # close RocksDict only (PG is mocked)

    def test_repr(self, handler):
        assert repr(handler) == "rocksdict_h3_grouped"

    def test_spatial_filter_type(self, handler):
        assert handler.spatial_filter_type == "geometry"

    def test_spatial_filter_includes_buffer(self, handler):
        assert handler.spatial_filter_includes_buffer is True

    def test_geom_to_h3_cell(self, handler):
        """geom_to_h3_cell should execute H3 conversion SQL."""
        handler._mock_cursor.fetchone.return_value = (12345,)
        result = handler.geom_to_h3_cell(b"\x00\x01\x02")
        assert result == 12345
        handler._mock_cursor.execute.assert_called_once()

    def test_geom_to_h3_cell_none(self, handler):
        """geom_to_h3_cell returns None for NULL result."""
        handler._mock_cursor.fetchone.return_value = (None,)
        result = handler.geom_to_h3_cell(b"\x00\x01\x02")
        assert result is None

    def test_get_spatial_filter(self, handler):
        """get_spatial_filter reconstructs geometry from intersected cells."""
        # Store grouped data
        handler.set_cache("h1", [frozenset({100, 200})], "spatial_h3")
        handler.set_cache("h2", [frozenset({200, 300})], "spatial_h3")

        # Mock PostgreSQL to return WKB geometry
        handler._mock_cursor.fetchone.return_value = (b"\x01\x02\x03\x04",)

        result = handler.get_spatial_filter({"h1", "h2"}, "spatial_h3", buffer_distance=500.0)
        assert result is not None
        wkb, srid = result
        assert wkb == b"\x01\x02\x03\x04"
        assert srid == 4326

    def test_get_spatial_filter_no_hits(self, handler):
        """get_spatial_filter returns None when no cache hits."""
        result = handler.get_spatial_filter({"nonexistent"}, "spatial_h3", buffer_distance=500.0)
        assert result is None

    def test_get_spatial_filter_no_overlap(self, handler):
        """get_spatial_filter returns None when groups don't overlap."""
        handler.set_cache("h1", [frozenset({100})], "spatial_h3")
        handler.set_cache("h2", [frozenset({200})], "spatial_h3")

        result = handler.get_spatial_filter({"h1", "h2"}, "spatial_h3", buffer_distance=500.0)
        assert result is None

"""
Tests for grouped match set support in RocksDictCacheHandler and
the thin RocksDictH3GroupedCacheHandler spatial subclass.

Tests cover:
- _build_spatial_grouped_query generates correct multi-column SELECT
- _grouped_intersection connected-component algorithm
- _grouped_kring_intersection k-ring expansion + cross-fragment intersection
- RocksDictCacheHandler polymorphic set_cache/get_intersected for grouped data
- RocksDictH3GroupedCacheHandler H3 cell filter (mocked PostgreSQL)
"""

from unittest.mock import MagicMock, patch

import pytest

from partitioncache.cache_handler.rocks_dict import _grouped_intersection, _grouped_kring_intersection
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
# Tests for _grouped_kring_intersection algorithm
# =============================================================================


class TestGroupedKringIntersection:
    """Test the k-ring expansion + cross-fragment intersection algorithm."""

    @pytest.fixture(autouse=True)
    def _check_h3(self):
        """Skip if h3 library is not installed."""
        pytest.importorskip("h3")

    def test_basic_two_fragments_overlapping_kring(self):
        """Two fragments with cells that overlap after k-ring expansion."""
        import h3

        # Get two cells that are within k=2 of each other
        center_hex = h3.latlng_to_cell(48.0, 11.0, 9)
        neighbors = [h3.str_to_int(c) for c in h3.grid_disk(center_hex, 1)]
        cell_a = h3.str_to_int(center_hex)
        cell_b = neighbors[1]  # Adjacent cell

        groups = [
            [frozenset({cell_a})],
            [frozenset({cell_b})],
        ]
        result = _grouped_kring_intersection(groups, k=1)
        # Both cells expanded by k=1 should overlap
        assert len(result) > 0
        # Both original cells should be in the result (they're within k=1 of each other)
        assert cell_a in result
        assert cell_b in result

    def test_no_overlap_with_k_zero(self):
        """With k=0, non-overlapping cells should return empty (same as connected-component)."""
        import h3

        cell_a = h3.str_to_int(h3.latlng_to_cell(48.0, 11.0, 9))
        cell_b = h3.str_to_int(h3.latlng_to_cell(48.1, 11.1, 9))  # Far enough apart

        groups = [
            [frozenset({cell_a})],
            [frozenset({cell_b})],
        ]
        result = _grouped_kring_intersection(groups, k=0)
        # k=0 means no expansion, cells are different → empty intersection
        assert result == set()

    def test_multi_city_only_correct_survives(self):
        """Multi-city scenario: only the city with matches in ALL fragments survives."""
        import h3

        # NYC-ish cells (convert hex strings to int, matching what geom_to_h3_cell returns)
        nyc_cell_a = h3.str_to_int(h3.latlng_to_cell(40.7, -74.0, 9))
        nyc_cell_b = h3.str_to_int(h3.latlng_to_cell(40.71, -74.0, 9))

        # LA-ish cells (far from NYC)
        la_cell = h3.str_to_int(h3.latlng_to_cell(34.0, -118.2, 9))

        # Fragment 0 has matches in both NYC and LA
        # Fragment 1 has matches only in NYC
        groups = [
            [frozenset({nyc_cell_a}), frozenset({la_cell})],
            [frozenset({nyc_cell_b})],
        ]
        result = _grouped_kring_intersection(groups, k=2)

        # NYC cells should survive (within k=2 of each other)
        nyc_a_disk = {h3.str_to_int(c) for c in h3.grid_disk(h3.int_to_str(nyc_cell_a), 2)}
        nyc_b_disk = {h3.str_to_int(c) for c in h3.grid_disk(h3.int_to_str(nyc_cell_b), 2)}
        nyc_overlap = nyc_a_disk & nyc_b_disk
        assert len(nyc_overlap & result) > 0, "NYC area should survive intersection"

        # LA cell should NOT survive (too far from any Fragment 1 match)
        la_disk = {h3.str_to_int(c) for c in h3.grid_disk(h3.int_to_str(la_cell), 2)}
        assert len(la_disk & result) == 0 or la_cell not in result, "LA should not survive — no Fragment 1 match nearby"

    def test_k_computation_from_buffer_distance(self):
        """Verify k computation from buffer distance and resolution."""
        import math

        import h3

        resolution = 9
        edge_length = h3.average_hexagon_edge_length(resolution, unit="m")

        # 150m buffer → k=1 (since edge ~174m)
        k_150 = math.ceil(150.0 / edge_length)
        assert k_150 == 1

        # 300m buffer → k=2
        k_300 = math.ceil(300.0 / edge_length)
        assert k_300 == 2

        # 500m buffer → k=3
        k_500 = math.ceil(500.0 / edge_length)
        assert k_500 == 3

    def test_single_fragment_returns_expanded_cells(self):
        """Single fragment: return all cells expanded by k-ring."""
        import h3

        cell_hex = h3.latlng_to_cell(48.0, 11.0, 9)
        cell = h3.str_to_int(cell_hex)
        groups = [[frozenset({cell})]]
        result = _grouped_kring_intersection(groups, k=1)

        # Should be the full grid_disk of k=1 (as integers)
        expected = {h3.str_to_int(c) for c in h3.grid_disk(cell_hex, 1)}
        assert result == expected

    def test_empty_fragments(self):
        """Empty input should return empty set."""
        assert _grouped_kring_intersection([], k=1) == set()

    def test_empty_groups_in_fragment(self):
        """Fragment with no groups: nothing can expand."""
        import h3

        cell = h3.str_to_int(h3.latlng_to_cell(48.0, 11.0, 9))
        groups = [
            [frozenset({cell})],
            [],
        ]
        result = _grouped_kring_intersection(groups, k=1)
        # Second fragment expands to empty set → intersection is empty
        assert result == set()


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
        assert handler.spatial_filter_type == "h3_cell_ids"

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

    def test_get_h3_cell_filter_no_hits(self, handler):
        """get_h3_cell_filter returns None when no cache hits."""
        result = handler.get_h3_cell_filter({"nonexistent"}, "spatial_h3", buffer_distance=500.0)
        assert result is None

    def test_h3_cell_mode_default(self, handler):
        """Default h3_cell_mode should be 'inline'."""
        assert handler.h3_cell_mode == "inline"

    def test_h3_cell_config_attributes(self, tmp_path):
        """H3 cell config attributes should be stored on the handler."""
        with patch("partitioncache.cache_handler.rocksdict_h3_grouped.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_psycopg.connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor

            from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler

            db_path = str(tmp_path / "test_h3_config.rocksdb")
            h = RocksDictH3GroupedCacheHandler(
                db_path=db_path,
                db_name="test",
                db_host="localhost",
                db_user="test",
                db_password="test",
                db_port=5432,
                h3_cell_mode="mv",
                h3_cell_table="pois_h3_cells",
                h3_cell_column="h3_cell_id",
                h3_cell_id_column="id",
            )
            assert h.h3_cell_mode == "mv"
            assert h.h3_cell_table == "pois_h3_cells"
            assert h.h3_cell_column == "h3_cell_id"
            assert h.h3_cell_id_column == "id"
            h.db.close()


class TestRocksDictH3GroupedHandlerKring:
    """Test get_h3_cell_filter with actual H3 k-ring expansion."""

    @pytest.fixture(autouse=True)
    def _check_h3(self):
        """Skip if h3 library is not installed."""
        pytest.importorskip("h3")

    @pytest.fixture
    def handler(self, tmp_path):
        """Create handler with mocked PostgreSQL connection."""
        with patch("partitioncache.cache_handler.rocksdict_h3_grouped.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_psycopg.connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor

            from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler

            db_path = str(tmp_path / "test_h3_kring.rocksdb")
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
            yield h
            h.db.close()

    def test_get_h3_cell_filter_single_fragment(self, handler):
        """Single fragment with k-ring expansion."""
        import h3

        cell = h3.str_to_int(h3.latlng_to_cell(48.0, 11.0, 9))
        handler.set_cache("h1", [frozenset({cell})], "spatial_h3")

        result = handler.get_h3_cell_filter({"h1"}, "spatial_h3", buffer_distance=200.0)
        assert result is not None
        # Should include the original cell and its k-ring neighbors
        assert cell in result
        assert len(result) > 1  # k=2 for 200m at res 9

    def test_get_h3_cell_filter_two_fragments_overlapping(self, handler):
        """Two fragments with overlapping k-rings."""
        import h3

        center_hex = h3.latlng_to_cell(48.0, 11.0, 9)
        neighbors = [h3.str_to_int(c) for c in h3.grid_disk(center_hex, 1)]
        cell_a = h3.str_to_int(center_hex)
        cell_b = neighbors[1]

        handler.set_cache("h1", [frozenset({cell_a})], "spatial_h3")
        handler.set_cache("h2", [frozenset({cell_b})], "spatial_h3")

        result = handler.get_h3_cell_filter({"h1", "h2"}, "spatial_h3", buffer_distance=200.0)
        assert result is not None
        assert len(result) > 0

    def test_get_h3_cell_filter_zero_buffer(self, handler):
        """Zero buffer distance means k=0 (no expansion)."""
        import h3

        cell_a = h3.str_to_int(h3.latlng_to_cell(48.0, 11.0, 9))
        cell_b = h3.str_to_int(h3.latlng_to_cell(48.1, 11.1, 9))

        handler.set_cache("h1", [frozenset({cell_a})], "spatial_h3")
        handler.set_cache("h2", [frozenset({cell_b})], "spatial_h3")

        result = handler.get_h3_cell_filter({"h1", "h2"}, "spatial_h3", buffer_distance=0.0)
        # Different cells with k=0 → empty intersection
        assert result is None


class TestH3GroupedGetInstance:
    """Regression: get_instance must forward the PostgreSQL/H3 config to __init__.

    The base RocksDictAbstractCacheHandler.get_instance only forwards db_path/read_only and
    constructed cls(db_path, read_only=read_only), which raised
    'RocksDictAbstractCacheHandler.get_instance() got an unexpected keyword argument db_host'
    when called with the full h3-grouped config (db_host, db_name, ...).
    """

    def _reset_singleton(self):
        from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler as H

        H._instance = None
        H._refcount = 0
        H._current_path = None

    def test_get_instance_forwards_pg_config(self, tmp_path):
        from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler as H

        self._reset_singleton()
        config = {
            "db_path": str(tmp_path / "h3.rocksdb"),
            "db_host": "localhost",
            "db_port": 5432,
            "db_user": "u",
            "db_password": "p",
            "db_name": "d",
            "resolution": 9,
            "srid": 4326,
        }
        try:
            with (
                patch("partitioncache.cache_handler.rocksdict_abstract.Rdict"),
                patch("partitioncache.cache_handler.rocksdict_h3_grouped.psycopg.connect", return_value=MagicMock()) as mock_connect,
            ):
                handler = H.get_instance(**config)
                assert handler is not None
                assert handler.resolution == 9
                assert handler.srid == 4326
                # PG connection params were forwarded to psycopg.connect
                mock_connect.assert_called_once()
                _, kwargs = mock_connect.call_args
                assert kwargs["host"] == "localhost"
                assert kwargs["dbname"] == "d"
        finally:
            self._reset_singleton()

    def test_get_instance_is_singleton_per_path(self, tmp_path):
        from partitioncache.cache_handler.rocksdict_h3_grouped import RocksDictH3GroupedCacheHandler as H

        self._reset_singleton()
        config = {
            "db_path": str(tmp_path / "h3.rocksdb"),
            "db_host": "localhost",
            "db_port": 5432,
            "db_user": "u",
            "db_password": "p",
            "db_name": "d",
        }
        try:
            with (
                patch("partitioncache.cache_handler.rocksdict_abstract.Rdict"),
                patch("partitioncache.cache_handler.rocksdict_h3_grouped.psycopg.connect", return_value=MagicMock()),
            ):
                h1 = H.get_instance(**config)
                h2 = H.get_instance(**config)
                assert h1 is h2  # same path → same singleton instance
        finally:
            self._reset_singleton()

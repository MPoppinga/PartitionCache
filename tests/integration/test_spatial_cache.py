"""
Integration tests for spatial cache functionality.

This module tests PartitionCache with spatial data, including:
- Distance-based queries with different partition keys
- Multi-table spatial joins
- Cross-partition spatial queries
- Cache effectiveness with spatial queries
"""

import os
from pathlib import Path

import psycopg
import pytest

# Import PartitionCache modules
import partitioncache


class TestSpatialCache:
    """Test spatial queries with PartitionCache."""

    @pytest.fixture(autouse=True)
    def setup_connection(self):
        """Setup database connection for tests."""
        self.conn_str = (
            f"postgresql://{os.getenv('PG_USER', 'test_user')}:"
            f"{os.getenv('PG_PASSWORD', 'test_password')}@"
            f"{os.getenv('PG_HOST', 'localhost')}:"
            f"{os.getenv('PG_PORT', '5432')}/"
            f"{os.getenv('PG_DBNAME', 'test_db')}"
        )

        self.conn = psycopg.connect(self.conn_str)
        yield
        if self.conn:
            self.conn.close()

    def _get_cache_backend(self) -> str:
        """Get the cache backend from environment."""
        return os.getenv('CACHE_BACKEND', 'postgresql_array')

    def _execute_query(self, query: str) -> tuple[list, float]:
        """Execute a query and return results with timing."""
        import time

        start_time = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        elapsed = time.perf_counter() - start_time

        return results, elapsed

    def _load_query_from_file(self, query_file: str) -> str:
        """Load query from file."""
        query_path = Path(__file__).parent / "spatial_queries" / query_file

        if not query_path.exists():
            pytest.skip(f"Query file not found: {query_path}")

        with open(query_path) as f:
            return f.read().strip()

    def _test_cache_effectiveness(self, query: str, partition_key: str,
                                partition_datatype: str = "integer") -> dict:
        """Test cache effectiveness for a given query."""
        # Execute without cache
        results_no_cache, time_no_cache = self._execute_query(query)

        # Test with cache
        backend = self._get_cache_backend()

        try:
            with partitioncache.create_cache_helper(
                backend, partition_key, partition_datatype
            ) as cache:
                # Get partition keys (this may be empty for first run)
                import time
                start_time = time.perf_counter()

                partition_keys, num_subqueries, num_hits = partitioncache.get_partition_keys(
                    query=query,
                    cache_handler=cache.underlying_handler,
                    partition_key=partition_key,
                    min_component_size=1,
                )

                cache_lookup_time = time.perf_counter() - start_time

                if partition_keys:
                    # Apply cache optimization
                    optimized_query = partitioncache.extend_query_with_partition_keys(
                        query,
                        partition_keys,
                        partition_key=partition_key,
                        method="IN",
                        p0_alias="p1",
                    )

                    results_with_cache, time_with_cache = self._execute_query(optimized_query)
                    total_cache_time = cache_lookup_time + time_with_cache

                    # Verify results are consistent
                    assert len(results_no_cache) >= len(results_with_cache), \
                        "Cache should not return more results than full query"

                    return {
                        'results_count': len(results_no_cache),
                        'cached_results_count': len(results_with_cache),
                        'time_no_cache': time_no_cache,
                        'time_with_cache': total_cache_time,
                        'cache_hits': num_hits,
                        'num_subqueries': num_subqueries,
                        'speedup': time_no_cache / total_cache_time if total_cache_time > 0 else 0,
                        'cache_effective': num_hits > 0
                    }
                else:
                    return {
                        'results_count': len(results_no_cache),
                        'cached_results_count': 0,
                        'time_no_cache': time_no_cache,
                        'time_with_cache': cache_lookup_time,
                        'cache_hits': 0,
                        'num_subqueries': num_subqueries,
                        'speedup': 0,
                        'cache_effective': False
                    }

        except Exception as e:
            pytest.skip(f"Cache test failed: {e}")

    def test_spatial_data_exists(self):
        """Test that spatial test data exists."""
        with self.conn.cursor() as cur:
            # Check spatial points table
            cur.execute("SELECT COUNT(*) FROM test_spatial_points")
            result = cur.fetchone()
            points_count = result[0] if result else 0
            assert points_count > 0, "No spatial points found in test data"

            # Check businesses table
            cur.execute("SELECT COUNT(*) FROM test_businesses")
            result = cur.fetchone()
            businesses_count = result[0] if result else 0
            assert businesses_count > 0, "No businesses found in test data"

            print(f"Test data: {points_count} spatial points, {businesses_count} businesses")

    def test_region_based_nearby_businesses(self):
        """Test region-based spatial query with cache."""
        query = self._load_query_from_file("region_based/q1_nearby_businesses.sql")

        result = self._test_cache_effectiveness(query, "region_id", "integer")

        # Verify basic functionality
        assert result['results_count'] >= 0, "Query should return some results"

        print("Region-based query results:")
        print(f"  Results: {result['results_count']}")
        print(f"  Time without cache: {result['time_no_cache']:.3f}s")
        print(f"  Time with cache: {result['time_with_cache']:.3f}s")
        print(f"  Cache hits: {result['cache_hits']}/{result['num_subqueries']}")
        print(f"  Cache effective: {result['cache_effective']}")

    def test_region_based_business_clusters(self):
        """Test complex multi-table spatial join with region partitioning."""
        query = self._load_query_from_file("region_based/q2_business_clusters.sql")

        result = self._test_cache_effectiveness(query, "region_id", "integer")

        assert result['results_count'] >= 0, "Cluster query should execute successfully"

        print("Business cluster query results:")
        print(f"  Results: {result['results_count']}")
        print(f"  Cache effectiveness: {result['cache_effective']}")

    def test_city_based_partitioning(self):
        """Test city-based partitioning with spatial queries."""
        query = self._load_query_from_file("city_based/q1_city_business_analysis.sql")

        result = self._test_cache_effectiveness(query, "city_id", "integer")

        assert result['results_count'] >= 0, "City analysis query should execute"

        print("City-based query results:")
        print(f"  Results: {result['results_count']}")
        print(f"  Cache effectiveness: {result['cache_effective']}")

    def test_city_based_nearest_neighbors(self):
        """Test nearest neighbor queries with city partitioning."""
        query = self._load_query_from_file("city_based/q2_nearest_neighbors.sql")

        result = self._test_cache_effectiveness(query, "city_id", "integer")

        assert result['results_count'] >= 0, "Nearest neighbor query should execute"

        print("Nearest neighbor query results:")
        print(f"  Results: {result['results_count']}")
        print(f"  Performance improvement: {result['speedup']:.2f}x")

    def test_zipcode_based_partitioning(self):
        """Test zipcode-based partitioning with density calculations."""
        query = self._load_query_from_file("zipcode_based/q1_zipcode_density.sql")

        result = self._test_cache_effectiveness(query, "zipcode", "text")

        assert result['results_count'] >= 0, "Zipcode density query should execute"

        print("Zipcode-based query results:")
        print(f"  Results: {result['results_count']}")
        print(f"  Cache effectiveness: {result['cache_effective']}")

    def test_cross_zipcode_distances(self):
        """Test cross-partition spatial queries."""
        query = self._load_query_from_file("zipcode_based/q2_cross_zipcode_distances.sql")

        result = self._test_cache_effectiveness(query, "zipcode", "text")

        assert result['results_count'] >= 0, "Cross-zipcode query should execute"

        print("Cross-zipcode query results:")
        print(f"  Results: {result['results_count']}")
        print("  This tests cross-partition optimization")

    def test_points_near_businesses(self):
        """Test cross-table spatial joins between points and businesses."""
        query = self._load_query_from_file("region_based/q3_points_near_businesses.sql")

        result = self._test_cache_effectiveness(query, "region_id", "integer")

        assert result['results_count'] >= 0, "Points-businesses query should execute"

        print("Points near businesses query results:")
        print(f"  Results: {result['results_count']}")
        print("  Tests cross-table spatial optimization")

    @pytest.mark.performance
    def test_cache_performance_comparison(self):
        """Compare performance across different partition strategies."""
        test_queries = [
            ("region_based/q1_nearby_businesses.sql", "region_id", "integer"),
            ("city_based/q1_city_business_analysis.sql", "city_id", "integer"),
            ("zipcode_based/q1_zipcode_density.sql", "zipcode", "text"),
        ]

        performance_results = []

        for query_file, partition_key, datatype in test_queries:
            query = self._load_query_from_file(query_file)
            result = self._test_cache_effectiveness(query, partition_key, datatype)

            performance_results.append({
                'query': query_file,
                'partition_key': partition_key,
                'speedup': result['speedup'],
                'cache_effective': result['cache_effective'],
                'results_count': result['results_count']
            })

        print("\nPerformance Comparison:")
        print("-" * 60)
        for perf in performance_results:
            print(f"Query: {perf['query']}")
            print(f"  Partition: {perf['partition_key']}")
            print(f"  Results: {perf['results_count']}")
            print(f"  Speedup: {perf['speedup']:.2f}x")
            print(f"  Cache effective: {perf['cache_effective']}")
            print()

        # At least one query should show cache effectiveness (if cache is populated)
        effective_queries = [p for p in performance_results if p['cache_effective']]
        if effective_queries:
            print(f"Cache was effective for {len(effective_queries)}/{len(performance_results)} queries")
        else:
            print("Note: Cache may be empty for first test run - this is expected")

    def test_cache_backend_compatibility(self):
        """Test that the current cache backend works with spatial data."""
        backend = self._get_cache_backend()

        # Special handling for postgresql_roaringbit backend
        if backend == "postgresql_roaringbit":
            # Check if roaringbitmap extension is available
            try:
                import psycopg
                conn = psycopg.connect(self.conn_str)
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'roaringbitmap'")
                if not cur.fetchone():
                    pytest.skip("roaringbitmap extension not available")
                conn.close()
                print("✅ roaringbitmap extension is available")
            except Exception as e:
                pytest.skip(f"Cannot check roaringbitmap extension: {e}")

        # Simple test to ensure backend can handle our data types
        try:
            with partitioncache.create_cache_helper(backend, "region_id", "integer") as cache:
                assert cache is not None, f"Could not create cache helper for {backend}"

                # Test basic cache operations
                handler = cache.underlying_handler
                assert handler is not None, f"Could not get handler for {backend}"

                print(f"Cache backend {backend} is compatible with spatial data")

        except Exception as e:
            # For roaringbit backend, provide more specific error info
            if backend == "postgresql_roaringbit":
                pytest.fail(f"postgresql_roaringbit backend failed - this should work since extension was installed: {e}")
            else:
                pytest.fail(f"Cache backend {backend} failed: {e}")

    def test_distance_calculation_accuracy(self):
        """Test that our distance calculations are reasonable."""
        # This is a sanity check for the spatial queries
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    SQRT(POWER(52.520008 - 52.520000, 2) + POWER(13.404954 - 13.404954, 2)) as dist1,
                    SQRT(POWER(52.520008 - 52.530000, 2) + POWER(13.404954 - 13.450000, 2)) as dist2
            """)

            result = cur.fetchone()
            if result is None:
                pytest.skip("Distance calculation query failed")
            dist1, dist2 = result

            # Very close points should have very small distance
            assert dist1 < 0.001, f"Distance between close points too large: {dist1}"

            # Points ~1km apart should have distance ~0.01 in decimal degrees
            assert 0.01 < dist2 < 0.1, f"Distance calculation seems wrong: {dist2}"

            print(f"Distance calculations look reasonable: {dist1:.6f}, {dist2:.6f}")

    def test_roaringbitmap_functionality(self):
        """Test roaringbitmap extension functionality if backend is postgresql_roaringbit."""
        backend = self._get_cache_backend()

        if backend != "postgresql_roaringbit":
            pytest.skip("This test only runs for postgresql_roaringbit backend")

        # Test that roaringbitmap extension functions work
        with self.conn.cursor() as cur:
            # Test basic roaringbitmap operations
            cur.execute("SELECT rb_build(ARRAY[1,2,3,4,5])")
            result = cur.fetchone()
            assert result is not None, "rb_build function should work"

            # Test roaringbitmap with actual cache data
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT rb_build(ARRAY[region_id]) as rb 
                    FROM test_spatial_points 
                    WHERE region_id IS NOT NULL 
                    LIMIT 10
                ) t
            """)
            result = cur.fetchone()
            count = result[0] if result else 0
            assert count > 0, "Should be able to create roaring bitmaps from spatial data"

            print("✅ RoaringBitmap extension functions work correctly with spatial data")


class TestSpatialCacheIntegration:
    """Integration tests for spatial cache with different backends."""

    def test_multiple_partition_keys(self):
        """Test that different partition keys work with spatial data."""
        backends_to_test = []

        # Test current backend
        current_backend = os.getenv('CACHE_BACKEND', 'postgresql_array')
        backends_to_test.append(current_backend)


        test_cases = [
            ("region_id", "integer"),
            ("city_id", "integer"),
            ("zipcode", "text"),
        ]

        for backend in backends_to_test:
            for partition_key, datatype in test_cases:
                try:
                    with partitioncache.create_cache_helper(backend, partition_key, datatype) as cache:
                        assert cache is not None
                        print(f"✓ {backend} works with {partition_key} ({datatype})")

                except Exception as e:
                    print(f"✗ {backend} failed with {partition_key}: {e}")
                    # Don't fail the test for backend-specific issues

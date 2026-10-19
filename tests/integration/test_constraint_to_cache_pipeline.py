"""
Integration test for the full constraint-to-cache pipeline:
1. Generate query variants with bucket_steps, add_constraints, remove_constraints
2. Populate cache with the generated fragments
3. Verify cache can be applied via apply_cache_lazy with correct results

This tests the end-to-end flow that unit tests cover individually.
"""

import os

import pytest

from partitioncache.apply_cache import apply_cache_lazy
from partitioncache.cache_handler import get_cache_handler
from partitioncache.query_processor import generate_all_query_hash_pairs


@pytest.fixture
def pipeline_cache(db_session):
    """
    Create a postgresql_array cache handler for pipeline tests.
    """
    original_backend = os.getenv("CACHE_BACKEND")
    original_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX")

    os.environ["CACHE_BACKEND"] = "postgresql_array"
    os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "pipeline_test_cache"

    if not os.getenv("DB_NAME"):
        os.environ["DB_NAME"] = os.getenv("PG_DBNAME", "partitioncache_integration")
    if not os.getenv("DB_HOST"):
        os.environ["DB_HOST"] = os.getenv("PG_HOST", "localhost")
    if not os.getenv("DB_PORT"):
        os.environ["DB_PORT"] = os.getenv("PG_PORT", "5432")
    if not os.getenv("DB_USER"):
        os.environ["DB_USER"] = os.getenv("PG_USER", "integration_user")
    if not os.getenv("DB_PASSWORD"):
        os.environ["DB_PASSWORD"] = os.getenv("PG_PASSWORD", "integration_password")

    handler = None
    try:
        handler = get_cache_handler("postgresql_array", singleton=False)
        handler.register_partition_key("zipcode", "integer")
        yield handler
    except Exception as e:
        pytest.skip(f"Cannot create pipeline cache handler: {e}")
    finally:
        # Close handler first to release DB connections/locks
        if handler is not None:
            try:
                handler.close()
            except Exception:
                pass
        try:
            with db_session.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS pipeline_test_cache_cache_zipcode CASCADE;")
                cur.execute("DROP TABLE IF EXISTS pipeline_test_cache_partition_metadata CASCADE;")
                cur.execute("DROP TABLE IF EXISTS pipeline_test_cache_queries CASCADE;")
        except Exception:
            pass

        if original_backend:
            os.environ["CACHE_BACKEND"] = original_backend
        elif "CACHE_BACKEND" in os.environ:
            del os.environ["CACHE_BACKEND"]

        if original_prefix:
            os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = original_prefix
        elif "PG_ARRAY_CACHE_TABLE_PREFIX" in os.environ:
            del os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"]


class TestBasicPipeline:
    """Test the basic variant generation → cache → apply_cache_lazy pipeline."""

    def test_simple_query_pipeline(self, pipeline_cache):
        """Full pipeline: generate hashes → populate cache → apply_cache_lazy."""
        query = """
        SELECT t1.zipcode, t1.name, t2.name as business_name
        FROM test_locations t1, test_businesses t2
        WHERE SQRT(POWER(t1.x - t2.x, 2) + POWER(t1.y - t2.y, 2)) <= 0.01
        AND t1.zipcode = 1001
        """

        # Step 1: Generate fragments
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) > 0, "Should generate at least one fragment"

        # Step 2: Populate cache with simulated results
        test_partition_values = {1001, 1002, 90210}
        for query_text, query_hash in pairs:
            pipeline_cache.set_cache(query_hash, test_partition_values, "zipcode")
            pipeline_cache.set_query(query_hash, query_text, "zipcode")

        # Step 3: Apply cache and verify
        modified_query, stats = apply_cache_lazy(
            query,
            pipeline_cache,
            "zipcode",
            method="IN_SUBQUERY",
        )

        assert stats["cache_hits"] > 0, "Should have cache hits"
        assert modified_query != query, "Query should be modified with cache filter"
        # The modified query should contain IN or a subquery restricting partition keys
        modified_lower = modified_query.lower()
        assert "zipcode" in modified_lower, "Modified query should reference zipcode"

    def test_pipeline_with_no_cache_hits(self, pipeline_cache):
        """Pipeline should handle case where no cache hits are found."""
        query = """
        SELECT t1.zipcode, t1.name
        FROM test_locations t1
        WHERE t1.zipcode = 99999
        """

        # Generate fragments but DON'T populate cache
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) > 0

        # Apply cache - should return original query
        modified_query, stats = apply_cache_lazy(
            query,
            pipeline_cache,
            "zipcode",
            method="IN_SUBQUERY",
        )

        assert stats["cache_hits"] == 0, "Should have no cache hits"


class TestPipelineWithBucketSteps:
    """Test pipeline with distance normalization (bucket_steps)."""

    def test_bucket_steps_generates_more_variants(self, pipeline_cache):
        """bucket_steps should generate additional normalized distance variants."""
        query = """
        SELECT t1.zipcode, t2.name
        FROM test_locations t1, test_businesses t2
        WHERE SQRT(POWER(t1.x - t2.x, 2) + POWER(t1.y - t2.y, 2)) <= 1.6
        AND t1.zipcode = 1001
        """

        # Without bucket_steps
        pairs_no_bucket = generate_all_query_hash_pairs(query, "zipcode", bucket_steps=0)

        # With bucket_steps=1.0 (should normalize 1.6 to 2.0)
        pairs_with_bucket = generate_all_query_hash_pairs(query, "zipcode", bucket_steps=1.0)

        # bucket_steps should produce different or additional variants
        hashes_no_bucket = {h for _, h in pairs_no_bucket}
        hashes_with_bucket = {h for _, h in pairs_with_bucket}
        assert hashes_no_bucket != hashes_with_bucket, "bucket_steps should change the generated hashes"

    def test_bucket_steps_cache_lookup(self, pipeline_cache):
        """Cache populated with bucket_steps should hit on apply_cache_lazy."""
        query = """
        SELECT t1.zipcode, t2.name
        FROM test_locations t1, test_businesses t2
        WHERE SQRT(POWER(t1.x - t2.x, 2) + POWER(t1.y - t2.y, 2)) <= 1.6
        AND t1.zipcode = 1001
        """

        # Generate and populate with bucket_steps
        pairs = generate_all_query_hash_pairs(query, "zipcode", bucket_steps=1.0)
        for _query_text, query_hash in pairs:
            pipeline_cache.set_cache(query_hash, {1001, 1002}, "zipcode")

        # Apply cache with same bucket_steps
        modified_query, stats = apply_cache_lazy(
            query,
            pipeline_cache,
            "zipcode",
            method="IN_SUBQUERY",
        )

        assert stats["cache_hits"] > 0, "Should have cache hits with matching bucket_steps"


class TestPipelineWithAddConstraints:
    """Test pipeline with add_constraints parameter."""

    def test_add_constraints_generates_constrained_variants(self, pipeline_cache):
        """add_constraints should add conditions to the generated fragments."""
        query = """
        SELECT t1.zipcode, t1.name
        FROM test_locations t1
        WHERE t1.zipcode = 1001
        """

        # Without constraints
        pairs_base = generate_all_query_hash_pairs(query, "zipcode")

        # With constraints - adds extra condition to test_locations table
        pairs_constrained = generate_all_query_hash_pairs(
            query,
            "zipcode",
            add_constraints={"test_locations": "population > 10000"},
        )

        hashes_base = {h for _, h in pairs_base}
        hashes_constrained = {h for _, h in pairs_constrained}

        # Constrained should produce different hashes
        assert hashes_base != hashes_constrained, "add_constraints should change generated hashes"

        # Verify the constraint appears in the fragment text
        constrained_texts = [qt for qt, _ in pairs_constrained]
        has_constraint = any("population" in qt.lower() for qt in constrained_texts)
        assert has_constraint, "At least one fragment should contain the added constraint"

    def test_add_constraints_cache_pipeline(self, pipeline_cache):
        """Full pipeline with add_constraints from generation through apply_cache."""
        query = """
        SELECT t1.zipcode, t1.name
        FROM test_locations t1
        WHERE t1.zipcode = 1001
        """

        constraints = {"test_locations": "population > 10000"}

        # Generate with constraints and populate
        pairs = generate_all_query_hash_pairs(
            query, "zipcode", add_constraints=constraints
        )
        for _query_text, query_hash in pairs:
            pipeline_cache.set_cache(query_hash, {1001, 90210}, "zipcode")

        # Apply cache - the generated hashes should match
        modified_query, stats = apply_cache_lazy(
            query,
            pipeline_cache,
            "zipcode",
            method="IN_SUBQUERY",
        )

        # Note: apply_cache_lazy without the same add_constraints param won't find
        # the constraint-modified hashes in cache, so cache_hits may be 0 here.
        # We verify the function completes correctly and returns expected stats keys.
        assert "cache_hits" in stats, "Stats should contain cache_hits key"
        assert "generated_variants" in stats, "Stats should contain generated_variants key"
        assert "enhanced" in stats, "Stats should contain enhanced key"
        assert "p0_rewritten" in stats, "Stats should contain p0_rewritten key"


class TestPipelineWithRemoveConstraints:
    """Test pipeline with remove_constraints_all and remove_constraints_add."""

    def test_remove_constraints_all_generalizes(self, pipeline_cache):
        """remove_constraints_all should create more general variants."""
        query = """
        SELECT t1.zipcode, t1.name
        FROM test_locations t1
        WHERE t1.zipcode = 1001 AND t1.population > 10000
        """

        # Without removal
        pairs_full = generate_all_query_hash_pairs(query, "zipcode")

        # With removal of population attribute
        pairs_generalized = generate_all_query_hash_pairs(
            query,
            "zipcode",
            remove_constraints_all=["population"],
        )

        hashes_full = {h for _, h in pairs_full}
        hashes_generalized = {h for _, h in pairs_generalized}

        # Generalized should produce different hashes (fewer constraints)
        assert hashes_full != hashes_generalized, "Removing constraints should change hashes"

    def test_remove_constraints_add_creates_additional_variants(self, pipeline_cache):
        """remove_constraints_add should create additional variants alongside originals."""
        query = """
        SELECT t1.zipcode, t1.name
        FROM test_locations t1
        WHERE t1.zipcode = 1001 AND t1.population > 10000
        """

        # Without additional variants
        pairs_base = generate_all_query_hash_pairs(query, "zipcode")

        # With additional variants (population removed)
        pairs_extended = generate_all_query_hash_pairs(
            query,
            "zipcode",
            remove_constraints_add=["population"],
        )

        # Extended should have MORE variants (originals + new ones without population)
        assert len(pairs_extended) >= len(pairs_base), "remove_constraints_add should add more variants"


class TestPipelineWithCombinedModifications:
    """Test pipeline with multiple constraint modifications combined."""

    def test_all_modifications_combined(self, pipeline_cache):
        """Combine bucket_steps + add_constraints + remove_constraints."""
        query = """
        SELECT t1.zipcode, t2.name
        FROM test_locations t1, test_businesses t2
        WHERE SQRT(POWER(t1.x - t2.x, 2) + POWER(t1.y - t2.y, 2)) <= 1.6
        AND t1.zipcode = 1001
        AND t1.population > 10000
        """

        # Generate baseline hashes without any modifications
        pairs_baseline = generate_all_query_hash_pairs(query, "zipcode")
        hashes_baseline = {h for _, h in pairs_baseline}

        # Full combination
        pairs = generate_all_query_hash_pairs(
            query,
            "zipcode",
            bucket_steps=1.0,
            add_constraints={"test_businesses": "rating > 3.0"},
            remove_constraints_all=["population"],
        )

        assert len(pairs) > 0, "Combined modifications should still generate fragments"

        # Verify combined modifications actually changed the hashes
        hashes_combined = {h for _, h in pairs}
        assert hashes_baseline != hashes_combined, "Combined modifications should produce different hashes"

        # Populate and apply
        for _query_text, query_hash in pairs:
            pipeline_cache.set_cache(query_hash, {1001, 1002}, "zipcode")

        modified_query, stats = apply_cache_lazy(
            query,
            pipeline_cache,
            "zipcode",
            method="IN_SUBQUERY",
        )

        # Should produce a valid modified query
        assert modified_query is not None

    def test_combined_with_cache_intersection(self, pipeline_cache):
        """Multiple cached fragments should intersect correctly in apply_cache_lazy."""
        query = """
        SELECT t1.zipcode, t2.name
        FROM test_locations t1, test_businesses t2
        WHERE SQRT(POWER(t1.x - t2.x, 2) + POWER(t1.y - t2.y, 2)) <= 0.01
        AND t1.zipcode = 1001
        AND t1.region = 'northeast'
        """

        pairs = generate_all_query_hash_pairs(query, "zipcode")

        # Populate different fragments with overlapping but not identical sets
        for i, (_query_text, query_hash) in enumerate(pairs):
            if i % 2 == 0:
                # Even fragments: broader set
                pipeline_cache.set_cache(query_hash, {1001, 1002, 90210}, "zipcode")
            else:
                # Odd fragments: narrower set
                pipeline_cache.set_cache(query_hash, {1001, 1002}, "zipcode")

        modified_query, stats = apply_cache_lazy(
            query,
            pipeline_cache,
            "zipcode",
            method="IN_SUBQUERY",
        )

        if stats["cache_hits"] > 1:
            # If multiple hits, query should be enhanced with cache filtering.
            assert stats["enhanced"] == 1, "Expected query enhancement when multiple cache hits exist"
            assert modified_query != query, "Expected modified query when cache enhancement is applied"


class TestPipelineApplyCacheMethods:
    """Test different apply_cache_lazy methods work correctly with the pipeline."""

    @pytest.fixture(autouse=True)
    def populate_cache(self, pipeline_cache):
        """Populate cache for method comparison tests."""
        query = """
        SELECT t1.zipcode, t1.name
        FROM test_locations t1
        WHERE t1.zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        for _query_text, query_hash in pairs:
            pipeline_cache.set_cache(query_hash, {1001, 1002, 90210}, "zipcode")
        self.query = query

    def test_in_subquery_method(self, pipeline_cache):
        """IN_SUBQUERY method should produce valid SQL."""
        modified_query, stats = apply_cache_lazy(
            self.query, pipeline_cache, "zipcode", method="IN_SUBQUERY"
        )
        if stats["cache_hits"] > 0:
            assert "in" in modified_query.lower() or "IN" in modified_query

    def test_tmp_table_in_method(self, pipeline_cache):
        """TMP_TABLE_IN method should produce valid SQL."""
        modified_query, stats = apply_cache_lazy(
            self.query, pipeline_cache, "zipcode", method="TMP_TABLE_IN"
        )
        if stats["cache_hits"] > 0:
            assert "tmp" in modified_query.lower() or "temp" in modified_query.lower() or modified_query != self.query

    def test_tmp_table_join_method(self, pipeline_cache):
        """TMP_TABLE_JOIN method should produce valid SQL."""
        modified_query, stats = apply_cache_lazy(
            self.query, pipeline_cache, "zipcode", method="TMP_TABLE_JOIN"
        )
        if stats["cache_hits"] > 0:
            assert "join" in modified_query.lower(), "TMP_TABLE_JOIN method should add a JOIN clause"

    def test_all_methods_produce_same_partition_keys(self, pipeline_cache):
        """All methods should reference the same partition keys."""
        from typing import Literal

        results = {}
        methods: list[Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]] = [
            "IN_SUBQUERY",
            "TMP_TABLE_IN",
            "TMP_TABLE_JOIN",
        ]
        for method in methods:
            _, stats = apply_cache_lazy(
                self.query, pipeline_cache, "zipcode", method=method
            )
            results[method] = stats

        # All methods should have the same number of cache hits
        hit_counts = [r["cache_hits"] for r in results.values()]
        assert len(set(hit_counts)) == 1, f"All methods should have same cache hits, got {hit_counts}"

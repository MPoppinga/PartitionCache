"""postgresql_roaringbit refuses negative keys on every write path.

rb_build stores keys as unsigned 32-bit values: -1 was stored as 4294967295 and came back so from get and
get_intersected, while the lazy path returned -1.
"""

import os
import uuid

import pytest
from psycopg import sql


@pytest.fixture
def roaring(db_session):
    with db_session.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'roaringbitmap'")
        if cur.fetchone() is None:
            pytest.skip("roaringbitmap extension not available")
    from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

    prefix = f"roaring_range_{uuid.uuid4().hex[:8]}"
    handler = PostgreSQLRoaringBitCacheHandler(
        db_name=os.getenv("PG_DBNAME", "partitioncache_integration"),
        db_host=os.getenv("PG_HOST", "localhost"),
        db_user=os.getenv("PG_USER", "integration_user"),
        db_password=os.getenv("PG_PASSWORD", "integration_password"),
        db_port=os.getenv("PG_PORT", "5432"),
        db_tableprefix=prefix,
    )
    handler.register_partition_key("zipcode", "integer")
    yield handler
    handler.close()
    with db_session.cursor() as cur:
        for suffix in ("cache_zipcode", "partition_metadata", "queries"):
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(f"{prefix}_{suffix}")))


def test_set_cache_refuses_a_negative_key(roaring):
    with pytest.raises(ValueError, match="negative"):
        roaring.set_cache("neg", {-1, 4}, "zipcode")
    assert roaring.get("neg", "zipcode") is None


def test_set_cache_lazy_refuses_a_negative_key(roaring):
    assert roaring.set_cache_lazy("neg", "SELECT -1 AS zipcode UNION ALL SELECT 4", "zipcode") is False
    assert roaring.get("neg", "zipcode") is None
    assert roaring.set_cache_lazy("pos", "SELECT 0 AS zipcode UNION ALL SELECT 4", "zipcode") is True
    assert set(roaring.get("pos", "zipcode")) == {0, 4}

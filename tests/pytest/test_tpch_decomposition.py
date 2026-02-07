import pytest

from partitioncache.query_processor import generate_all_query_hash_pairs


Q3_COMPLEX = (
    """
    SELECT
        l.l_orderkey,
        o.o_orderdate,
        o.o_shippriority,
        SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue,
        COUNT(*) as line_count,
        AVG(l.l_quantity) as avg_quantity
    FROM
        customer c,
        orders o,
        lineitem l
    WHERE
        c.c_mktsegment = 'BUILDING'
        AND c.c_custkey = o.o_custkey
        AND l.l_orderkey = o.o_orderkey
        AND o.o_orderdate < DATE '1995-03-15'
        AND l.l_shipdate > DATE '1995-03-15'
    GROUP BY
        l.l_orderkey,
        o.o_orderdate,
        o.o_shippriority
    ORDER BY
        revenue DESC,
        o.o_orderdate
    LIMIT 100
    """
)


@pytest.mark.parametrize(
    "partition_key",
    [
        "nation_key",  # via customer or orders derivation
        "region_key",  # via customer/region
        "orderdate_year",  # via orders
    ],
)
def test_tpch_q3_generates_fragments(partition_key: str):
    pairs = generate_all_query_hash_pairs(
        query=Q3_COMPLEX,
        partition_key=partition_key,
        min_component_size=1,
        follow_graph=True,
        keep_all_attributes=True,
        warn_no_partition_key=False,
    )
    # Should create at least one fragment for TPC-H Q3 with robust table alias detection
    assert len(pairs) > 0



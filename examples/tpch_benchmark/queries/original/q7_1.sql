-- TPC-H Q7.1: Revenue by customer nation, BUILDING segment
SELECT cn.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, nation cn
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND c.c_nationkey = cn.n_nationkey
  AND c.c_mktsegment = 'BUILDING'
GROUP BY cn.n_name
ORDER BY revenue DESC

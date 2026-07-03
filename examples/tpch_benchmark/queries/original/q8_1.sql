-- TPC-H Q8.1: Revenue by year, ASIA customers, 1995, parts size 1-15
SELECT cn.n_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, part p,
     nation cn, region cr
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_partkey = p.p_partkey
  AND c.c_nationkey = cn.n_nationkey
  AND cn.n_regionkey = cr.r_regionkey
  AND cr.r_name = 'ASIA'
  AND o.o_orderdate >= '1995-01-01' AND o.o_orderdate < '1996-01-01'
  AND p.p_size >= 1 AND p.p_size <= 15
GROUP BY cn.n_name
ORDER BY revenue DESC

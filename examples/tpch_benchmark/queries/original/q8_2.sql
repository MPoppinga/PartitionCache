-- TPC-H Q8.2: Revenue by supplier nation, BUILDING customers, 1996, EUROPE suppliers
SELECT sn.n_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s,
     nation sn, region sr
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND s.s_nationkey = sn.n_nationkey
  AND sn.n_regionkey = sr.r_regionkey
  AND c.c_mktsegment = 'BUILDING'
  AND sr.r_name = 'EUROPE'
  AND o.o_orderdate >= '1996-01-01' AND o.o_orderdate < '1997-01-01'
GROUP BY sn.n_name
ORDER BY revenue DESC

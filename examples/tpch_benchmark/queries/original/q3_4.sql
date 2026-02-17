-- TPC-H Q3.4: Revenue for BUILDING customers, FRANCE suppliers, Dec 1996
SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s, nation sn
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND s.s_nationkey = sn.n_nationkey
  AND c.c_mktsegment = 'BUILDING'
  AND sn.n_name = 'FRANCE'
  AND o.o_orderdate >= '1996-12-01' AND o.o_orderdate < '1997-01-01'

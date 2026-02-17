-- TPC-H Q7.2: Revenue by supplier nation, EUROPE suppliers
SELECT sn.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, supplier s, nation sn, region sr
WHERE l.l_suppkey = s.s_suppkey
  AND s.s_nationkey = sn.n_nationkey
  AND sn.n_regionkey = sr.r_regionkey
  AND sr.r_name = 'EUROPE'
GROUP BY sn.n_name
ORDER BY revenue DESC

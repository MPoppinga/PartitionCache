-- TPC-H Q2.1: Revenue by part mfgr for BRASS parts from EUROPE suppliers
SELECT p.p_mfgr, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, part p, supplier s, nation n, region r
WHERE l.l_partkey = p.p_partkey
  AND l.l_suppkey = s.s_suppkey
  AND s.s_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND r.r_name = 'EUROPE'
  AND p.p_type LIKE '%BRASS'
GROUP BY p.p_mfgr
ORDER BY revenue DESC

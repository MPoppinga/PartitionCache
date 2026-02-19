-- TPC-H Q2.2: Revenue by part brand for Brand#23-#28 parts from ASIA suppliers
SELECT p.p_brand, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, part p, supplier s, nation n, region r
WHERE l.l_partkey = p.p_partkey
  AND l.l_suppkey = s.s_suppkey
  AND s.s_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND r.r_name = 'ASIA'
  AND p.p_brand >= 'Brand#23' AND p.p_brand <= 'Brand#28'
GROUP BY p.p_brand
ORDER BY revenue DESC

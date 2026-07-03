-- TPC-H Q7.3: Revenue by part brand, STEEL type parts
SELECT p.p_brand, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, part p
WHERE l.l_partkey = p.p_partkey
  AND p.p_type LIKE '%STEEL'
GROUP BY p.p_brand
ORDER BY revenue DESC

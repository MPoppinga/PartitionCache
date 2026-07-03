-- TPC-H Q8.3: Revenue by supplier nation, BRASS parts, MIDDLE EAST suppliers
SELECT sn.n_name, p.p_brand,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, supplier s, part p,
     nation sn, region sr
WHERE l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND s.s_nationkey = sn.n_nationkey
  AND sn.n_regionkey = sr.r_regionkey
  AND sr.r_name = 'MIDDLE EAST'
  AND p.p_type LIKE '%BRASS'
GROUP BY sn.n_name, p.p_brand
ORDER BY revenue DESC

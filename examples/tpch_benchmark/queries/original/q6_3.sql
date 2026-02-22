-- TPC-H Q6.3: Revenue by year, 1994-1996, AMERICA suppliers, Brand#13 BRASS types
SELECT EXTRACT(YEAR FROM o.o_orderdate) AS l_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, supplier s, part p,
     nation sn, region sr
WHERE l.l_orderkey = o.o_orderkey
  AND l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND s.s_nationkey = sn.n_nationkey
  AND sn.n_regionkey = sr.r_regionkey
  AND sr.r_name = 'AMERICA'
  AND o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1997-01-01'
  AND p.p_brand = 'Brand#13'
  AND p.p_type LIKE '%BRASS'
GROUP BY EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY l_year

-- SSB Q4.3: Profit by year, supplier city, part brand for UNITED STATES, MFGR#14, 1997-1998
SELECT d.d_year, s.s_city, p.p_brand, SUM(lo.lo_revenue - lo.lo_supplycost) AS profit
FROM lineorder lo, date_dim d, customer c, supplier s, part p
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'AMERICA'
  AND s.s_nation = 'UNITED STATES'
  AND (d.d_year = 1997 OR d.d_year = 1998)
  AND p.p_category = 'MFGR#14'
GROUP BY d.d_year, s.s_city, p.p_brand
ORDER BY d.d_year, s.s_city, p.p_brand

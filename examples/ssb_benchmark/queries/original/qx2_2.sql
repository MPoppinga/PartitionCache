-- SSB QX2.2: Cross-region profit excluding specific nations
-- EXPENSIVE: md5 per fact row + NOT IN exclusion on nation values
-- NOT IN ('CANADA', 'PERU') forces anti-join evaluation, non-indexed
SELECT c.c_nation, p.p_category, d.d_year,
       SUM(lo.lo_revenue - lo.lo_supplycost) AS profit,
       COUNT(*) AS order_count
FROM lineorder lo, customer c, supplier s, part p, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'AMERICA'
  AND s.s_region = 'AMERICA'
  AND c.c_nation NOT IN ('CANADA', 'PERU')
  AND (p.p_mfgr = 'MFGR#1' OR p.p_mfgr = 'MFGR#2')
  AND d.d_year BETWEEN 1995 AND 1998
  AND md5(lo.lo_orderkey::text) < 'a'
GROUP BY c.c_nation, p.p_category, d.d_year
ORDER BY d.d_year, profit DESC

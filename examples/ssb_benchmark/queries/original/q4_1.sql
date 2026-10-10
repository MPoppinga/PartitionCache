-- SSB Q4.1: Profit by customer nation, year for AMERICA region and MFGR#1/MFGR#2 parts
SELECT d.d_year, c.c_nation, SUM(lo.lo_revenue - lo.lo_supplycost) AS profit
FROM lineorder lo, date_dim d, customer c, supplier s, part p
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'AMERICA'
  AND s.s_region = 'AMERICA'
  AND (p.p_mfgr = 'MFGR#1' OR p.p_mfgr = 'MFGR#2')
GROUP BY d.d_year, c.c_nation
ORDER BY d.d_year, c.c_nation

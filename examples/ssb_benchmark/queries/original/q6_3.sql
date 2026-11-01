-- SSB Q6.3: Part hierarchy drill-down - Brand level (within MFGR#12)
-- Revenue by year for MFGR#1201 brand, AMERICA suppliers, 1992-1997
SELECT d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, part p, supplier s, date_dim d
WHERE lo.lo_partkey = p.p_partkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND p.p_brand = 'MFGR#1201'
  AND s.s_region = 'AMERICA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY d.d_year
ORDER BY d.d_year ASC

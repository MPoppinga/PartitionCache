-- SSB Q6.2: Part hierarchy drill-down - Category level (within MFGR#1)
-- Revenue by part brand, year for MFGR#12 parts, AMERICA suppliers, 1992-1997
SELECT p.p_brand, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, part p, supplier s, date_dim d
WHERE lo.lo_partkey = p.p_partkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND p.p_category = 'MFGR#12'
  AND s.s_region = 'AMERICA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY p.p_brand, d.d_year
ORDER BY d.d_year ASC, revenue DESC

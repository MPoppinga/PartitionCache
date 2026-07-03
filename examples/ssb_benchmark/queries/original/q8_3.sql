-- SSB Q8.3: Multi-PK combination - supplier + date
-- Revenue by supplier nation, year for EUROPE suppliers, 1996-1997
SELECT s.s_nation, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, supplier s, date_dim d
WHERE lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND s.s_region = 'EUROPE'
  AND d.d_year IN (1996, 1997)
GROUP BY s.s_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC

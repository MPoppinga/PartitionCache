-- SSB Q5.1: Customer hierarchy drill-down - Region level (broadest)
-- Revenue by customer nation, year for ASIA customers+suppliers, 1992-1997
SELECT c.c_nation, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'ASIA'
  AND s.s_region = 'ASIA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY c.c_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC

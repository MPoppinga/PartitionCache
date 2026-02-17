-- SSB Q5.3: Customer hierarchy drill-down - City level (within CHINA)
-- Revenue by year for CHINA0 customers, ASIA suppliers, 1992-1997
SELECT d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_city = 'CHINA0'
  AND s.s_region = 'ASIA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY d.d_year
ORDER BY d.d_year ASC

-- SSB Q3.2: Revenue by customer city, supplier city for UNITED STATES, 1992-1997
SELECT c.c_city, s.s_city, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_nation = 'UNITED STATES'
  AND s.s_nation = 'UNITED STATES'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year ASC, revenue DESC

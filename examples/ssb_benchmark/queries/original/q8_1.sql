-- SSB Q8.1: Multi-PK combination - customer + date
-- Revenue by customer nation, year for ASIA customers, 1995
SELECT c.c_nation, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'ASIA'
  AND d.d_year = 1995
GROUP BY c.c_nation, d.d_year
ORDER BY revenue DESC

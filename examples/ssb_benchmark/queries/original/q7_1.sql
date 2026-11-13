-- SSB Q7.1: Single partition key - customer only
-- Revenue by customer nation for AMERICA region
SELECT c.c_nation, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c
WHERE lo.lo_custkey = c.c_custkey
  AND c.c_region = 'AMERICA'
GROUP BY c.c_nation
ORDER BY revenue DESC

-- SSB Q7.2: Single partition key - supplier only
-- Revenue by supplier nation for EUROPE region
SELECT s.s_nation, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, supplier s
WHERE lo.lo_suppkey = s.s_suppkey
  AND s.s_region = 'EUROPE'
GROUP BY s.s_nation
ORDER BY revenue DESC

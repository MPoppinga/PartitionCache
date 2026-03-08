-- SSB Q7.3: Single partition key - part only
-- Revenue by part manufacturer for MFGR#3
SELECT p.p_mfgr, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, part p
WHERE lo.lo_partkey = p.p_partkey
  AND p.p_mfgr = 'MFGR#3'
GROUP BY p.p_mfgr
ORDER BY revenue DESC

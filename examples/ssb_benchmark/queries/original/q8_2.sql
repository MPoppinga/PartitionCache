-- SSB Q8.2: Multi-PK combination - customer + part
-- Revenue by customer city, part category for UNITED STATES customers, MFGR#2 parts
SELECT c.c_city, p.p_category, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, part p
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_partkey = p.p_partkey
  AND c.c_nation = 'UNITED STATES'
  AND p.p_mfgr = 'MFGR#2'
GROUP BY c.c_city, p.p_category
ORDER BY revenue DESC

-- SSB QX1.1: Premium revenue - ASIA region 1994-1996
-- EXPENSIVE: md5 per fact row prevents index pushdown, forces full lineorder scan
-- Selects ~50% of orders via md5 hash prefix filter (simulates hash-partitioned access pattern)
-- Cache reuse: shares 'c.c_region=ASIA', 's.s_region=ASIA' fragments with q3_1/q5_1
-- Partition pre-filter (lo_custkey) cuts md5 work from 5.56M to ~1.1M rows (5x)
SELECT c.c_city, s.s_city, d.d_year,
       COUNT(*) AS order_count,
       SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'ASIA'
  AND s.s_region = 'ASIA'
  AND d.d_year BETWEEN 1994 AND 1996
  AND md5(lo.lo_orderkey::text) < '8'
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year, revenue DESC

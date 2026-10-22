-- SSB QX2.1: Supplier name search - AMERICA region profit analysis
-- EXPENSIVE: md5 per fact row + ILIKE on s_name forces seq scan of supplier
-- ILIKE 'Supplier#0000001%' selects suppliers 10-19, 100-199, 1000-1999 (~37% of 2K suppliers)
-- No index on s_name → full scan per evaluation
SELECT s.s_city, p.p_mfgr, d.d_year,
       COUNT(*) AS order_count,
       SUM(lo.lo_revenue - lo.lo_supplycost) AS profit
FROM lineorder lo, supplier s, part p, date_dim d
WHERE lo.lo_suppkey = s.s_suppkey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_orderdate = d.d_datekey
  AND s.s_region = 'AMERICA'
  AND s.s_name ILIKE 'Supplier#0000001%'
  AND d.d_year IN (1995, 1996, 1997)
  AND md5(lo.lo_orderkey::text) < 'b'
GROUP BY s.s_city, p.p_mfgr, d.d_year
ORDER BY d.d_year, profit DESC

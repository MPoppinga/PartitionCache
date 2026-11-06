-- SSB QX1.2: High-revenue transactions - AMERICA region with customer name filter
-- EXPENSIVE: md5 per fact row + ILIKE on c_name forces seq scan of customer
-- ILIKE 'Customer#0000001%' selects customers 100-199, 1000-1999, 10000-19999 (~37%)
-- No index on c_name → full scan of 30K customers per evaluation
SELECT c.c_nation, s.s_nation, d.d_year,
       SUM(lo.lo_revenue) AS revenue,
       COUNT(DISTINCT lo.lo_custkey) AS unique_customers
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'AMERICA'
  AND s.s_region = 'AMERICA'
  AND c.c_name ILIKE 'Customer#0000001%'
  AND d.d_year BETWEEN 1993 AND 1997
  AND md5(lo.lo_orderkey::text) < 'c'
GROUP BY c.c_nation, s.s_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC

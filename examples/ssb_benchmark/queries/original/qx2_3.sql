-- SSB QX2.3: Customer phone prefix analysis - ASIA suppliers
-- EXPENSIVE: md5 per fact row + LIKE on c_phone forces seq scan of customer
-- Phone prefix '1%' selects customers with area codes 10-19: ~40% of customers
-- Simulates phone-number-based customer segmentation query
SELECT c.c_nation, s.s_nation, d.d_year,
       SUM(lo.lo_revenue) AS revenue,
       SUM(lo.lo_extendedprice * lo.lo_discount) AS discount_revenue,
       COUNT(*) AS order_count
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_phone LIKE '1%'
  AND s.s_region = 'ASIA'
  AND d.d_year BETWEEN 1992 AND 1997
  AND md5(lo.lo_orderkey::text) < '9'
GROUP BY c.c_nation, s.s_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC

-- SSB QX1.3: Part color pattern analysis - EUROPE customers
-- EXPENSIVE: md5 per fact row + ILIKE on p_color forces seq scan of part
-- ILIKE 'b%' matches blue/burnished/beige/blush/burlywood (~25% of 200K parts)
-- No index on p_color → full scan of part table per evaluation
SELECT c.c_nation, p.p_category, d.d_year,
       COUNT(*) AS order_count,
       SUM(lo.lo_revenue) AS revenue,
       AVG(lo.lo_discount) AS avg_discount
FROM lineorder lo, customer c, part p, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'EUROPE'
  AND p.p_color ILIKE 'b%'
  AND d.d_year BETWEEN 1992 AND 1997
  AND md5(lo.lo_orderkey::text) < 'a'
GROUP BY c.c_nation, p.p_category, d.d_year
ORDER BY d.d_year, revenue DESC

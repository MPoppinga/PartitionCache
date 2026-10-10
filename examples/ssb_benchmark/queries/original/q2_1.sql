-- SSB Q2.1: Revenue by brand for AMERICA suppliers, MFGR#1 or MFGR#2 parts
SELECT SUM(lo.lo_revenue), d.d_year, p.p_brand
FROM lineorder lo, date_dim d, part p, supplier s
WHERE lo.lo_orderdate = d.d_datekey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_suppkey = s.s_suppkey
  AND p.p_category = 'MFGR#12'
  AND s.s_region = 'AMERICA'
GROUP BY d.d_year, p.p_brand
ORDER BY d.d_year, p.p_brand

-- SSB Q2.3: Revenue by brand for EUROPE suppliers, specific brand
SELECT SUM(lo.lo_revenue), d.d_year, p.p_brand
FROM lineorder lo, date_dim d, part p, supplier s
WHERE lo.lo_orderdate = d.d_datekey
  AND lo.lo_partkey = p.p_partkey
  AND lo.lo_suppkey = s.s_suppkey
  AND p.p_brand = 'MFGR#2239'
  AND s.s_region = 'EUROPE'
GROUP BY d.d_year, p.p_brand
ORDER BY d.d_year, p.p_brand

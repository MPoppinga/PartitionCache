-- SSB Q1.2: Revenue from lineorder where discount between 4-6, quantity between 26-35, yearmonthnum 199401
SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder lo, date_dim d
WHERE lo.lo_orderdate = d.d_datekey
  AND d.d_yearmonthnum = 199401
  AND lo.lo_discount >= 4 AND lo.lo_discount <= 6
  AND lo.lo_quantity >= 26 AND lo.lo_quantity <= 35

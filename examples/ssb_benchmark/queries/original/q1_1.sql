-- SSB Q1.1: Revenue from lineorder where discount between 1-3, quantity < 25, year 1993
SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder lo, date_dim d
WHERE lo.lo_orderdate = d.d_datekey
  AND d.d_year = 1993
  AND lo.lo_discount >= 1 AND lo.lo_discount <= 3
  AND lo.lo_quantity < 25

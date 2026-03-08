-- SSB Q1.3: Revenue from lineorder where discount between 5-7, quantity between 26-35, specific week in 1994
SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder lo, date_dim d
WHERE lo.lo_orderdate = d.d_datekey
  AND d.d_weeknuminyear = 6 AND d.d_year = 1994
  AND lo.lo_discount >= 5 AND lo.lo_discount <= 7
  AND lo.lo_quantity >= 26 AND lo.lo_quantity <= 35

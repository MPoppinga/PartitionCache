-- TPC-H Q1.1: Revenue from lineitem, year 1994, discount 5-7%, quantity < 24
SELECT SUM(l.l_extendedprice * l.l_discount) AS revenue
FROM lineitem l, orders o
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1995-01-01'
  AND l.l_discount >= 0.05 AND l.l_discount <= 0.07
  AND l.l_quantity < 24

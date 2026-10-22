-- TPC-H Q1.3: Revenue from lineitem, first week Feb 1994, discount 5-7%, quantity 26-35
SELECT SUM(l.l_extendedprice * l.l_discount) AS revenue
FROM lineitem l, orders o
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_orderdate >= '1994-02-01' AND o.o_orderdate < '1994-02-08'
  AND l.l_discount >= 0.05 AND l.l_discount <= 0.07
  AND l.l_quantity >= 26 AND l.l_quantity <= 35

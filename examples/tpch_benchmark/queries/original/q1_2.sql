-- TPC-H Q1.2: Revenue from lineitem, January 1994, discount 4-6%, quantity 26-35
SELECT SUM(l.l_extendedprice * l.l_discount) AS revenue
FROM lineitem l, orders o
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1994-02-01'
  AND l.l_discount >= 0.04 AND l.l_discount <= 0.06
  AND l.l_quantity >= 26 AND l.l_quantity <= 35

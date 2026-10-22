-- TPC-H Q1.2 adapted: January 1994 + lineitem attrs
-- Partition key: l_orderkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1994-01-01' AND o_orderdate < '1994-02-01')
  AND l.l_discount >= 0.04 AND l.l_discount <= 0.06
  AND l.l_quantity >= 26 AND l.l_quantity <= 35

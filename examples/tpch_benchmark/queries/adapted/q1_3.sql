-- TPC-H Q1.3 adapted: first week Feb 1994 + lineitem attrs
-- Partition key: l_orderkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1994-02-01' AND o_orderdate < '1994-02-08')
  AND l.l_discount >= 0.05 AND l.l_discount <= 0.07
  AND l.l_quantity >= 26 AND l.l_quantity <= 35

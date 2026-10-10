-- TPC-H Q1.1 adapted: date via orders + lineitem attrs
-- Partition key: l_orderkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01')
  AND l.l_discount >= 0.05 AND l.l_discount <= 0.07
  AND l.l_quantity < 24

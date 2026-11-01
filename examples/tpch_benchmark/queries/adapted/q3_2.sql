-- TPC-H Q3.2 adapted: FRANCE customers + GERMANY suppliers + 1994-1996
-- Partition keys: l_orderkey, l_suppkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE c_nationkey IN (SELECT n_nationkey FROM nation WHERE n_name = 'FRANCE')))
  AND l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1994-01-01' AND o_orderdate < '1997-01-01')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_name = 'GERMANY'))

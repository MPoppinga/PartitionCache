-- TPC-H Q3.3 adapted: FRANCE/GERMANY customers + FRANCE/GERMANY suppliers + 1995
-- Partition keys: l_orderkey, l_suppkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE c_nationkey IN (SELECT n_nationkey FROM nation WHERE n_name IN ('FRANCE', 'GERMANY'))))
  AND l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1995-01-01' AND o_orderdate < '1996-01-01')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_name IN ('FRANCE', 'GERMANY')))

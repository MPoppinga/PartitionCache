-- TPC-H Q8.1 adapted: l_orderkey + l_partkey - ASIA customers, 1995, parts size 1-15
-- Partition keys: l_orderkey, l_partkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE c_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'ASIA'))))
  AND l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1995-01-01' AND o_orderdate < '1996-01-01')
  AND l.l_partkey IN (SELECT p_partkey FROM part WHERE p_size >= 1 AND p_size <= 15)

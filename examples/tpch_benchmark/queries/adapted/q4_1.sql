-- TPC-H Q4.1 adapted: AMERICA customers + AMERICA suppliers + Manufacturer#1/#2 parts
-- Partition keys: l_orderkey, l_partkey, l_suppkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE c_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'AMERICA'))))
  AND l.l_partkey IN (SELECT p_partkey FROM part WHERE p_mfgr = 'Manufacturer#1' OR p_mfgr = 'Manufacturer#2')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'AMERICA')))

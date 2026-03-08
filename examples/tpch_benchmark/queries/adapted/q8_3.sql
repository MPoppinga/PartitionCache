-- TPC-H Q8.3 adapted: l_partkey + l_suppkey - BRASS parts, MIDDLE EAST suppliers
-- Partition keys: l_partkey, l_suppkey
SELECT l.l_partkey
FROM lineitem l
WHERE l.l_partkey IN (SELECT p_partkey FROM part WHERE p_type LIKE '%BRASS')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'MIDDLE EAST')))

-- TPC-H Q2.2 adapted: Brand#23-#28 parts + ASIA suppliers
-- Partition keys: l_partkey, l_suppkey
SELECT l.l_partkey
FROM lineitem l
WHERE l.l_partkey IN (SELECT p_partkey FROM part WHERE p_brand >= 'Brand#23' AND p_brand <= 'Brand#28')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'ASIA')))

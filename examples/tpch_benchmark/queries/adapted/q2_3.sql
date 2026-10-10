-- TPC-H Q2.3 adapted: Brand#34 parts + GERMANY suppliers
-- Partition keys: l_partkey, l_suppkey
SELECT l.l_partkey
FROM lineitem l
WHERE l.l_partkey IN (SELECT p_partkey FROM part WHERE p_brand = 'Brand#34')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_name = 'GERMANY'))

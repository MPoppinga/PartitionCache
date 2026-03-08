-- TPC-H Q6.2 adapted: part hierarchy - Brand#13
-- Partition keys: l_orderkey, l_partkey, l_suppkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1994-01-01' AND o_orderdate < '1997-01-01')
  AND l.l_partkey IN (SELECT p_partkey FROM part WHERE p_brand = 'Brand#13')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'AMERICA')))

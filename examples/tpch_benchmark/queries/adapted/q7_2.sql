-- TPC-H Q7.2 adapted: l_suppkey only - EUROPE region suppliers
-- Partition key: l_suppkey
SELECT l.l_suppkey
FROM lineitem l
WHERE l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'EUROPE')))

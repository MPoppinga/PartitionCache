-- TPC-H Q7.3 adapted: l_partkey only - STEEL type parts
-- Partition key: l_partkey
SELECT l.l_partkey
FROM lineitem l
WHERE l.l_partkey IN (SELECT p_partkey FROM part WHERE p_type LIKE '%STEEL')

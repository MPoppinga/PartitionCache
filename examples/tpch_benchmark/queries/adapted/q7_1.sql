-- TPC-H Q7.1 adapted: l_orderkey only - BUILDING segment customers
-- Partition key: l_orderkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE c_mktsegment = 'BUILDING'))

-- TPC-H Q8.2 adapted: l_orderkey + l_suppkey - BUILDING customers, 1996, EUROPE suppliers
-- Partition keys: l_orderkey, l_suppkey
SELECT l.l_orderkey
FROM lineitem l
WHERE l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer WHERE c_mktsegment = 'BUILDING'))
  AND l.l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderdate >= '1996-01-01' AND o_orderdate < '1997-01-01')
  AND l.l_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'EUROPE')))

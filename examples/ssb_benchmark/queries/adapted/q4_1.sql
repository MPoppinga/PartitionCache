-- SSB Q4.1 adapted for PartitionCache: single fact table with IN subqueries
-- Partition keys: lo_custkey, lo_suppkey, lo_partkey, lo_orderdate
SELECT lo.lo_custkey
FROM lineorder lo
WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_region = 'AMERICA')
  AND lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'AMERICA')
  AND lo.lo_partkey IN (SELECT p_partkey FROM part WHERE p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')

-- SSB Q2.1 adapted for PartitionCache: single fact table with IN subqueries
-- Partition keys: lo_suppkey, lo_partkey
SELECT lo.lo_suppkey
FROM lineorder lo
WHERE lo.lo_partkey IN (SELECT p_partkey FROM part WHERE p_category = 'MFGR#12')
  AND lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'AMERICA')

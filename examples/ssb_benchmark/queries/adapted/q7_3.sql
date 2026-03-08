-- SSB Q7.3 adapted for PartitionCache: single partition key - part only
-- Partition key: lo_partkey
SELECT lo.lo_partkey
FROM lineorder lo
WHERE lo.lo_partkey IN (SELECT p_partkey FROM part WHERE p_mfgr = 'MFGR#3')

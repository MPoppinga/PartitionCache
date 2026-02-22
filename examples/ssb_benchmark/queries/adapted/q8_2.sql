-- SSB Q8.2 adapted for PartitionCache: multi-PK combination - customer + part
-- Partition keys: lo_custkey, lo_partkey
SELECT lo.lo_custkey
FROM lineorder lo
WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_nation = 'UNITED STATES')
  AND lo.lo_partkey IN (SELECT p_partkey FROM part WHERE p_mfgr = 'MFGR#2')

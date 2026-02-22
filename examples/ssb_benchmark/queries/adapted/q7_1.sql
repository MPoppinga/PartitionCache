-- SSB Q7.1 adapted for PartitionCache: single partition key - customer only
-- Partition key: lo_custkey
SELECT lo.lo_custkey
FROM lineorder lo
WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_region = 'AMERICA')

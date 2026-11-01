-- SSB Q7.2 adapted for PartitionCache: single partition key - supplier only
-- Partition key: lo_suppkey
SELECT lo.lo_suppkey
FROM lineorder lo
WHERE lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'EUROPE')

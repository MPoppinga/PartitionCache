-- SSB Q8.1 adapted for PartitionCache: multi-PK combination - customer + date
-- Partition keys: lo_custkey, lo_orderdate
SELECT lo.lo_custkey
FROM lineorder lo
WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_region = 'ASIA')
  AND lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_year = 1995)

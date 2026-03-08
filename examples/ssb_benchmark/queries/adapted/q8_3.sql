-- SSB Q8.3 adapted for PartitionCache: multi-PK combination - supplier + date
-- Partition keys: lo_suppkey, lo_orderdate
SELECT lo.lo_suppkey
FROM lineorder lo
WHERE lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'EUROPE')
  AND lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_year IN (1996, 1997))

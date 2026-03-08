-- SSB Q6.2 adapted for PartitionCache: part hierarchy drill-down - Category level
-- Partition keys: lo_partkey, lo_suppkey, lo_orderdate
SELECT lo.lo_partkey
FROM lineorder lo
WHERE lo.lo_partkey IN (SELECT p_partkey FROM part WHERE p_category = 'MFGR#12')
  AND lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_region = 'AMERICA')
  AND lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_year >= 1992 AND d_year <= 1997)

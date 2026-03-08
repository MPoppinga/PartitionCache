-- SSB Q3.4 adapted for PartitionCache: single fact table with IN subqueries
-- Partition keys: lo_custkey, lo_suppkey, lo_orderdate
SELECT lo.lo_custkey
FROM lineorder lo
WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
  AND lo.lo_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
  AND lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_yearmonth = 'Dec1997')

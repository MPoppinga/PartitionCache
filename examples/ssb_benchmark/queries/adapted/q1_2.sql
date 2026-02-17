-- SSB Q1.2 adapted for PartitionCache: single fact table with IN subqueries
-- Partition key: lo_orderdate
SELECT lo.lo_orderdate
FROM lineorder lo
WHERE lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_yearmonthnum = 199401)
  AND lo.lo_discount >= 4 AND lo.lo_discount <= 6
  AND lo.lo_quantity >= 26 AND lo.lo_quantity <= 35

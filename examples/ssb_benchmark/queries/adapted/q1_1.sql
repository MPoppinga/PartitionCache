-- SSB Q1.1 adapted for PartitionCache: single fact table with IN subqueries
-- Partition key: lo_orderdate
SELECT lo.lo_orderdate
FROM lineorder lo
WHERE lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_year = 1993)
  AND lo.lo_discount >= 1 AND lo.lo_discount <= 3
  AND lo.lo_quantity < 25

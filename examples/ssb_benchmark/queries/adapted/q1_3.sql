-- SSB Q1.3 adapted for PartitionCache: single fact table with IN subqueries
-- Partition key: lo_orderdate
SELECT lo.lo_orderdate
FROM lineorder lo
WHERE lo.lo_orderdate IN (SELECT d_datekey FROM date_dim WHERE d_weeknuminyear = 6 AND d_year = 1994)
  AND lo.lo_discount >= 5 AND lo.lo_discount <= 7
  AND lo.lo_quantity >= 26 AND lo.lo_quantity <= 35

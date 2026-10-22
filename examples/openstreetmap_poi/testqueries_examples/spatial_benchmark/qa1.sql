-- Category A: Double leading-wildcard LIKE (no btree index usable on name)
-- Both name conditions force sequential scans. Only GiST helps for spatial.
-- Expected: PartitionCache pre-filters p1 to cells where Pizza near Apotheke exists.
SELECT p1.name AS pizza_name,
       p2.name AS pharmacy_name
FROM pois AS p1
   , pois AS p2
WHERE p1.name LIKE '%Pizza%'
  AND p2.name LIKE '%Apotheke%'
  AND ST_DWithin(p1.geom, p2.geom, 300);

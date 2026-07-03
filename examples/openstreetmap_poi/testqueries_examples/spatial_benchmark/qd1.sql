-- Category D: Expression-based filter (no index on computed values)
-- LENGTH() and LOWER()+LIKE defeat all indexes. Only GiST spatial index helps.
SELECT p1.name AS long_restaurant_name,
       p2.name AS markt_poi
FROM pois AS p1
   , pois AS p2
WHERE p1.subtype = 'restaurant'
  AND LENGTH(p1.name) > 30
  AND LOWER(p2.name) LIKE '%markt%'
  AND ST_DWithin(p1.geom, p2.geom, 400);

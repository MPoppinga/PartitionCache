SELECT p1.name AS cafe_name,
       p2.name AS bakery_name
FROM pois AS p1
   , pois AS p2
WHERE p1.subtype = 'cafe'
  AND p2.subtype = 'bakery'
  AND p1.name LIKE '%Starbucks%'
  AND p2.name LIKE '%Back%'
  AND ST_DWithin(p1.geom, p2.geom, 200);

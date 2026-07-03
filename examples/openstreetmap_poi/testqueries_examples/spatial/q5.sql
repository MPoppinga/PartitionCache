SELECT p1.name AS restaurant_name,
       p2.name AS hotel_name
FROM pois AS p1
   , pois AS p2
WHERE p1.subtype = 'restaurant'
  AND p2.subtype = 'hotel'
  AND p1.name LIKE '%Sushi%'
  AND p2.name LIKE '%Best Western%'
  AND ST_DWithin(p1.geom, p2.geom, 1000);

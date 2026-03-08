SELECT p1.name AS ice_cream_name,
       p2.name AS bakery_name,
       p3.name AS cafe_name,
       p4.name AS fast_food_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
   , pois AS p4
WHERE p1.subtype = 'ice_cream'
  AND p2.subtype = 'bakery'
  AND p2.name LIKE '%Back%'
  AND p3.subtype = 'cafe'
  AND p3.name LIKE '%Starbucks%'
  AND p4.subtype = 'fast_food'
  AND p4.name LIKE '%McDonald%'
  AND ST_DWithin(p1.geom, p2.geom, 150)
  AND ST_DWithin(p1.geom, p3.geom, 200)
  AND ST_DWithin(p1.geom, p4.geom, 250);

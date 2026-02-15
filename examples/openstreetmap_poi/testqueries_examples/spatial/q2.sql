SELECT p1.name AS ice_cream_name,
       p2.name AS swimming_pool,
       p3.name AS supermarket_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'swimming_pool'
  AND p3.subtype = 'supermarket'
  AND p3.name ILIKE '%Edeka%'
  AND ST_DWithin(p1.geom, p3.geom, 400)
  AND ST_DWithin(p1.geom, p2.geom, 400);

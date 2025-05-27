
SELECT p1.name AS ice_cream_name,
       p2.name AS swimming_pool,
       p3.name AS supermark_name,
       p1.landkreis
FROM pois AS p1
   , pois as p2
   , pois as p3
WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'swimming_pool'
  and p3.subtype = 'supermarket'
  and p3.name ILIKE '%Edeka%'
  AND p2.landkreis = p1.landkreis
  and p3.landkreis = p1.landkreis
  and p3.landkreis = p2.landkreis
  and ST_DWithin(p1.geom, p3.geom, 400)
  AND ST_DWithin(p1.geom, p2.geom, 400)
;

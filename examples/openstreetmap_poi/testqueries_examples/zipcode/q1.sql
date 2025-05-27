SELECT p1.name AS ice_cream_name,
       p2.name AS pharmacy_name,
       p3.name AS supermark_name,
       p1.landkreis
FROM pois AS p1
   , pois as p2
   , pois as p3
WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'pharmacy'
  and p3.subtype = 'supermarket'
  and p3.name LIKE '%ALDI%'
  AND p2.landkreis = p1.landkreis
  and p3.landkreis = p1.landkreis
  and p3.landkreis = p2.landkreis
  and ST_DWithin(p1.geom, p3.geom, 300)
  AND ST_DWithin(p1.geom, p2.geom, 400);

  
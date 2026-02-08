SELECT p1.name AS ice_cream_name,
       p2.name AS pharmacy_name,
       p3.name AS supermarket_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'pharmacy'
  AND p3.subtype = 'supermarket'
  AND p3.name LIKE '%ALDI%'
  AND ST_DWithin(p1.geom, p3.geom, 300)
  AND ST_DWithin(p1.geom, p2.geom, 400);

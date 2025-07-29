SELECT p1.name AS ice_cream_name,
       p2.name AS pharmacy_name,
       p3.name AS supermarket_name,
       p1.zipcode
FROM pois AS p1
   , pois as p2
   , pois as p3
WHERE p1.name LIKE '%Eis %' AND p2.subtype = 'pharmacy'
  and p3.subtype = 'supermarket'
  and p3.name LIKE '%ALDI%'
  AND p2.zipcode = p1.zipcode
  and p3.zipcode = p1.zipcode
  and p3.zipcode = p2.zipcode
  and ST_DWithin(p1.geom, p3.geom, 300)
  AND ST_DWithin(p1.geom, p2.geom, 400);

  
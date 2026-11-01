SELECT p1.name AS cafe_name,
       p2.name AS bakery_name,
       p3.name AS pharmacy_name,
       p4.name AS supermarket_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
   , pois AS p4
WHERE p1.subtype = 'cafe'
  AND p1.name LIKE '%Starbucks%'
  AND p2.subtype = 'bakery'
  AND p2.name LIKE '%Back%'
  AND p3.subtype = 'pharmacy'
  AND p4.subtype = 'supermarket'
  AND p4.name LIKE '%ALDI%'
  AND ST_DWithin(p1.geom, p2.geom, 200)
  AND ST_DWithin(p1.geom, p3.geom, 300)
  AND ST_DWithin(p1.geom, p4.geom, 400);

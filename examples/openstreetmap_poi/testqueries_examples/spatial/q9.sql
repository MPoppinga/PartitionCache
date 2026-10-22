SELECT p1.name AS restaurant_name,
       p2.name AS hotel_name,
       p3.name AS cinema_name,
       p4.name AS bar_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
   , pois AS p4
WHERE p1.subtype = 'restaurant'
  AND p1.name LIKE '%Pizz%'
  AND p2.subtype = 'hotel'
  AND p2.name LIKE '%Ibis%'
  AND p3.subtype = 'cinema'
  AND p4.subtype = 'bar'
  AND p4.name LIKE '%Irish%'
  AND ST_DWithin(p1.geom, p2.geom, 500)
  AND ST_DWithin(p1.geom, p3.geom, 1000)
  AND ST_DWithin(p1.geom, p4.geom, 800);

-- Category B: Triple high-cardinality join with large radius
-- restaurant(95k) × pharmacy(16k) × supermarket(32k) with 500m radius
-- The 3-way spatial join creates a very large search space.
SELECT p1.name AS restaurant_name,
       p2.name AS pharmacy_name,
       p3.name AS supermarket_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
WHERE p1.subtype = 'restaurant'
  AND p2.subtype = 'pharmacy'
  AND p3.subtype = 'supermarket'
  AND p1.name LIKE '%Pizza%'
  AND ST_DWithin(p1.geom, p2.geom, 500)
  AND ST_DWithin(p1.geom, p3.geom, 500);

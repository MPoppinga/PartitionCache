-- Category B: High-cardinality cross-product (restaurant 95k × cafe 29k)
-- Both subtypes are very common. Cross-product before spatial filter is enormous.
-- The subtype index helps narrow each side but the spatial join dominates.
SELECT p1.name AS restaurant_name,
       p2.name AS cafe_name
FROM pois AS p1
   , pois AS p2
WHERE p1.subtype = 'restaurant'
  AND p2.subtype = 'cafe'
  AND p1.name LIKE '%Pizza%'
  AND ST_DWithin(p1.geom, p2.geom, 200);

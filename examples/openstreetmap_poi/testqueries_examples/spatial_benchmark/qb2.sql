-- Category B: Same-subtype self-join with wildcards
-- Self-join on restaurant (95k × 95k) with leading wildcards = worst case for planner.
SELECT p1.name AS pizza_place,
       p2.name AS doener_place
FROM pois AS p1
   , pois AS p2
WHERE p1.subtype = 'restaurant'
  AND p2.subtype = 'restaurant'
  AND p1.name LIKE '%Pizza%'
  AND p2.name LIKE '%Döner%'
  AND ST_DWithin(p1.geom, p2.geom, 500);

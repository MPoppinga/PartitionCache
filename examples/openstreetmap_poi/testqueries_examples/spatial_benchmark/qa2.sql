-- Category A: Case-insensitive multi-pattern search (no index usable)
-- LOWER() + LIKE prevents any btree usage. Multiple OR conditions compound the problem.
SELECT p1.name AS food_name,
       p2.name AS pharmacy_name
FROM pois AS p1
   , pois AS p2
WHERE (LOWER(p1.name) LIKE '%pizza%' OR LOWER(p1.name) LIKE '%sushi%'
       OR LOWER(p1.name) LIKE '%döner%' OR LOWER(p1.name) LIKE '%burger%')
  AND LOWER(p2.name) LIKE '%apotheke%'
  AND ST_DWithin(p1.geom, p2.geom, 500);

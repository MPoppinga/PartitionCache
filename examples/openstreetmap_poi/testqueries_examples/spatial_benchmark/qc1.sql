-- Category C: 4-table spatial chain with mixed filters
-- Chain: pizza→pharmacy→supermarket→bakery, each within 300m of next.
-- Plan complexity grows with chain length. No single index covers the chain.
SELECT p1.name AS pizza_name,
       p2.name AS pharmacy_name,
       p3.name AS supermarket_name,
       p4.name AS bakery_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
   , pois AS p4
WHERE p1.name LIKE '%Pizza%'
  AND p2.subtype = 'pharmacy'
  AND p3.subtype = 'supermarket'
  AND p4.subtype = 'bakery'
  AND ST_DWithin(p1.geom, p2.geom, 300)
  AND ST_DWithin(p2.geom, p3.geom, 300)
  AND ST_DWithin(p3.geom, p4.geom, 300);

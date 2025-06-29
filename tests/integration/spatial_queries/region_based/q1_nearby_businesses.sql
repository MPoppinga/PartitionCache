-- Find restaurants near pharmacies within the same region
-- This query demonstrates spatial proximity search with partition optimization
SELECT 
    r.name AS restaurant_name,
    p.name AS pharmacy_name,
    r.region_id,
    r.city_id,
    -- Simple distance calculation using Euclidean distance
    SQRT(POWER(r.x - p.x, 2) + POWER(r.y - p.y, 2)) AS distance_approx,
    r.rating AS restaurant_rating
FROM test_businesses r
INNER JOIN test_businesses p ON r.region_id = p.region_id
WHERE r.business_type = 'restaurant'
  AND p.business_type = 'pharmacy'
  AND r.id != p.id
  -- Distance filter: approximately 500 meters in decimal degrees (rough conversion)
  AND SQRT(POWER(r.x - p.x, 2) + POWER(r.y - p.y, 2)) < 0.005
  AND r.rating >= 3.0
ORDER BY r.region_id, distance_approx
LIMIT 100;
-- Find points of interest near specific business types
-- Tests cross-table spatial joins with different partition keys
SELECT 
    poi.name AS poi_name,
    poi.point_type,
    bus.name AS business_name,
    bus.business_type,
    poi.region_id,
    poi.city_id,
    SQRT(POWER(poi.x - bus.x, 2) + POWER(poi.y - bus.y, 2)) AS distance,
    bus.rating
FROM test_spatial_points poi
INNER JOIN test_businesses bus ON poi.region_id = bus.region_id
WHERE poi.point_type IN ('park', 'school', 'hospital')
  AND bus.business_type IN ('pharmacy', 'supermarket')
  -- Within 300 meters
  AND SQRT(POWER(poi.x - bus.x, 2) + POWER(poi.y - bus.y, 2)) < 0.003
  AND bus.rating >= 4.0
ORDER BY poi.region_id, poi.point_type, distance
LIMIT 200;
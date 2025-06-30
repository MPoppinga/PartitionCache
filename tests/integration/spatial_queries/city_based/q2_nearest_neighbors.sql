-- Find nearest neighbors for each restaurant in a city
-- Tests window functions with spatial calculations
WITH restaurant_distances AS (
  SELECT 
    r1.id AS restaurant_id,
    r1.name AS restaurant_name,
    r1.city_id,
    r2.id AS neighbor_id,
    r2.name AS neighbor_name,
    r2.business_type AS neighbor_type,
    SQRT(POWER(r1.x - r2.x, 2) + POWER(r1.y - r2.y, 2)) AS distance,
    ROW_NUMBER() OVER (
      PARTITION BY r1.id 
      ORDER BY SQRT(POWER(r1.x - r2.x, 2) + POWER(r1.y - r2.y, 2))
    ) AS rank
  FROM test_businesses r1
  INNER JOIN test_businesses r2 ON r1.city_id = r2.city_id
  WHERE r1.business_type = 'restaurant'
    AND r2.business_type != 'restaurant'
    AND r1.id != r2.id
    AND r1.rating >= 3.5
    AND SQRT(POWER(r1.x - r2.x, 2) + POWER(r1.y - r2.y, 2)) < 0.01  -- Within 1km
)
SELECT 
  restaurant_id,
  restaurant_name,
  city_id,
  neighbor_name,
  neighbor_type,
  distance,
  rank
FROM restaurant_distances
WHERE rank <= 3  -- Top 3 nearest neighbors
ORDER BY city_id, restaurant_id, rank;
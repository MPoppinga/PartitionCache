-- Find clusters of different business types in the same region
-- Tests multi-table joins with spatial constraints
SELECT 
    s.name AS supermarket_name,
    c.name AS cafe_name,
    b.name AS bank_name,
    s.region_id,
    s.zipcode,
    -- Calculate cluster center point
    (s.x + c.x + b.x) / 3.0 AS cluster_center_x,
    (s.y + c.y + b.y) / 3.0 AS cluster_center_y,
    -- Calculate cluster spread (max distance between any two points)
    GREATEST(
        SQRT(POWER(s.x - c.x, 2) + POWER(s.y - c.y, 2)),
        SQRT(POWER(s.x - b.x, 2) + POWER(s.y - b.y, 2)),
        SQRT(POWER(c.x - b.x, 2) + POWER(c.y - b.y, 2))
    ) AS cluster_spread
FROM test_businesses s
INNER JOIN test_businesses c ON s.region_id = c.region_id 
INNER JOIN test_businesses b ON s.region_id = b.region_id
WHERE s.business_type = 'supermarket'
  AND c.business_type = 'cafe'
  AND b.business_type = 'bank'
  AND s.id != c.id AND s.id != b.id AND c.id != b.id
  -- All businesses must be within 1km of each other
  AND SQRT(POWER(s.x - c.x, 2) + POWER(s.y - c.y, 2)) < 0.01
  AND SQRT(POWER(s.x - b.x, 2) + POWER(s.y - b.y, 2)) < 0.01
  AND SQRT(POWER(c.x - b.x, 2) + POWER(c.y - b.y, 2)) < 0.01
ORDER BY s.region_id, cluster_spread
LIMIT 50;
-- Find businesses within walking distance across zipcode boundaries
-- Tests cross-partition spatial queries
SELECT 
    b1.name AS business1_name,
    b1.business_type AS type1,
    b1.zipcode AS zipcode1,
    b2.name AS business2_name,
    b2.business_type AS type2,
    b2.zipcode AS zipcode2,
    SQRT(POWER(b1.x - b2.x, 2) + POWER(b1.y - b2.y, 2)) AS distance,
    (b1.rating + b2.rating) / 2.0 AS avg_rating
FROM test_businesses b1
INNER JOIN test_businesses b2 ON b1.region_id = b2.region_id  -- Same region but different zipcodes
WHERE b1.zipcode != b2.zipcode
  AND b1.business_type IN ('restaurant', 'cafe')
  AND b2.business_type IN ('pharmacy', 'supermarket')
  AND b1.id < b2.id  -- Avoid duplicates
  -- Walking distance: approximately 800 meters
  AND SQRT(POWER(b1.x - b2.x, 2) + POWER(b1.y - b2.y, 2)) < 0.008
  AND b1.rating >= 3.0 AND b2.rating >= 3.0
ORDER BY distance, avg_rating DESC
LIMIT 100;
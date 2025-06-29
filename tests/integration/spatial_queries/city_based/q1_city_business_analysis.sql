-- Analyze business distribution within cities
-- Tests city-level partitioning with aggregations
SELECT 
    city_id,
    business_type,
    COUNT(*) AS business_count,
    AVG(rating) AS avg_rating,
    MIN(x) AS min_x,
    MAX(x) AS max_x,
    MIN(y) AS min_y,
    MAX(y) AS max_y,
    -- Calculate city center based on business locations
    AVG(x) AS center_x,
    AVG(y) AS center_y,
    -- Calculate spread of businesses in city
    STDDEV(x) AS spread_x,
    STDDEV(y) AS spread_y
FROM test_businesses
WHERE rating >= 2.0
GROUP BY city_id, business_type
HAVING COUNT(*) >= 3  -- Only include business types with at least 3 locations
ORDER BY city_id, business_count DESC;
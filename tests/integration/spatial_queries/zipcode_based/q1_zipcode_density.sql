-- Calculate point and business density by zipcode
-- Tests zipcode partitioning with spatial density calculations
SELECT 
    z.zipcode,
    z.total_points,
    z.total_businesses,
    z.business_density,
    z.avg_business_rating,
    z.area_coverage,
    -- Find dominant business type in each zipcode
    z.dominant_business_type
FROM (
    SELECT 
        COALESCE(p.zipcode, b.zipcode) AS zipcode,
        COUNT(DISTINCT p.id) AS total_points,
        COUNT(DISTINCT b.id) AS total_businesses,
        CASE 
            WHEN COUNT(DISTINCT b.id) > 0 THEN 
                COUNT(DISTINCT b.id)::FLOAT / NULLIF(COUNT(DISTINCT p.id), 0)
            ELSE 0 
        END AS business_density,
        AVG(b.rating) AS avg_business_rating,
        -- Calculate approximate area coverage (bounding box)
        CASE 
            WHEN COUNT(*) > 1 THEN
                (MAX(COALESCE(p.x, b.x)) - MIN(COALESCE(p.x, b.x))) * 
                (MAX(COALESCE(p.y, b.y)) - MIN(COALESCE(p.y, b.y)))
            ELSE 0
        END AS area_coverage,
        -- Find most common business type
        MODE() WITHIN GROUP (ORDER BY b.business_type) AS dominant_business_type
    FROM test_spatial_points p
    FULL OUTER JOIN test_businesses b ON p.zipcode = b.zipcode
    WHERE COALESCE(p.zipcode, b.zipcode) IS NOT NULL
    GROUP BY COALESCE(p.zipcode, b.zipcode)
    HAVING COUNT(DISTINCT COALESCE(p.id, b.id)) >= 5  -- At least 5 total entities
) z
ORDER BY z.business_density DESC, z.total_businesses DESC
LIMIT 50;
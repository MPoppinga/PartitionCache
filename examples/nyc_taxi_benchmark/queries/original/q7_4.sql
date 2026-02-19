-- q7_4: Cross-dim - Park to Park, expensive per-mile
-- Cross-dimension reuse: combines park pickup (from Flight 5)
-- with expensive per-mile condition (from Flight 1) and park dropoff.
SELECT t.is_weekend,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount / NULLIF(t.trip_distance, 0)) AS avg_fare_per_mile,
       AVG(t.total_amount) AS avg_total
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 300)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 300)
WHERE p_start.poi_type = 'park'
  AND p_end.poi_type = 'park'
  AND t.fare_amount / NULLIF(t.trip_distance, 0) > 8
GROUP BY t.is_weekend
ORDER BY t.is_weekend

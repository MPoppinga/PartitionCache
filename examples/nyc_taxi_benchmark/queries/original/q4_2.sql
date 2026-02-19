-- q4_2: Nightlife - Bar to Subway, expensive per-mile
-- Expensive (>$8/mile) bar-to-subway trips, identifying bars
-- where patrons pay premium fares for short rides to transit.
SELECT p_start.name AS bar_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount / NULLIF(t.trip_distance, 0)) AS avg_fare_per_mile,
       AVG(t.total_amount) AS avg_total
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 150)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 200)
WHERE p_start.poi_type = 'bar'
  AND p_end.poi_type = 'station'
  AND t.fare_amount / NULLIF(t.trip_distance, 0) > 8
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

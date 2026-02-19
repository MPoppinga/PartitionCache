-- q8_1: Complexity - Museum, long+indirect+night, Hotel
-- Maximum complexity: stacks all cached conditions together.
-- Museum pickup + long trip (>45min) + indirect route (>3x) + nighttime (1-4am)
-- + hotel dropoff.
SELECT p_start.name AS museum_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0)) AS avg_distance_ratio
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 200)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 200)
WHERE p_start.poi_type = 'museum'
  AND p_end.poi_type = 'hotel'
  AND t.duration_seconds > 2700
  AND t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3
  AND t.pickup_hour BETWEEN 1 AND 4
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

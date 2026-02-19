-- q3_2: Commuter - Subway to Hotel, congested
-- Congested subway-to-hotel rides (<5mph, >30min), showing
-- transit bottlenecks affecting hotel-bound travelers.
SELECT p_start.name AS station_name,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0)) AS avg_speed_mph
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 200)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 200)
WHERE p_start.poi_type = 'station'
  AND p_end.poi_type = 'hotel'
  AND t.duration_seconds > 1800
  AND t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0) < 5
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

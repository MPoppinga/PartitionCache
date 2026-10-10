-- q2_2: Medical - Hospital to Hospital, congested
-- Congested trips (<5mph, >30min) between hospitals, highlighting
-- traffic-impacted medical transport corridors.
SELECT p_start.name AS from_hospital,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0)) AS avg_speed_mph
FROM taxi_trips t, osm_pois p_start
WHERE ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND p_start.poi_type = 'hospital'
  AND t.duration_seconds > 1800
  AND t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0) < 5
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'hospital'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  )
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

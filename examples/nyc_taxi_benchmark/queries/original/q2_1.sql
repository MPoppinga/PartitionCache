-- q2_1: Medical - Hospital to Hospital, nighttime
-- Night (1-4am) inter-hospital trips, potentially representing
-- emergency patient transfers or staff shift changes.
SELECT p_start.name AS from_hospital,
       p_end.name AS to_hospital,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.fare_amount) AS avg_fare
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  AND p_start.poi_type = 'hospital'
  AND p_end.poi_type = 'hospital'
  AND t.pickup_hour BETWEEN 1 AND 4
GROUP BY p_start.name, p_end.name
ORDER BY trip_count DESC
LIMIT 20

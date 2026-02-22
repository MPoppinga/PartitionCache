-- q7_1: Cross-dim - Museum to Hospital, nighttime
-- Cross-dimension reuse: combines museum pickup (from Flight 1)
-- with nighttime condition (from Flight 2) and hospital dropoff.
SELECT p_end.name AS hospital_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t, osm_pois p_end
WHERE ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  AND p_end.poi_type = 'hospital'
  AND t.pickup_hour BETWEEN 1 AND 4
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'museum'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
GROUP BY p_end.name
ORDER BY trip_count DESC
LIMIT 20

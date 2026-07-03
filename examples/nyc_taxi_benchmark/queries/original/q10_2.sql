-- q10_2: Community - Library to Cafe, afternoon
-- Afternoon library-to-cafe trips (study break pattern).
SELECT COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.trip_distance) AS avg_distance,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t
WHERE t.pickup_hour BETWEEN 12 AND 18
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'library'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'cafe'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  )

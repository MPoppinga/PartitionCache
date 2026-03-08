-- q9_1: Daily Life - Cafe to Supermarket, morning trips
-- Morning (6-10am) trips from cafes to supermarkets, revealing daily routine patterns.
SELECT COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.trip_distance) AS avg_distance,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t
WHERE t.pickup_hour BETWEEN 6 AND 10
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'cafe'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'supermarket'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )

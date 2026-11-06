-- q3_1: Commuter - Subway to Subway, far trips
-- Long-distance (>10 mile) subway-to-subway commutes broken down
-- by hour to reveal commuter peak patterns.
SELECT EXTRACT(HOUR FROM t.pickup_datetime) AS hour,
       COUNT(*) AS trip_count,
       AVG(t.trip_distance) AS avg_distance,
       AVG(t.fare_amount) AS avg_fare
FROM taxi_trips t
WHERE t.trip_distance > 10
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'station'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'station'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY EXTRACT(HOUR FROM t.pickup_datetime)
ORDER BY hour

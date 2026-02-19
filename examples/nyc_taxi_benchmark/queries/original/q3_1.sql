-- q3_1: Commuter - Subway to Subway, far trips
-- Long-distance (>10 mile) subway-to-subway commutes broken down
-- by hour to reveal commuter peak patterns.
SELECT EXTRACT(HOUR FROM t.pickup_datetime) AS hour,
       COUNT(*) AS trip_count,
       AVG(t.trip_distance) AS avg_distance,
       AVG(t.fare_amount) AS avg_fare
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 200)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 200)
WHERE p_start.poi_type = 'station'
  AND p_end.poi_type = 'station'
  AND t.trip_distance > 10
GROUP BY EXTRACT(HOUR FROM t.pickup_datetime)
ORDER BY hour

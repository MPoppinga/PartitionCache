-- q7_2: Cross-dim - Hospital to Subway, indirect routes
-- Cross-dimension reuse: combines hospital pickup (from Flight 2)
-- with indirect route condition (from Flight 1) and subway dropoff.
SELECT p_start.name AS hospital_name,
       COUNT(*) AS trip_count,
       AVG(t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0)) AS avg_distance_ratio,
       AVG(t.fare_amount) AS avg_fare
FROM taxi_trips t, osm_pois p_start
WHERE ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND p_start.poi_type = 'hospital'
  AND t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'station'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

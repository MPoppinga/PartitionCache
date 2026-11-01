-- q7_3: Cross-dim - Bar to Museum, long trips
-- Cross-dimension reuse: combines bar pickup (from Flight 4)
-- with long trip condition (from Flight 1) and museum dropoff (new endpoint).
SELECT p_end.name AS museum_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t, osm_pois p_end
WHERE ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'museum'
  AND t.duration_seconds > 2700
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'bar'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  )
GROUP BY p_end.name
ORDER BY trip_count DESC
LIMIT 20

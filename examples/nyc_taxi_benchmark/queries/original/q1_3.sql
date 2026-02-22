-- q1_3: Tourism - Museum to Subway, expensive per-mile
-- Trips from museums to subway stations with fare >$8/mile,
-- indicating short, expensive rides typical of tourist areas.
SELECT p_start.name AS museum_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount / NULLIF(t.trip_distance, 0)) AS avg_fare_per_mile,
       AVG(t.total_amount) AS avg_total
FROM taxi_trips t, osm_pois p_start
WHERE ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'museum'
  AND t.fare_amount / NULLIF(t.trip_distance, 0) > 8
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'station'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

-- q11_2: Hospital pickup → Museum dropoff, medium-distance trips (2–8 miles)
-- Spatial selectivity: hospital@100ft=1.4%, museum@100ft=4.7%, combined spatial=0.059% (8,581 trips)
-- Trip condition: trip_distance BETWEEN 2 AND 8 (~40% of trips)
-- Final selectivity: ~0.012% (≈1,795 trips from 14.5M)
-- Most selective cached fragment (no trip filter): 8,581 → 99.94% reduction before query execution
SELECT passenger_count,
       COUNT(*) AS trip_count,
       ROUND(AVG(t.fare_amount)::numeric, 2) AS avg_fare,
       ROUND(AVG(t.trip_distance)::numeric, 2) AS avg_distance_miles,
       ROUND(AVG(t.duration_seconds / 60.0)::numeric, 1) AS avg_duration_min
FROM taxi_trips t
WHERE t.trip_distance BETWEEN 2 AND 8
  AND EXISTS (
    SELECT 1 FROM osm_pois p
    WHERE p.poi_type = 'hospital'
      AND ST_DWithin(t.pickup_geom, p.geom, 100)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p
    WHERE p.poi_type = 'museum'
      AND ST_DWithin(t.dropoff_geom, p.geom, 100)
  )
GROUP BY passenger_count
ORDER BY passenger_count

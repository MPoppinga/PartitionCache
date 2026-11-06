-- q11_3: University pickup → University dropoff, fare > $8 (cross-campus trips)
-- Spatial selectivity: university@100ft pickup=1.7%, university@100ft dropoff=1.7%
-- Combined spatial (pickup+dropoff, no fare): 0.043% (6,251 trips)
-- Trip condition: fare_amount > 8 (~30–40% of trips)
-- Final selectivity: ~0.013% (≈1,833 trips from 14.5M)
-- Most selective cached fragment (no fare filter): 6,251 → 99.96% reduction before query execution
SELECT is_weekend,
       COUNT(*) AS trip_count,
       ROUND(AVG(t.fare_amount)::numeric, 2) AS avg_fare,
       ROUND(AVG(t.trip_distance)::numeric, 2) AS avg_distance_miles,
       ROUND(AVG(t.tip_amount)::numeric, 2) AS avg_tip
FROM taxi_trips t
WHERE t.fare_amount > 8
  AND EXISTS (
    SELECT 1 FROM osm_pois p
    WHERE p.poi_type = 'university'
      AND ST_DWithin(t.pickup_geom, p.geom, 100)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p
    WHERE p.poi_type = 'university'
      AND ST_DWithin(t.dropoff_geom, p.geom, 100)
  )
GROUP BY is_weekend
ORDER BY is_weekend

-- q11_1: Hospital pickup → University dropoff, tipped trips
-- Targets the narrow intersection of two rare spatial filters combined with a trip condition.
-- Spatial selectivity: hospital@100ft=1.4%, university@100ft=1.7%, combined spatial=0.026% (3,775 trips)
-- Trip condition: tip_amount>0 (~31%)
-- Final selectivity: ~0.008% (≈1,141 trips from 14.5M)
-- Most selective cached fragment (no trip filter): 3,775 → 99.97% reduction before query execution
SELECT pickup_hour,
       COUNT(*) AS trip_count,
       ROUND(AVG(t.fare_amount)::numeric, 2) AS avg_fare,
       ROUND(AVG(t.tip_amount)::numeric, 2) AS avg_tip,
       ROUND(AVG(t.trip_distance)::numeric, 2) AS avg_distance_miles
FROM taxi_trips t
WHERE t.tip_amount > 0
  AND EXISTS (
    SELECT 1 FROM osm_pois p
    WHERE p.poi_type = 'hospital'
      AND ST_DWithin(t.pickup_geom, p.geom, 100)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p
    WHERE p.poi_type = 'university'
      AND ST_DWithin(t.dropoff_geom, p.geom, 100)
  )
GROUP BY pickup_hour
ORDER BY pickup_hour

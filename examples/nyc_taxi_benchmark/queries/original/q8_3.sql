-- q8_3: Complexity - Bar, expensive+night, near Bar AND Hotel
-- Maximum complexity with dual dropoff spatial join.
-- Bar pickup + expensive per-mile (>$8/mi) + nighttime (1-4am)
-- + dropoff near both a bar AND a hotel simultaneously.
SELECT t.is_weekend,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.tip_amount) AS avg_tip,
       AVG(t.total_amount) AS avg_total
FROM taxi_trips t
WHERE t.fare_amount / NULLIF(t.trip_distance, 0) > 8
  AND t.pickup_hour BETWEEN 1 AND 4
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'bar'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end_bar
    WHERE p_end_bar.poi_type = 'bar'
      AND ST_DWithin(t.dropoff_geom, p_end_bar.geom, 150)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end_hotel
    WHERE p_end_hotel.poi_type = 'hotel'
      AND ST_DWithin(t.dropoff_geom, p_end_hotel.geom, 200)
  )
GROUP BY t.is_weekend
ORDER BY t.is_weekend

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
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 150)
JOIN osm_pois p_end_bar ON ST_DWithin(t.dropoff_geom, p_end_bar.geom, 150)
JOIN osm_pois p_end_hotel ON ST_DWithin(t.dropoff_geom, p_end_hotel.geom, 200)
WHERE p_start.poi_type = 'bar'
  AND p_end_bar.poi_type = 'bar'
  AND p_end_hotel.poi_type = 'hotel'
  AND t.fare_amount / NULLIF(t.trip_distance, 0) > 8
  AND t.pickup_hour BETWEEN 1 AND 4
GROUP BY t.is_weekend
ORDER BY t.is_weekend

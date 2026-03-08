-- q9_2: Daily Life - School to Pharmacy, weekday daytime
-- Weekday daytime trips from schools to pharmacies.
SELECT COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.trip_distance) AS avg_distance
FROM taxi_trips t
WHERE NOT t.is_weekend
  AND t.pickup_hour BETWEEN 8 AND 15
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'school'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'pharmacy'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )

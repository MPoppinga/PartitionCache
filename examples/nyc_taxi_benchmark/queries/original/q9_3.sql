-- q9_3: Daily Life - Nightclub to Hotel, late night
-- Late-night (midnight-5am) nightclub-to-hotel trips with tipping analysis.
SELECT t.is_weekend,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.tip_amount) AS avg_tip,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t
WHERE (t.pickup_hour >= 23 OR t.pickup_hour < 5)
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'nightclub'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'hotel'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY t.is_weekend

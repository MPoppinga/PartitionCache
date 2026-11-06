-- q10_3: Community - Cinema to Restaurant, evening
-- Evening cinema-to-restaurant trips (dinner and a movie pattern).
SELECT t.is_weekend,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.tip_amount) AS avg_tip
FROM taxi_trips t
WHERE t.pickup_hour BETWEEN 19 AND 23
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'cinema'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'restaurant'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  )
GROUP BY t.is_weekend

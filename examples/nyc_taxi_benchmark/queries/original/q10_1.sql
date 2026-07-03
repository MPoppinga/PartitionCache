-- q10_1: Community - Place of Worship to Restaurant, weekend
-- Weekend midday trips from places of worship to restaurants (brunch pattern).
SELECT EXTRACT(DOW FROM t.pickup_datetime) AS day_of_week,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.trip_distance) AS avg_distance
FROM taxi_trips t
WHERE t.is_weekend
  AND t.pickup_hour BETWEEN 10 AND 14
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'place_of_worship'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'restaurant'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  )
GROUP BY EXTRACT(DOW FROM t.pickup_datetime)
ORDER BY day_of_week

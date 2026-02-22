-- q4_1: Nightlife - Bar to Hotel, nighttime
-- Late-night (1-4am) bar-to-hotel trips broken down by day of week
-- to reveal weekend vs weekday nightlife patterns and tipping behavior.
SELECT EXTRACT(DOW FROM t.pickup_datetime) AS day_of_week,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.tip_amount) AS avg_tip,
       AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) AS avg_tip_pct
FROM taxi_trips t
WHERE t.pickup_hour BETWEEN 1 AND 4
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'bar'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'hotel'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY EXTRACT(DOW FROM t.pickup_datetime)
ORDER BY day_of_week

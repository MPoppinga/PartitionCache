-- q2_3: Medical - Hospital to Hotel, long trips
-- Hospital to hotel trips lasting >45min, representing patient
-- discharge to nearby hotel accommodations.
SELECT p_start.name AS hospital_name,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.tip_amount) AS avg_tip
FROM taxi_trips t, osm_pois p_start
WHERE ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND p_start.poi_type = 'hospital'
  AND t.duration_seconds > 2700
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'hotel'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

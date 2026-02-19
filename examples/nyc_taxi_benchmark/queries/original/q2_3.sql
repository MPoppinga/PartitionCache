-- q2_3: Medical - Hospital to Hotel, long trips
-- Hospital to hotel trips lasting >45min, representing patient
-- discharge to nearby hotel accommodations.
SELECT p_start.name AS hospital_name,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.tip_amount) AS avg_tip
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 300)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 200)
WHERE p_start.poi_type = 'hospital'
  AND p_end.poi_type = 'hotel'
  AND t.duration_seconds > 2700
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

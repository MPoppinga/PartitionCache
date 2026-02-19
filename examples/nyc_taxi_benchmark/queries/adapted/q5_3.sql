-- Flight 5: Start Hierarchy (park -> museum -> theatre)
-- Q5_3: S_THEATRE(100m) + T_LONG(>45min) + E_HOTEL(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.duration_seconds > 2700
  AND ST_DWithin(t.pickup_geom, p_start.geom, 100)
  AND p_start.poi_type = 'theatre'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'hotel'

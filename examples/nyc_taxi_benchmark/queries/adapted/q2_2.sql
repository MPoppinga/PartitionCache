-- Flight 2: Medical/Emergency Pattern
-- Q2_2: S_HOSPITAL(300m) + T_CONGESTED(>30min,<5mph) + E_HOSPITAL(300m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.duration_seconds > 1800
  AND t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0) < 5
  AND ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND p_start.poi_type = 'hospital'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  AND p_end.poi_type = 'hospital'

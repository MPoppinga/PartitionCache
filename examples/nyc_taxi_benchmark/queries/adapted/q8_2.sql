-- Flight 8: Maximum Complexity
-- Q8_2: S_SUBWAY(200m) + T_FAR + T_CONGESTED + T_ANOMALY + E_HOSPITAL(300m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.trip_distance > 10
  AND t.duration_seconds > 1800
  AND t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0) < 5
  AND t.duration_seconds > 2.0 * (ST_Distance(t.pickup_geom, t.dropoff_geom) / 1609.34) / 12.0 * 3600
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'station'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  AND p_end.poi_type = 'hospital'

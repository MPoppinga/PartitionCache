-- Flight 3: Commuter Pattern
-- Q3_3: S_SUBWAY(200m) + T_ANOMALY(>2x) + E_PARK(300m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.duration_seconds > 2.0 * (ST_Distance(t.pickup_geom, t.dropoff_geom) / 1609.34) / 12.0 * 3600
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'station'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  AND p_end.poi_type = 'park'

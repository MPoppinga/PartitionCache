-- Flight 3: Commuter Pattern
-- Q3_2: S_SUBWAY(200m) + T_CONGESTED(>30min,<5mph) + E_HOTEL(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.duration_seconds > 1800
  AND t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0) < 5
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'station'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'hotel'

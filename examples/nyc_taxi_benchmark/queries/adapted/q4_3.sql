-- Flight 4: Nightlife Pattern
-- Q4_3: S_BAR(150m) + T_INDIRECT(>3x) + E_BAR(150m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3
  AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  AND p_start.poi_type = 'bar'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  AND p_end.poi_type = 'bar'

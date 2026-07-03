-- Flight 7: Cross-Dimension Reuse
-- Q7_2: S_HOSPITAL(300m) + T_INDIRECT(>3x) + E_STATION(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3
  AND ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND p_start.poi_type = 'hospital'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'station'

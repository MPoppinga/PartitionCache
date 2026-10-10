-- Flight 7: Cross-Dimension Reuse
-- Q7_3: S_BAR(150m) + T_LONG(>45min) + E_MUSEUM(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.duration_seconds > 2700
  AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  AND p_start.poi_type = 'bar'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'museum'

-- Flight 9: Daily Life Patterns
-- Q9_1: S_CAFE(150m) + T_MORNING(6-10) + E_SUPERMARKET(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.pickup_hour BETWEEN 6 AND 10
  AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  AND p_start.poi_type = 'cafe'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'supermarket'

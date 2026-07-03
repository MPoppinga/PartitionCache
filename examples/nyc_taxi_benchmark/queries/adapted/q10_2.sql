-- Flight 10: Community & Culture Patterns
-- Q10_2: S_LIBRARY(200m) + T_AFTERNOON(12-18) + E_CAFE(150m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.pickup_hour BETWEEN 12 AND 18
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'library'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  AND p_end.poi_type = 'cafe'

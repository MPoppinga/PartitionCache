-- Flight 10: Community & Culture Patterns
-- Q10_3: S_CINEMA(200m) + T_EVENING(19-23) + E_RESTAURANT(150m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.pickup_hour BETWEEN 19 AND 23
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'cinema'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  AND p_end.poi_type = 'restaurant'

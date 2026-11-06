-- Flight 9: Daily Life Patterns
-- Q9_2: S_SCHOOL(200m) + T_WEEKDAY + T_DAYTIME(8-15) + E_PHARMACY(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE NOT t.is_weekend
  AND t.pickup_hour BETWEEN 8 AND 15
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'school'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'pharmacy'

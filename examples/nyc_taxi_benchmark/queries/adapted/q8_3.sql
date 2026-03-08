-- Flight 8: Maximum Complexity
-- Q8_3: S_BAR(150m) + T_EXPENSIVE + T_NIGHT + E_BAR(150m) + E_HOTEL(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables (3 spatial joins)

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end1, osm_pois p_end2
WHERE t.fare_amount / NULLIF(t.trip_distance, 0) > 8
  AND t.pickup_hour BETWEEN 1 AND 4
  AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  AND p_start.poi_type = 'bar'
  AND ST_DWithin(t.dropoff_geom, p_end1.geom, 150)
  AND p_end1.poi_type = 'bar'
  AND ST_DWithin(t.dropoff_geom, p_end2.geom, 200)
  AND p_end2.poi_type = 'hotel'

-- Flight 4: Nightlife Pattern
-- Q4_2: S_BAR(150m) + T_EXPENSIVE(>$8/mi) + E_STATION(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.fare_amount / NULLIF(t.trip_distance, 0) > 8
  AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  AND p_start.poi_type = 'bar'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'station'

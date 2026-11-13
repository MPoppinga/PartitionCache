-- Flight 7: Cross-Dimension Reuse
-- Q7_4: S_PARK(300m) + T_EXPENSIVE(>$8/mi) + E_PARK(300m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.fare_amount / NULLIF(t.trip_distance, 0) > 8
  AND ST_DWithin(t.pickup_geom, p_start.geom, 300)
  AND p_start.poi_type = 'park'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 300)
  AND p_end.poi_type = 'park'

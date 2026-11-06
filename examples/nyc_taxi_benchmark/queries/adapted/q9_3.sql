-- Flight 9: Daily Life Patterns
-- Q9_3: S_NIGHTCLUB(150m) + T_LATENIGHT(23-5) + E_HOTEL(200m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE (t.pickup_hour >= 23 OR t.pickup_hour < 5)
  AND ST_DWithin(t.pickup_geom, p_start.geom, 150)
  AND p_start.poi_type = 'nightclub'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  AND p_end.poi_type = 'hotel'

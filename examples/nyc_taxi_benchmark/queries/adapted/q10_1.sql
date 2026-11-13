-- Flight 10: Community & Culture Patterns
-- Q10_1: S_PLACE_OF_WORSHIP(200m) + T_WEEKEND + T_MIDDAY(10-14) + E_RESTAURANT(150m)
-- Flat join: taxi_trips fact table with osm_pois dimension tables

SELECT t.trip_id
FROM taxi_trips t, osm_pois p_start, osm_pois p_end
WHERE t.is_weekend
  AND t.pickup_hour BETWEEN 10 AND 14
  AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  AND p_start.poi_type = 'place_of_worship'
  AND ST_DWithin(t.dropoff_geom, p_end.geom, 150)
  AND p_end.poi_type = 'restaurant'

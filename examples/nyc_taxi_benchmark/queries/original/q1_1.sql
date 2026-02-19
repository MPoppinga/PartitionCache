-- q1_1: Tourism - Museum to Hotel, long trips
-- Average fare and duration for trips that start near museums (200m),
-- last >45 min, and end near hotels (200m). Grouped by museum name.
SELECT p_start.name AS museum_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.trip_distance) AS avg_distance
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 200)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 200)
WHERE p_start.poi_type = 'museum'
  AND p_end.poi_type = 'hotel'
  AND t.duration_seconds > 2700
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

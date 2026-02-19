-- q4_3: Nightlife - Bar to Bar, indirect routes
-- Bar-hopping trips with indirect routes (>3x straight-line distance),
-- comparing weekend vs weekday patterns.
SELECT t.is_weekend,
       COUNT(*) AS trip_count,
       AVG(t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0)) AS avg_distance_ratio,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 150)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 150)
WHERE p_start.poi_type = 'bar'
  AND p_end.poi_type = 'bar'
  AND t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3
GROUP BY t.is_weekend
ORDER BY t.is_weekend

-- q3_3: Commuter - Subway to Park, anomalous duration
-- Subway-to-park trips where duration is >2x what straight-line
-- distance suggests at 12mph, indicating unusual routing or stops.
SELECT p_end.name AS park_name,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(ST_Distance(t.pickup_geom, t.dropoff_geom)) AS avg_straight_line_m
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 200)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 300)
WHERE p_start.poi_type = 'station'
  AND p_end.poi_type = 'park'
  AND t.duration_seconds > 2.0 * (ST_Distance(t.pickup_geom, t.dropoff_geom) / 1609.34) / 12.0 * 3600
GROUP BY p_end.name
ORDER BY trip_count DESC
LIMIT 20

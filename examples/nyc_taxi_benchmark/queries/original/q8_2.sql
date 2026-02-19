-- q8_2: Complexity - Subway, far+congested+anomalous, Hospital
-- Maximum complexity: stacks all cached conditions together.
-- Subway pickup + far (>10mi) + congested (<5mph, >30min) + anomalous duration
-- + hospital dropoff.
SELECT p_start.name AS station_name,
       COUNT(*) AS trip_count,
       AVG(t.fare_amount) AS avg_fare,
       AVG(t.duration_seconds) AS avg_duration
FROM taxi_trips t
JOIN osm_pois p_start ON ST_DWithin(t.pickup_geom, p_start.geom, 200)
JOIN osm_pois p_end ON ST_DWithin(t.dropoff_geom, p_end.geom, 300)
WHERE p_start.poi_type = 'station'
  AND p_end.poi_type = 'hospital'
  AND t.trip_distance > 10
  AND t.duration_seconds > 1800
  AND t.trip_distance / NULLIF(t.duration_seconds / 3600.0, 0) < 5
  AND t.duration_seconds > 2.0 * (ST_Distance(t.pickup_geom, t.dropoff_geom) / 1609.34) / 12.0 * 3600
GROUP BY p_start.name
ORDER BY trip_count DESC
LIMIT 20

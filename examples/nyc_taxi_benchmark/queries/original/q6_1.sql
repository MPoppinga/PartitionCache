-- q6_1: Trip Hierarchy - Subway to Subway, anomalous (broadest)
-- Trip condition hierarchy drill-down: broadest condition (anomalous duration).
-- Constant start+end: S_SUBWAY(200m) + E_SUBWAY(200m).
-- Anomalous = duration >2x expected at 12mph straight-line.
SELECT EXTRACT(HOUR FROM t.pickup_datetime) AS hour,
       COUNT(*) AS trip_count,
       AVG(t.duration_seconds) AS avg_duration,
       AVG(t.trip_distance) AS avg_distance
FROM taxi_trips t
WHERE t.duration_seconds > 2.0 * (ST_Distance(t.pickup_geom, t.dropoff_geom) / 1609.34) / 12.0 * 3600
  AND EXISTS (
    SELECT 1 FROM osm_pois p_start
    WHERE p_start.poi_type = 'station'
      AND ST_DWithin(t.pickup_geom, p_start.geom, 200)
  )
  AND EXISTS (
    SELECT 1 FROM osm_pois p_end
    WHERE p_end.poi_type = 'station'
      AND ST_DWithin(t.dropoff_geom, p_end.geom, 200)
  )
GROUP BY EXTRACT(HOUR FROM t.pickup_datetime)
ORDER BY hour

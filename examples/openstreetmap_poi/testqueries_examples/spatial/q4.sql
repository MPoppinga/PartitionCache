SELECT p1.name, p1.type, p1.subtype
FROM pois AS p1
WHERE p1.name LIKE '%Museum%'
  AND p1.subtype = 'museum'
  AND ST_DWithin(p1.geom, ST_Transform(ST_SetSRID(ST_MakePoint(10.0, 51.0), 4326), 25832), 50000);

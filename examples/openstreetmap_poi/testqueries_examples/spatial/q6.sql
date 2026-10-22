SELECT p1.name AS school_name,
       p2.name AS library_name,
       p3.name AS hospital_name
FROM pois AS p1
   , pois AS p2
   , pois AS p3
WHERE p1.subtype = 'school'
  AND p2.subtype = 'library'
  AND p3.subtype = 'hospital'
  AND p1.name LIKE '%Gymnasium%'
  AND p2.name LIKE '%ücherei%'
  AND ST_DWithin(p1.geom, p2.geom, 500)
  AND ST_DWithin(p1.geom, p3.geom, 500);

-- Derived from LDBC SNB BI bi-9 (Top thread initiators)
-- Parameters (official): startDate=2012-08-31, endDate=2012-12-03
-- Rewrite: Post_View inlined as m.parentmessageid IS NULL; the MPP pre-aggregation CTE removed (it collapses into the same flat join); ORDER BY/LIMIT dropped.
SELECT p.id AS person_id, p.firstname, p.lastname,
       count(DISTINCT m.messageid) AS threadcount,
       count(DISTINCT r.messageid) AS messagecount
FROM message AS m, person AS p, message AS r
WHERE p.id = m.creatorpersonid
  AND m.parentmessageid IS NULL
  AND m.creationdate >= DATE '2012-08-31' AND m.creationdate <= DATE '2012-12-03'
  AND r.rootpostid = m.messageid
  AND r.creationdate >= DATE '2012-08-31' AND r.creationdate <= DATE '2012-12-03'
GROUP BY p.id, p.firstname, p.lastname

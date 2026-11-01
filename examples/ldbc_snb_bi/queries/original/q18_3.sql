-- Derived from LDBC SNB BI bi-18 (Friend recommendation)
-- Parameters (official): tag=Johann_Sebastian_Bach
-- Rewrite: The two CTEs are inlined; the NOT EXISTS anti-join is kept verbatim; ORDER BY/LIMIT dropped.
SELECT i1.personid AS person1_id, i2.personid AS person2_id,
       count(*) AS mutualfriendcount
FROM person_knows_person AS k1, person_hasinterest_tag AS i1, tag AS t,
     person_knows_person AS k2, person_hasinterest_tag AS i2
WHERE t.id = i1.tagid AND t.name = 'Johann_Sebastian_Bach'
  AND t.id = i2.tagid
  AND k1.person1id = i1.personid
  AND k2.person1id = i2.personid
  AND k1.person2id = k2.person2id
  AND i1.personid < i2.personid
  AND NOT EXISTS (SELECT 1 FROM person_knows_person AS k3
                  WHERE k3.person1id = i1.personid AND k3.person2id = i2.personid)
GROUP BY i1.personid, i2.personid

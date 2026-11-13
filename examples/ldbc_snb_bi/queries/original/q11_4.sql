-- Derived from LDBC SNB BI bi-11 (Friend triangles)
-- Parameters (official): country=India, startDate=2012-10-18, endDate=2013-01-30
-- Rewrite: The Persons_of_country_w_friends CTE is inlined three times, once per triangle corner. Structurally unchanged otherwise - bi-11 is already a pure conjunctive self-join with AND-ed ordering and date-range predicates.
SELECT count(*) AS triangle_count
FROM person_knows_person AS k1, person AS p1, city AS ci1, country AS co1,
     person_knows_person AS k2, person AS p2, city AS ci2, country AS co2,
     person_knows_person AS k3, person AS p3, city AS ci3, country AS co3
WHERE k1.person1id = p1.id AND ci1.id = p1.locationcityid
  AND co1.id = ci1.partofcountryid AND co1.name = 'India'
  AND k2.person1id = p2.id AND ci2.id = p2.locationcityid
  AND co2.id = ci2.partofcountryid AND co2.name = 'India'
  AND k3.person1id = p3.id AND ci3.id = p3.locationcityid
  AND co3.id = ci3.partofcountryid AND co3.name = 'India'
  AND k1.person2id = k2.person1id
  AND k2.person2id = k3.person1id
  AND k3.person2id = k1.person1id
  AND k1.person1id < k2.person1id
  AND k2.person1id < k3.person1id
  AND k1.creationdate >= DATE '2012-10-18' AND k1.creationdate <= DATE '2013-01-30'
  AND k2.creationdate >= DATE '2012-10-18' AND k2.creationdate <= DATE '2013-01-30'
  AND k3.creationdate >= DATE '2012-10-18' AND k3.creationdate <= DATE '2013-01-30'

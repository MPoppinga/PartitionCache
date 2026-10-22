-- Derived from LDBC SNB BI bi-3 (Popular topics in a country)
-- Parameters (official): tagClass=TennisPlayer, country=India
-- Rewrite: EXISTS over Message_hasTag_Tag/Tag/TagClass promoted to inner joins; count(Message.MessageId) -> count(DISTINCT m.messageid) because a message may carry several tags of the class; ORDER BY/LIMIT dropped.
SELECT f.id AS forum_id, f.title AS forum_title, f.moderatorpersonid AS person_id,
       count(DISTINCT m.messageid) AS messagecount
FROM message AS m, forum AS f, person AS modp, city AS ci, country AS co,
     message_hastag_tag AS mt, tag AS t, tagclass AS tc
WHERE f.id = m.containerforumid
  AND modp.id = f.moderatorpersonid
  AND ci.id = modp.locationcityid
  AND co.id = ci.partofcountryid
  AND co.name = 'India'
  AND mt.messageid = m.messageid
  AND t.id = mt.tagid
  AND tc.id = t.typetagclassid
  AND tc.name = 'TennisPlayer'
GROUP BY f.id, f.title, f.moderatorpersonid

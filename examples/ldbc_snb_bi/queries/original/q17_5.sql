-- Derived from LDBC SNB BI bi-17 (Information propagation analysis)
-- Parameters (official): tag=He_Said_She_Said, delta=12
-- Rewrite: The MyMessage CTE is inlined three times as explicit message/message_hastag_tag/tag joins; the NOT EXISTS anti-join is kept verbatim (nested negation is supported); the non-equi conjuncts (interval comparison, two disequalities) are kept; ORDER BY/LIMIT dropped.
SELECT m.creatorpersonid AS person1_id, count(DISTINCT m2.messageid) AS messagecount
FROM message AS m, message_hastag_tag AS mt1, tag AS t,
     message AS m2, message_hastag_tag AS mt2,
     message AS c, message_hastag_tag AS mt3,
     forum_hasmember_person AS f2, forum_hasmember_person AS f3
WHERE mt1.messageid = m.messageid AND t.id = mt1.tagid AND t.name = 'He_Said_She_Said'
  AND mt2.messageid = m2.messageid AND mt2.tagid = t.id
  AND mt3.messageid = c.messageid AND mt3.tagid = t.id
  AND m.creationdate + INTERVAL '12 hour' < m2.creationdate
  AND c.parentmessageid = m2.messageid
  AND f2.forumid = m.containerforumid AND f2.personid = c.creatorpersonid
  AND f3.forumid = m.containerforumid AND f3.personid = m2.creatorpersonid
  AND m.containerforumid <> m2.containerforumid
  AND f2.personid <> f3.personid
  AND NOT EXISTS (SELECT 1 FROM forum_hasmember_person AS f1
                  WHERE f1.forumid = m2.containerforumid
                    AND f1.personid = m.creatorpersonid)
GROUP BY m.creatorpersonid

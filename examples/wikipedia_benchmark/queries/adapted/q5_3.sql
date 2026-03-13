-- Flight 5: Fragment reuse - same relational as Flight 3
-- Q5_3 adapted: relational filters only (reuses Flight 3 cached fragments)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c
              WHERE c ILIKE '%scien%' OR c ILIKE '%physic%'
                 OR c ILIKE '%chemi%' OR c ILIKE '%biolog%')
  AND a.edit_count > 100
  AND a.num_references > 20

-- Flight 3: Science categories + heavy edits + many references
-- Q3_3 adapted: relational filters only (shared by q3_1/q3_2/q3_3)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c
              WHERE c ILIKE '%scien%' OR c ILIKE '%physic%'
                 OR c ILIKE '%chemi%' OR c ILIKE '%biolog%')
  AND a.edit_count > 100
  AND a.num_references > 20

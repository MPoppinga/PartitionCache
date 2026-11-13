-- Flight 5: Fragment reuse - same relational as Flight 1
-- Q5_1 adapted: relational filters only (reuses Flight 1 cached fragments)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005

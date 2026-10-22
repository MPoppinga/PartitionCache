-- Flight 1: History category + high edits + recent
-- Q1_1 adapted: relational filters only (shared by q1_1/q1_2/q1_3)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005

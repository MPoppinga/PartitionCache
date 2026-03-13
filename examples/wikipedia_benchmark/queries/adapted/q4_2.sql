-- Flight 4: Multi-constraint (history + year + length + edits + infobox)
-- Q4_2 adapted: relational filters only (shared by q4_1/q4_2/q4_3)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.creation_year >= 2008
  AND a.content_length BETWEEN 3000 AND 50000
  AND a.edit_count > 30
  AND a.infobox_type IS NOT NULL

-- Flight 5: Fragment reuse - same relational as Flight 2
-- Q5_2 adapted: relational filters only (reuses Flight 2 cached fragments)
SELECT a.article_id
FROM wikipedia_articles a
WHERE a.creation_year BETWEEN 2010 AND 2015
  AND a.content_length > 5000

-- Flight 2: Date range + content length
-- Q2_3 adapted: relational filters only (shared by q2_1/q2_2/q2_3)
SELECT a.article_id
FROM wikipedia_articles a
WHERE a.creation_year BETWEEN 2010 AND 2015
  AND a.content_length > 5000

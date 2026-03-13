-- Flight 2: Date range + content length + LLM
-- Q2_2: Notable biographies with controversy (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE a.creation_year BETWEEN 2010 AND 2015
  AND a.content_length > 5000
  AND wiki_llm_classify(a.content, 'Is this article a biography of a real person?')
  AND wiki_llm_classify(a.content, 'Did this person achieve international fame or recognition?')
  AND wiki_llm_classify(a.content, 'Was this person involved in any controversy or scandal?')

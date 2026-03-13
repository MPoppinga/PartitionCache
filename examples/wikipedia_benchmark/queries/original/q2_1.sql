-- Flight 2: Date range + content length + LLM
-- Q2_1: Scientific breakthroughs with real-world impact (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE a.creation_year BETWEEN 2010 AND 2015
  AND a.content_length > 5000
  AND wiki_llm_classify(a.content, 'Is this article about a scientific discovery, invention, or breakthrough?')
  AND wiki_llm_classify(a.content, 'Has the discovery described here been applied in medicine, technology, or industry?')

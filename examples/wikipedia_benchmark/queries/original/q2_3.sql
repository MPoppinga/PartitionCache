-- Flight 2: Date range + content length + LLM
-- Q2_3: Northern European events with environmental angle (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE a.creation_year BETWEEN 2010 AND 2015
  AND a.content_length > 5000
  AND wiki_llm_classify(a.content, 'Is this article primarily about a place, event, or tradition in Scandinavia, the Baltics, or the British Isles?')
  AND wiki_llm_classify(a.content, 'Does the article discuss environmental issues, climate, or natural landscapes?')

-- Flight 2: Science category + high edits + recent + LLM
-- Q2_3: Environmental science and climate topics (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%scienc%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Does this article discuss environmental issues, climate change, or ecology?')
  AND wiki_llm_classify(a.content, 'Does the article mention specific data, measurements, or statistics about environmental impact?')
